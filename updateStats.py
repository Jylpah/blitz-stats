#!/usr/bin/python3.7

# Script fetch Blitz player stats and tank stats

import sys, argparse, json, os, inspect, pprint, aiohttp, asyncio, aiofiles, aioconsole, re, logging, time, xmltodict, collections, pymongo
import motor.motor_asyncio, ssl, configparser
import blitzutils as bu
from blitzutils import BlitzStars
from blitzutils import WG

logging.getLogger("asyncio").setLevel(logging.DEBUG)

N_WORKERS = 10
MAX_RETRIES = 3
CACHE_VALID = 24*3600*5   # 5 days
MAX_UPDATE_INTERVAL = 365*24*3600 # 1 year
SLEEP = 0.5
WG_appID = 'cd770f38988839d7ab858d1cbe54bdd0'

FILE_CONFIG = 'blitzstats.ini'
FILE_ACTIVE_PLAYERS='activeinlast30days.json'

DB_C_ACCOUNTS   		= 'WG_Accounts'
DB_C_PLAYER_STATS		= 'WG_PlayerStats'
DB_C_TANK_STATS     	= 'WG_TankStats'
DB_C_BS_PLAYER_STATS   	= 'BS_PlayerStats'
DB_C_BS_TANK_STATS     	= 'BS_PlayerTankStats'
DB_C_TANKS     			= 'Tankopedia'
DB_C_ERROR_LOG			= 'ErrorLog'
DB_C_UPDATE_LOG			= 'UpdateLog'

UPDATE_FIELD = { 'tankstats'	: 'updated_WGtankstats',
				'playerstats'	: 'updated_WGplayerstats',
				'playerstatsBS' : 'updated_BSplayerstats',
				'tankstatsBS'	: 'updated_BStankstats'
				}
bs = None
wg = None
STATS_ADDED = 0

## main() -------------------------------------------------------------


async def main(argv):
	global bs, wg

	parser = argparse.ArgumentParser(description='Analyze Blitz replay JSONs from WoTinspector.com')
	parser.add_argument('--mode', default='help', choices=[ 'tankstats', 'playerstats', 'playerstatsBS', 'tankstatsBS', 'tankopedia', 'all' ], help='Choose what to update')
	parser.add_argument('--file', default=None, help='JSON file to read')
	parser.add_argument('--force', action='store_true', default=False, help='Force refreshing the active player list')
	parser.add_argument('--workers', type=int, default=N_WORKERS, help='Number of asynchronous workers')
	parser.add_argument('--nodb', action='store_true', default=False, help='Do NOT use DB for active players')
	parser.add_argument('--runErrorLog', action='store_true', default=False, help='Re-try previously failed requests')
	arggroup = parser.add_mutually_exclusive_group()
	arggroup.add_argument('-d', '--debug', 		action='store_true', default=False, help='Debug mode')
	arggroup.add_argument('-v', '--verbose', 	action='store_true', default=False, help='Verbose mode')
	arggroup.add_argument('-s', '--silent', 	action='store_true', default=False, help='Silent mode')
	
	args = parser.parse_args()
	bu.setSilent(args.silent)
	bu.setVerbose(args.verbose)
	bu.setDebug(args.debug)
	
	active_players = None
	try:
		bs = BlitzStars()
		wg = WG(WG_appID)

		## Read config
		config 	= configparser.ConfigParser()
		config.read(FILE_CONFIG)
		configDB 		= config['DATABASE']
		DB_SERVER 	= configDB.get('db_server', 'localhost')
		DB_PORT 	= configDB.getint('db_port', 27017)
		DB_SSL 		= configDB.getboolean('db_ssl', False)
		DB_CERT_REQ = configDB.getint('db_ssl_req', ssl.CERT_NONE)
		DB_AUTH 	= configDB.get('db_auth', 'admin')
		DB_NAME 	= configDB.get('db_name', 'BlitzStats')
		DB_USER 	= configDB.get('db_user', 'mongouser')
		DB_PASSWD 	= configDB.get('db_password', "PASSWORD")
		DB_CERT		= configDB.get('db_ssl_cert_file', None)
		DB_CA		= configDB.get('db_ssl_ca_file', None)
		
		#### Connect to MongoDB
		client = motor.motor_asyncio.AsyncIOMotorClient(DB_SERVER,DB_PORT, authSource=DB_AUTH, username=DB_USER, password=DB_PASSWD, ssl=DB_SSL, ssl_cert_reqs=DB_CERT_REQ, ssl_certfile=DB_CERT, tlsCAFile=DB_CA)

		db = client[DB_NAME]
		bu.debug(str(type(db)))	

		await db[DB_C_BS_PLAYER_STATS].create_index([('account_id', pymongo.ASCENDING), ('last_battle_time', pymongo.DESCENDING) ], background=True)	
		await db[DB_C_BS_TANK_STATS].create_index([('account_id', pymongo.ASCENDING), ('tank_id', pymongo.ASCENDING), ('last_battle_time', pymongo.DESCENDING) ], background=True)	
		await db[DB_C_BS_TANK_STATS].create_index([('tank_id', pymongo.ASCENDING), ('last_battle_time', pymongo.DESCENDING) ], background=True)	
		await db[DB_C_TANK_STATS].create_index([('account_id', pymongo.ASCENDING), ('tank_id', pymongo.ASCENDING), ('last_battle_time', pymongo.DESCENDING) ], background=True)	
		await db[DB_C_TANK_STATS].create_index([('tank_id', pymongo.ASCENDING), ('last_battle_time', pymongo.DESCENDING) ], background=True)	
		await db[DB_C_TANKS].create_index([('tank_id', pymongo.ASCENDING), ('tier', pymongo.DESCENDING) ], background=True)	
		await db[DB_C_TANKS].create_index([ ('name', pymongo.TEXT)], background=True)	
		await db[DB_C_ERROR_LOG].create_index([('account_id', pymongo.ASCENDING), ('time', pymongo.DESCENDING), ('type', pymongo.ASCENDING) ], background=True)	
		
		## get active player list ------------------------------
		if args.mode == 'tankopedia':
			await updateTankopedia(db, args.file, args.force)
		else:
			if args.nodb :
				active_players = await getActivePlayersBS(args.force)
			else:
				if args.runErrorLog:
					active_players = await getPrevErrors(db, args.mode)
				else:
					active_players = await getActivePlayersDB(db, args.mode, args.force)
			bu.verbose_std(str(len(active_players)) + ' players\' stats to be updated.')	

		tasks = []
		if args.mode == 'playerstatsBS' or args.mode == 'all':
			playerQ  = asyncio.Queue(1000)
			playerlist_task = asyncio.create_task(mkPlayerQ(playerQ, active_players))
			for i in range(args.workers):
				tasks.append(asyncio.create_task(BSplayerStatWorker(playerQ, i, db, args.runErrorLog)))
				bu.debug('Player stat Task ' + str(i) + ' started')
				await asyncio.sleep(SLEEP)
        
		if args.mode == 'tankstats' or args.mode == 'all':
			tankListQ  = asyncio.Queue(1000)
			tanklist_task = asyncio.create_task(mkPlayerQ(tankListQ, active_players))
			for i in range(args.workers):
				tasks.append(asyncio.create_task(tankStatWorker(tankListQ, i, db, args.runErrorLog)))
				bu.debug('Tank list Task ' + str(i) + ' started')	
				await asyncio.sleep(SLEEP)		
		
		if args.mode == 'tankstatsBS' or args.mode == 'all':
			tankListQ  = asyncio.Queue(1000)
			tanklist_task = asyncio.create_task(mkPlayerQ(tankListQ, active_players))
			for i in range(args.workers):
				tasks.append(asyncio.create_task(BStankStatWorker(tankListQ, i, db, args.runErrorLog)))
				bu.debug('Tank stat Task ' + str(i) + ' started')	
				await asyncio.sleep(SLEEP)		
		
		## wait queues to finish --------------------------------------
		if args.mode == 'playerstatsBS' or args.mode == 'all':
			bu.debug('Waiting for the player queue maker to finish')
			await asyncio.wait([playerlist_task])
			bu.debug('All active players added to the queue. Waiting for player stat workers to finish')
			await playerQ.join()
		
		if args.mode == 'tankstats' or args.mode == 'tankstatsBS' or args.mode == 'all':
			bu.debug('Waiting for the tanklist queue maker to finish')
			await asyncio.wait([ tanklist_task])
			bu.debug('Waiting for tank list workers to finish')
			await tankListQ.join()
		
		bu.debug('Cancelling workers')
		for task in tasks:
			task.cancel()
		bu.debug('Waiting for workers to cancel')
		if len(tasks) > 0:
			await asyncio.gather(*tasks, return_exceptions=True)

		logStatUpdated(db, args.mode)
		printUpdateStats(args.mode)

	except asyncio.CancelledError as err:
		bu.error('Queue got cancelled while still working.')
	except Exception as err:
		bu.error('Unexpected Exception: ' + str(type(err)) + ' : '+ str(err))
	finally:
		bu.printNewline(True)
		await bs.close()
		await wg.close()

	return None

def logStatUpdated(db : motor.motor_asyncio.AsyncIOMotorDatabase, mode : str):
	"""Log successfully finished status update"""
	dbc = db[DB_C_UPDATE_LOG]
	try:
		dbc.insert_one( { 'mode': mode, 'updated': NOW() } )
	except Exception as err:
		bu.error('Unexpected error: ' + str(type(err)) + ' : ' + str(err))
		return False
	return True

async def getActivePlayersDB(db : motor.motor_asyncio.AsyncIOMotorDatabase, mode: str, force = False):
	"""Get list of active accounts from the database"""
	dbc = db[DB_C_ACCOUNTS]
	players = list()
	i = 0
	if force:
		cursor = dbc.find()
	else:
		cursor = dbc.find(  { '$or': [ { UPDATE_FIELD[mode]: None }, { UPDATE_FIELD[mode] : { '$lt': NOW() - CACHE_VALID } } ] }, { '_id' : 1} )
	
	async for player in cursor:
		i += 1
		if (i % 1000) == 0 : 
			bu.printWaiter()
			bu.debug('Accounts read from DB: ' + str(i))
		try:
			players.append(player['_id'])
		except Exception as err:
			bu.error('Unexpected error: ' + str(type(err)) + ' : ' + str(err))
	bu.debug(str(len(players)) + ' read from the DB')
	return players

async def getActivePlayersBS(force: bool):
	"""Get active_players from BlitzStars or local cache file"""
	global bs, wg

	active_players = None
	if force or not (os.path.exists(FILE_ACTIVE_PLAYERS) and os.path.isfile(FILE_ACTIVE_PLAYERS)) or (NOW() - os.path.getmtime(FILE_ACTIVE_PLAYERS) > CACHE_VALID):
		try:
			url = bs.getUrlActivePlayers()
			bu.verbose('Retrieving active players file from BlitzStars.com')
			active_players = await bu.getUrlJSON(bs.session, url)
			await bu.saveJSON(FILE_ACTIVE_PLAYERS, active_players)
		except aiohttp.ClientError as err:
			bu.error("Could not retrieve URL" + url)
			bu.error(str(err))
		except Exception as err:
			bu.error('Unexpected Exception: ' + str(type(err)) + ' : '+ str(err))
	else:
		async with aiofiles.open(FILE_ACTIVE_PLAYERS, 'rt') as f:
			active_players = json.loads(await f.read())
	return active_players
				

async def getPrevErrors(db, stat_type: str):
	"""Get list of acccount_ids of the previous failed requests"""
	dbc = db[DB_C_ERROR_LOG]
	account_ids =  set()
	
	cursor = dbc.find({'type': stat_type}, { 'account_id': 1, '_id': 0 } )
	
	async for stat in cursor:
		try:
			account_ids.add(stat['account_id'])
		except Exception as err:
			bu.error('Unexpected error: ' + str(type(err)) + ' : ' + str(err))
	return list(account_ids)


async def clearErrors(db, account_id, stat_type: str):
	"""Delete ErrorLog entry for account_id, stat_type"""
	dbc = db[DB_C_ERROR_LOG]
	await dbc.delete_many({ 'account_id': account_id, 'type': stat_type })


async def hasFreshStats(db, account_id : int, stat_type: str ) -> bool:
	"""Check whether the DB has fresh enough stats for the account_id & stat_type"""
	dbc = db[DB_C_ACCOUNTS]
	#res = await dbc.find_one( { 'account_id' : account_id, '$or' : [ { UPDATE_FIELD['tankstats'] : { '$exists' : False }}, { UPDATE_FIELD['tankstats'] : None } , { UPDATE_FIELD['tankstats'] : { '$lt' : (NOW() - CACHE_VALID) } } ]  } )
	try:
		res = await dbc.find_one( { '_id' : account_id })
		if res == None:
			return False
		if (UPDATE_FIELD[stat_type] in res) and ('latest_battle_time' in res):
			if (res[UPDATE_FIELD[stat_type]] == None) or (res['latest_battle_time'] == None) or (res['latest_battle_time'] > NOW()):
				return False
			elif (NOW() - res[UPDATE_FIELD[stat_type]])  > min(MAX_UPDATE_INTERVAL, (res[UPDATE_FIELD[stat_type]] - res['latest_battle_time'])/2):
				return False
			
			return True
		else:
			return False
	except Exception as err:
		bu.error('Unexpected error: ' + str(type(err)) + ' : ' + str(err))
		return False


def printUpdateStats(mode: str):
	if mode in [ 'tankstats', 'playerstats', 'playerstatsBS', 'tankstatsBS']:
		bu.verbose_std('Total ' + str(STATS_ADDED) + ' stats updated')
		return True
	else:
		return False


async def updateStatsUpdated(db, account_id, field, last_battle_time = None) -> bool:
	dbc = db[DB_C_ACCOUNTS]
	try:
		await dbc.update_one( { '_id' : account_id }, { '$set': { 'last_battle_time': last_battle_time, UPDATE_FIELD[field] : NOW() }} )
		return True
	except Exception as err:
		bu.error('Unexpected Exception: ' + str(type(err)) + ' : '+ str(err) )
		return False	


async def updateTankopedia( db: motor.motor_asyncio.AsyncIOMotorDatabase, filename: str, force: bool):
	"""Update tankopedia in the database"""
	dbc = db[DB_C_TANKS]
	if filename != None:
		async with aiofiles.open(filename, 'rt', encoding="utf8") as fp:
			tanks = json.loads(await fp.read())
			inserted = 0
			updated = 0
			for tank_id in tanks['data']:
				try:
					tank = tanks['data'][tank_id]
					tank['_id'] = int(tank_id)
					if force:
						await dbc.replace_one( { 'tank_id' : int(tank_id) } , tank, upsert=force)
						updated += 1
					else:
						await dbc.insert_one(tank)
						inserted += 1
				except pymongo.errors.DuplicateKeyError:
					pass
				except Exception as err:
					bu.error(str(err))
			bu.verbose_std('Added ' + str(inserted) + ' tanks, updated ' + str(updated) + ' tanks')
			return True			
	else:
		bu.error('--file argument not set')
	return False


async def mkPlayerQ(queue : asyncio.Queue, accountID_list : list):
	"""Create queue of replays to post"""
	for accountID in accountID_list:
		bu.debug('Adding account_id: ' + str(accountID) + ' to the queue')
		await queue.put(accountID)

	return None

def NOW() -> int:
	return int(time.time())


async def removeAccountID(db: motor.motor_asyncio.AsyncIOMotorDatabase, account_id: int):
	"""Remove account_id from the DB"""
	dbc = db[DB_C_ACCOUNTS]
	try: 
		await dbc.delete_one({ '_id': account_id } )		
	except Exception as err:
		bu.error('Unexpected Exception: ' + str(type(err)) + ' : '+ str(err))
	finally:
		bu.debug('Removed account_id: ' + str(account_id))
	return None


async def BSplayerStatWorker(playerQ : asyncio.Queue, workerID : int, db : motor.motor_asyncio.AsyncIOMotorDatabase, clrErrorLog = False):
	"""Async Worker to process the player queue for BlitzStars.com Player stats"""
	dbc = db[DB_C_BS_PLAYER_STATS]
	field = 'playerstatsBS'

	i = 1
	while True:
		account_id = await playerQ.get()
		if i == 1:
			bu.printWaiter()
		try:
			i = (i+1)  % 100
			url = None
			if await hasFreshStats(db, account_id, field):
				bu.debug('[' + str(workerID) + ']: Fresh-enough stats for account_id=' + str(account_id) + ' exists in the DB')
			else:
				url = bs.getUrlPlayerStats(account_id)				
				stats = await bu.getUrlJSON(bs.session, url)
				last_battle_time = -1
				for stat in stats:
					last_battle_time = max(last_battle_time, stat['last_battle_time'])
				try: 
					await dbc.insert_many(stats, ordered=False)
				except pymongo.errors.BulkWriteError as err:
					pass
				finally:
					await updateStatsUpdated(db, account_id, field, last_battle_time)
					bu.debug('[' + str(workerID) + ']: Added stats for account_id=' + str(account_id))		
			
		except Exception as err:
			bu.error('[' + str(workerID) + '] Unexpected Exception: ' + str(type(err)) + ' : '+ str(err) + (' URL: ' + url) if url!= None else "")
			await logError(db, account_id, field, clrErrorLog)
		finally:
			if clrErrorLog:
				await clearErrors(db, account_id, field)
			playerQ.task_done()		
			await asyncio.sleep(SLEEP)
	return None


async def BStankStatWorker(playerQ : asyncio.Queue, workerID : int, db : motor.motor_asyncio.AsyncIOMotorDatabase, clrErrorLog = False):
	"""Async Worker to fetch players' tank stats from BlitzStars.com"""
	dbc = db[DB_C_BS_TANK_STATS]
	field = 'tankstatsBS'

	i = 1
	while True:
		account_id = await playerQ.get()
		if i == 1:
			bu.printWaiter()
		try:
			i = (i+1)  % 100
			url = None
			if await hasFreshStats(db, account_id, field):
				bu.debug('[' + str(workerID) + ']: Fresh-enough stats for account_id=' + str(account_id) + ' exists in the DB')
			else:
				url = bs.getUrlPlayersTankStats(account_id)				
				stats = await bu.getUrlJSON(bs.session, url, bs.chkJSONtankStats)				
				if (stats == None) or (len(stats['data']) == 0):
					bu.debug('Did not receive stats for account_id=' + str(account_id))
				else:
					tankStats = []
					for tankStat in stats['data']:
						accountID 			= tankStat['account_id'] 
						tankID 				= tankStat['tank_id']
						last_battle_time 	= tankStat['last_battle_time']
						tankStat['_id']  	= mkID(accountID, tankID, last_battle_time)
						tankStats.append(tankStat)
					try: 
						await dbc.insert_many(tankStats, ordered=False)
					except pymongo.errors.BulkWriteError as err:
						pass
					finally:
						await updateStatsUpdated(db, account_id, field, last_battle_time)
						bu.debug('[' + str(workerID) + ']: Added stats for account_id=' + str(account_id))	

		except Exception as err:
			bu.error('[' + str(workerID) + '] Unexpected Exception: ' + str(type(err)) + ' : '+ str(err) + (' URL: ' + url) if url!= None else "")
			await logError(db, account_id, field, clrErrorLog)
		finally:
			if clrErrorLog:
				await clearErrors(db, account_id, field)
			playerQ.task_done()	
			await asyncio.sleep(SLEEP)
	return None


async def tankStatWorker(playerQ : asyncio.Queue, workerID : int, db : motor.motor_asyncio.AsyncIOMotorDatabase, clrErrorLog = False):
	"""Async Worker to process the replay queue: WG player tank stats """
	dbc = db[DB_C_TANK_STATS]
	field = 'tankstats'
	global STATS_ADDED
	i = 1
	while True:
		account_id = await playerQ.get()
		if i == 1:
			bu.printWaiter()
		try:
			# bu.error('[' + str(workerID) + ']: START')
			i = (i+1)  % 100
			##TBD: Check acccounts, fetch data, update accounts, even if not data fetched. 
			url = None
			if await hasFreshStats(db, account_id, field):
				bu.debug('[' + str(workerID) + ']: account_id=' + str(account_id) + ' has Fresh-enough stats in the DB')
			else:	
				bu.debug('[' + str(workerID) + ']: account_id=' + str(account_id) + ' does not have Fresh-enough stats in the DB')
				url = wg.getUrlPlayerTankList(account_id)	
				#bu.error('[' + str(workerID) + ']: URL: ' + url)			
				stats = await bu.getUrlJSON(wg.session, url, wg.chkJSONstatus)
				tankStats = []
				latest_battle = 0
				for tankStat in stats['data'][str(account_id)]:
					tankID 				= tankStat['tank_id']
					last_battle_time 	= tankStat['last_battle_time']
					tankStat['_id']  	= mkID(account_id, tankID, last_battle_time)
					tankStats.append(tankStat)

					if (last_battle_time > latest_battle):
						latest_battle = last_battle_time 
				#	RECOMMENDATION TO USE SINGLE INSERTS OVER MANY
				try: 
					res = await dbc.insert_many(tankStats, ordered=False)
					tmp = len(res.inserted_ids)
					bu.debug(str(tmp) + ' stats added (insert_many() result)', workerID)
					STATS_ADDED += tmp
					#STATS_ADDED += len(res.inserted_ids)					
				except pymongo.errors.BulkWriteError as err:
					tmp = err.details['nInserted']
					bu.debug(str(tmp) + ' stats added', workerID)
					STATS_ADDED += tmp
					#STATS_ADDED += err.details['nInserted']					
					pass
				finally:
					await updateStatsUpdated(db, account_id, field, latest_battle)
					bu.debug('[' + str(workerID) + ']: Added stats for account_id=' + str(account_id))	
	
		except TypeError as err:
			bu.debug('\naccount_id ' + str(account_id) + ' : Received empty stats')
			await logError(db, account_id, field, clrErrorLog)
		except Exception as err:
			bu.error('[' + str(workerID) + '] Unexpected Exception: ' + str(type(err)) + ' : '+ str(err) + (' URL: ' + url) if url!= None else "")
			await logError(db, account_id, field, clrErrorLog)
		finally:
			if clrErrorLog:
				await clearErrors(db, account_id, field)
			playerQ.task_done()	
			await asyncio.sleep(SLEEP)
	return None


async def logError(db, account_id: int, stat_type: str, clrErrorLog = False):
	dbc = db[DB_C_ERROR_LOG]
	try:
		if clrErrorLog:
			await removeAccountID(db, account_id)
		else:
			await dbc.insert_one( {'account_id': account_id, 'type': stat_type, 'time': NOW() } )
			bu.debug('Logging Error: account_id=' + str(account_id) + ' stat_type=' + stat_type)
	except Exception as err:
		bu.error('Unexpected Exception: ' + str(type(err)) + ' : '+ str(err))


def mkID(accountID: int, tankID: int, last_battle_time: int) -> str:
	return hex(accountID)[2:].zfill(10) + hex(tankID)[2:].zfill(6) + hex(last_battle_time)[2:].zfill(8)

    ### main()
if __name__ == "__main__":
   #asyncio.run(main(sys.argv[1:]), debug=True)
   asyncio.run(main(sys.argv[1:]))
