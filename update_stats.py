#!/usr/bin/env python3

# Script fetch Blitz player stats and tank stats

import sys, argparse, json, os, inspect, pprint, aiohttp, asyncio, aiofiles, aioconsole, re, logging, time, xmltodict, collections, pymongo
import motor.motor_asyncio, ssl, configparser, random, datetime
import blitzutils as bu
from blitzutils import BlitzStars, WG, RecordLogger

logging.getLogger("asyncio").setLevel(logging.DEBUG)

N_WORKERS = 50
MAX_RETRIES = 3
CACHE_VALID = 5   # 5 days
#MIN_UPDATE_INTERVAL = 7*24*3600 # 7 days
MAX_UPDATE_INTERVAL = 6*30*24*3600 # 6 months
SLEEP = 0.1
WG_APP_ID = 'cd770f38988839d7ab858d1cbe54bdd0'

FILE_CONFIG = 'blitzstats.ini'
FILE_ACTIVE_PLAYERS='activeinlast30days.json'

# MongoDB collections for storing stats
DB_C_ACCOUNTS   		= 'WG_Accounts'
DB_C_PLAYER_STATS		= 'WG_PlayerStats'
DB_C_PLAYER_ACHIVEMENTS	= 'WG_PlayerAchievements'
DB_C_TANK_STATS     	= 'WG_TankStats'
DB_C_BS_PLAYER_STATS   	= 'BS_PlayerStats'
DB_C_BS_TANK_STATS     	= 'BS_PlayerTankStats'
DB_C_TANKS     			= 'Tankopedia'
DB_C_TANK_STR			= 'WG_TankStrs'
DB_C_ERROR_LOG			= 'ErrorLog'
DB_C_UPDATE_LOG			= 'UpdateLog'

## Matches --mode param
UPDATE_FIELD = {'tank_stats'			: 'updated_WGtankstats',
				'player_stats'			: 'updated_WGplayerstats',
				'player_achievements'	: 'updated_WGplayerachievements'
				# 'player_stats_BS' 		: 'updated_BSplayerstats',
				# 'tank_stats_BS'			: 'updated_BStankstats'
				}
bs = None
wg = None
stats_added = 0

## main() -------------------------------------------------------------


async def main(argv):
	global bs, wg, WG_APP_ID
	# set the directory for the script
	current_dir = os.getcwd()
	os.chdir(os.path.dirname(sys.argv[0]))

	# set detault parameters
	## Default options:
	
	# WG_APP_ID		= WG_APP_ID
	WG_RATE_LIMIT	= 20  ## WG standard. Do not edit unless you have your
						  ## own server app ID, it will REDUCE the performance
	
	# DB defaults
	DB_SERVER 	= 'localhost'
	DB_PORT 	= 27017
	DB_SSL		= False
	DB_CERT_REQ = ssl.CERT_NONE
	DB_AUTH 	= 'admin'
	DB_NAME 	= 'BlitzStats'
	DB_USER		= 'mongouser'
	DB_PASSWD 	= 'PASSWORD'
	DB_CERT 	= None
	DB_CA 		= None
	
	## Read config
	if os.path.isfile(FILE_CONFIG):
		config 	= configparser.ConfigParser()
		config.read(FILE_CONFIG)
		
		if 'WG' in config.sections():
			configWG 		= config['WG']
			WG_APP_ID		= configWG.get('wg_app_id', WG_APP_ID)
			WG_RATE_LIMIT	= configWG.getint('wg_rate_limit', WG_RATE_LIMIT)

		if 'DATABASE' in config.sections():
			configDB 	= config['DATABASE']
			DB_SERVER 	= configDB.get('db_server', DB_SERVER)
			DB_PORT 	= configDB.getint('db_port', DB_PORT)
			DB_SSL 		= configDB.getboolean('db_ssl', DB_SSL)
			DB_CERT_REQ = configDB.getint('db_ssl_req', DB_CERT_REQ)
			DB_AUTH 	= configDB.get('db_auth', DB_AUTH)
			DB_NAME 	= configDB.get('db_name', DB_NAME)
			DB_USER 	= configDB.get('db_user', DB_USER)
			DB_PASSWD 	= configDB.get('db_password', DB_PASSWD)
			DB_CERT		= configDB.get('db_ssl_cert_file', DB_CERT)
			DB_CA		= configDB.get('db_ssl_ca_file', DB_CA)
	else:
		bu.warning(FILE_CONFIG + ' Config file not found')

	parser = argparse.ArgumentParser(description='Analyze Blitz replay JSONs from WoTinspector.com')
	parser.add_argument('--mode', default='help', nargs='+', choices=list(UPDATE_FIELD.keys()) + [ 'tankopedia' ], help='Choose what to update')
	parser.add_argument('--file', default=None, help='JSON file to read')
	parser.add_argument('--force', action='store_true', default=False, help='Force refreshing the active player list')
	parser.add_argument('--workers', type=int, default=N_WORKERS, help='Number of asynchronous workers')
	parser.add_argument('--cache-valid', type=int, dest='cache_valid', default=CACHE_VALID, help='Do not update stats newer than N Days')
	parser.add_argument('--player-src', dest='player_src', default='db', choices=[ 'db', 'blitzstars' ], help='Source for the account list. Default: db')
	parser.add_argument('--sample', type=int, default=0, help='Sample size of accounts to update')
	
	parser.add_argument('--run-error-log', dest='run_error_log', action='store_true', default=False, help='Re-try previously failed requests and clear errorlog')
	parser.add_argument('--check-invalid', dest='chk_invalid', action='store_true', default=False, help='Re-check invalid accounts')
	
	parser.add_argument('-l', '--log', action='store_true', default=False, help='Enable file logging')
	arggroup = parser.add_mutually_exclusive_group()
	arggroup.add_argument('-d', '--debug', 		action='store_true', default=False, help='Debug mode')
	arggroup.add_argument('-v', '--verbose', 	action='store_true', default=False, help='Verbose mode')
	arggroup.add_argument('-s', '--silent', 	action='store_true', default=False, help='Silent mode')
	
	args = parser.parse_args()
	args.cache_valid = args.cache_valid*24*3600  # from days to secs	
	bu.set_log_level(args.silent, args.verbose, args.debug)
	bu.set_progress_step(1000)
	
	if args.file != None:
		args.file = bu.rebase_file_args(current_dir, args.file)
	
	if args.log:
		datestr = datetime.datetime.now().strftime("%Y%m%d_%H%M")
		await bu.set_file_logging(bu.rebase_file_args(current_dir, 'update_stats_' + datestr + '.log'))
		
	try:		
		bs = BlitzStars()
		wg = WG(WG_APP_ID, rate_limit=WG_RATE_LIMIT, global_rate_limit=False)

		#### Connect to MongoDB
		if (DB_USER==None) or (DB_PASSWD==None):
			client = motor.motor_asyncio.AsyncIOMotorClient(DB_SERVER,DB_PORT, ssl=DB_SSL, ssl_cert_reqs=DB_CERT_REQ, ssl_certfile=DB_CERT, tlsCAFile=DB_CA)
		else:
			client = motor.motor_asyncio.AsyncIOMotorClient(DB_SERVER,DB_PORT, authSource=DB_AUTH, username=DB_USER, password=DB_PASSWD, ssl=DB_SSL, ssl_cert_reqs=DB_CERT_REQ, ssl_certfile=DB_CERT, tlsCAFile=DB_CA)

		db = client[DB_NAME]
		bu.debug(str(type(db)))	

		await db[DB_C_BS_PLAYER_STATS].create_index([('account_id', pymongo.ASCENDING), ('last_battle_time', pymongo.DESCENDING) ])	
		await db[DB_C_BS_TANK_STATS].create_index([('account_id', pymongo.ASCENDING), ('tank_id', pymongo.ASCENDING), ('last_battle_time', pymongo.DESCENDING) ])	
		await db[DB_C_BS_TANK_STATS].create_index([('tank_id', pymongo.ASCENDING), ('last_battle_time', pymongo.DESCENDING) ])	
		#await db[DB_C_TANK_STATS].create_index([('account_id', pymongo.ASCENDING), ('tank_id', pymongo.ASCENDING), ('last_battle_time', pymongo.DESCENDING) ])	
		await db[DB_C_TANK_STATS].create_index([('account_id', pymongo.ASCENDING), ('last_battle_time', pymongo.DESCENDING) ])	
		await db[DB_C_TANK_STATS].create_index([('tank_id', pymongo.ASCENDING), ('last_battle_time', pymongo.DESCENDING) ])	
		await db[DB_C_PLAYER_ACHIVEMENTS].create_index([('account_id', pymongo.ASCENDING), ('updated', pymongo.DESCENDING) ])	
		await db[DB_C_TANKS].create_index([('tank_id', pymongo.ASCENDING), ('tier', pymongo.DESCENDING) ])	
		await db[DB_C_TANKS].create_index([ ('name', pymongo.TEXT)])	
		await db[DB_C_ERROR_LOG].create_index([('account_id', pymongo.ASCENDING), ('time', pymongo.DESCENDING), ('type', pymongo.ASCENDING) ])	
		

		## get active player list ------------------------------
		active_players = {}
		start_time = 0
		if 'tankopedia' in args.mode:
			await update_tankopedia(db, args.file, args.force)
		else:
			start_time = print_date('DB update started')
			active_players  = await get_active_players(db, args)			
			Qcreator_tasks 	= []
			worker_tasks 	= []
			Q = {}

			## set progress bar
			tmp_progress_max = 0
			for mode in set(args.mode) & set(UPDATE_FIELD.keys()):
				tmp_progress_max += len(active_players[mode])
			# bu.print_new_line()
			bu.set_progress_bar('Fetching stats:', tmp_progress_max, 50, True)

			for mode in UPDATE_FIELD:
				Q[mode] = asyncio.Queue()
			
			if 'tank_stats' in args.mode:
				mode = 'tank_stats'
				Qcreator_tasks.append(asyncio.create_task(mk_playerQ(Q[mode], active_players[mode])))
				for i in range(args.workers):
					worker_tasks.append(asyncio.create_task(WG_tank_stat_worker( db, Q[mode], i, args )))
					bu.debug('Tank list Task started', id=i)	
		
			if 'tank_stats_BS' in args.mode:
				mode = 'tank_stats_BS'
				Qcreator_tasks.append(asyncio.create_task(mk_playerQ(Q[mode], active_players[mode])))
				for i in range(args.workers):
					worker_tasks.append(asyncio.create_task(BS_tank_stat_worker(db, Q[mode], i, args )))
					bu.debug('Tank stat Task ' + str(i) + ' started')	

			if 'player_achievements' in args.mode:
				mode = 'player_achievements'
				Qcreator_tasks.append(asyncio.create_task(mk_playerQ(Q[mode], active_players[mode])))
				for i in range(args.workers):
					worker_tasks.append(asyncio.create_task(WG_player_achivements_worker(db, Q[mode], i, args )))
					bu.debug('Player achievement stat Task started', id=i)

			if 'player_stats' in args.mode:
				mode = 'player_stats'
				bu.error('Fetching WG player stats NOT IMPLEMENTED YET')

			if 'player_stats_BS' in args.mode:
				mode = 'player_stats_BS'
				Qcreator_tasks.append(asyncio.create_task(mk_playerQ(Q[mode], active_players[mode])))
				for i in range(args.workers):
					worker_tasks.append(asyncio.create_task(BS_player_stat_worker(db, Q[mode], i, args )))
					bu.debug('Player stat Task ' + str(i) + ' started')
					
			## wait queues to finish --------------------------------------
			
			if len(Qcreator_tasks) > 0: 
				bu.log('Waiting for the work queue makers to finish')
				await asyncio.wait(Qcreator_tasks)

			bu.log('All active players added to the queue. Waiting for stat workers to finish')
			for mode in UPDATE_FIELD:
				await Q[mode].join()		

			bu.finish_progress_bar()

			bu.log('All work queues empty. Cancelling workers')
			for task in worker_tasks:
				task.cancel()
			bu.log('Waiting for workers to cancel')
			stat_logger = RecordLogger()
			if len(worker_tasks) > 0:				
				for stats_logged in await asyncio.gather(*worker_tasks, return_exceptions=True):
					stat_logger.merge(stats_logged)

			if (args.sample == 0) and (not args.run_error_log):
				# only for full stats
				log_update_time(db, args.mode)
			print_update_stats(args.mode, args.run_error_log, stat_logger)
			wg.print_request_stats()
			print_date('DB update ended', start_time)
			bu.print_new_line(True)
	except asyncio.CancelledError as err:
		bu.error('Queue got cancelled while still working.')
	except Exception as err:
		bu.error('Unexpected Exception', err)
	finally:		
		await bs.close()
		await wg.close()
		if args.log:
			await bu.close_file_logging()

	return None


async def get_active_players(db : motor.motor_asyncio.AsyncIOMotorDatabase, args : argparse.Namespace):
	"""Get activbe player list from the sources (DB, BlitzStars)"""
	active_players = {}
	if args.run_error_log:
		for mode in get_stat_modes(args.mode):
			active_players[mode] = await get_players_errorlog(db, mode)
	elif args.player_src == 'blitzstars':
		bu.debug('src BlitzStars')
		tmp_players = await get_active_players_BS(args)
		if args.sample > 0:
			tmp_players = random.sample(tmp_players, args.sample)		
		for mode in get_stat_modes(args.mode): 
			active_players[mode] = tmp_players
	elif args.player_src == 'db':
		bu.debug('src DB')
		for mode in get_stat_modes_WG(args.mode):
			bu.debug('Getting players from DB: ' + mode)
			active_players[mode] = await get_active_players_DB(db, mode, args)			
		if (len(get_stat_modes_BS(args.mode)) > 0):
			tmp_players = await get_active_players_BS(args)
			if args.sample > 0:
				tmp_players = random.sample(tmp_players, args.sample)
			for mode in get_stat_modes_BS(args.mode):
				bu.debug('Getting players from BS: ' + mode)
				active_players[mode] = tmp_players
		
		for mode in active_players.keys():
			random.shuffle(active_players[mode])
	
	return active_players


def print_date(msg : str = '', start_time : datetime.datetime = None ) -> datetime.datetime:
	timenow = datetime.datetime.now()
	bu.verbose_std(msg + ': ' + timenow.replace(microsecond=0).isoformat(sep=' '))
	if start_time != None:
		delta = timenow - start_time
		secs = delta.total_seconds()
		hours = int(secs // 3600)
		minutes = int(secs // 60)
		bu.verbose_std('The processing took ' + str(hours) + 'h ' + str(minutes) + 'min')
	return timenow


def get_stat_modes(mode_list: list) -> list:
	"""Return modes of fetching stats (i.e. NOT tankopedia)"""
	return list(set(mode_list) & set(UPDATE_FIELD.keys()))


def get_stat_modes_WG(mode_list: list) -> list:
	"""Return modes of fetching stats from WG API"""
	return list(set(mode_list) & set( [ 'tank_stats', 'player_stats', 'player_achievements'] ))


def get_stat_modes_BS(mode_list: list) -> list:
	"""Return modes of fetching stats from BlitzStars"""
	return list(set(mode_list) & set( [ 'tank_stats_BS', 'player_stats_BS' ] ))


def log_update_time(db : motor.motor_asyncio.AsyncIOMotorDatabase, mode : list):
	"""Log successfully finished status update"""
	dbc = db[DB_C_UPDATE_LOG]
	try:
		now = bu.NOW()
		for m in mode:
			dbc.insert_one( { 'mode': m, 'updated': now } )
	except Exception as err:
		bu.error('Unexpected Exception', err)
		return False
	return True


async def get_active_players_DB(db : motor.motor_asyncio.AsyncIOMotorDatabase, mode: str, args : argparse.Namespace):
	"""Get list of active accounts from the database"""
	try:
		dbc = db[DB_C_ACCOUNTS]
		
		force 			= args.force
		cache_valid 	= args.cache_valid
		sample 			= args.sample
		chk_invalid 	= args.chk_invalid
		update_field 	= UPDATE_FIELD[mode]
		NOW = bu.NOW()

		if chk_invalid:
			pipeline = [   	{'$match': { '$and': [ { '_id': { '$lt': 31e8 } }, 
													{ 'invalid': { '$exists': True } }] } }, 
							]
		elif force:
			pipeline = [   	{'$match': { '$and': [ { '_id': { '$lt': 31e8 } }, 
													{ 'invalid': { '$exists': False } }] } }, 
							]
		else:
			pipeline = [ 	{'$match': {  '$and' : [ { '_id': { '$lt': 31e8 } }, 
													{ 'invalid': { '$exists': False} }, 
													{ '$or': [ 
																{ update_field : None }, 
																{ update_field : { '$lt': NOW - cache_valid}}
																] } 
													] } }						
						]
		
		if sample > 0:
			pipeline.append({'$sample': {'size' : sample} })

		cursor = dbc.aggregate(pipeline, allowDiskUse=False)
		
		account_ids = list()
		i = 0		
		tmp_steps = bu.get_progress_step()
		bu.set_progress_step(1000)
		bu.set_counter('Fetching players:')

		async for player in cursor:
			try:
				i += 1
				if bu.print_progress():
					bu.debug('Accounts read from DB: ' + str(i))
				# REMOVED 2020-12-09 to ensure inactive players are being captured
				# if (not force) and (not chk_invalid) and (update_field in player) and ('latest_battle_time' in player):
				# 	if (player[update_field] != None) and (player['latest_battle_time'] != None) and (player['latest_battle_time'] < NOW):
				# 		if (NOW - player[update_field]) < min(MAX_UPDATE_INTERVAL, (player[update_field] - player['latest_battle_time'])/2):
				# 			continue
				account_ids.append(player['_id'])
			except Exception as err:
				bu.error('account_id=' + str(player), err)

		bu.finish_progress_bar()
		bu.set_progress_step(tmp_steps)
		
		bu.log(str(len(account_ids)) + ' read from the DB')
		return account_ids
	except Exception as err:
		bu.error('Unexpected error', err)

async def get_active_players_BS(args : argparse.Namespace):
	"""Get active_players from BlitzStars or local cache file"""
	global bs, wg

	force 		= args.force
	cache_valid = args.cache_valid

	active_players = None
	if force or not (os.path.exists(FILE_ACTIVE_PLAYERS) and os.path.isfile(FILE_ACTIVE_PLAYERS)) or (bu.NOW() - os.path.getmtime(FILE_ACTIVE_PLAYERS) > cache_valid):
		try:
			bu.verbose('Retrieving active players file from BlitzStars.com')
			url = bs.get_url_active_players()
			active_players = await bu.get_url_JSON(bs.session, url)
			await bu.save_JSON(FILE_ACTIVE_PLAYERS, active_players)
		except aiohttp.ClientError as err:
			bu.error("Could not retrieve URL" + url)
			bu.error(exception=err)
		except Exception as err:
			bu.error('Unexpected error', err)
	else:
		async with aiofiles.open(FILE_ACTIVE_PLAYERS, 'rt') as f:
			active_players = json.loads(await f.read())
	return active_players
				

async def get_players_errorlog(db : motor.motor_asyncio.AsyncIOMotorDatabase, mode :str):
	"""Get list of acccount_ids of the previous failed requests"""
	try:
		dbc = db[DB_C_ERROR_LOG]
		account_ids =  set()
		
		cursor = dbc.find({'type': mode}, { 'account_id': 1, '_id': 0 } )
		async for stat in cursor:
			try:
				account_ids.add(stat['account_id'])
			except Exception as err:
				bu.error('Unexpected error', err)
		bu.verbose_std('Re-checking ' + str(len(account_ids)) + ' account_ids')
		return list(account_ids)

	except Exception as err:
		bu.error('Unexpected error', err)	
	return None


## DEPRECIATED
async def chk_accounts2update(db : motor.motor_asyncio.AsyncIOMotorDatabase, account_ids: list, stat_type: str) -> list:
	"""Check whether the DB has fresh enough stats for the account_id & stat_type"""
	dbc = db[DB_C_ACCOUNTS]
	try:
		bu.verbose_std('chk_accounts2update() is depreciated')
		stats_update_needed = list()
		update_field = UPDATE_FIELD[stat_type]
		NOW = bu.NOW()

		# this should work since Python does not enforce type hints: https://docs.python.org/3/library/typing.html 
		if isinstance(account_ids, int):
			account_ids = list(account_ids)

		cursor = dbc.find( { '$and' : [{ '_id' : { '$in': account_ids }}, \
		                               { 'invalid' : { '$exists': False }}, \
									   { '$or': [ { 'last_battle_time' : { '$exists': False }}, \
												{ 'last_battle_time' : { '$gt': NOW }}, \
												{ update_field : { '$exists': False }}, \
											#	{ update_field : { '$lt': NOW - MIN_UPDATE_INTERVAL}}, \
												{ update_field : { '$gt': NOW}} ]}]}, \
							{'last_battle_time' : 1, update_field : 1 } )
		
		

		async for res in cursor: 
			if (update_field in res) and ('latest_battle_time' in res):
				if (res[update_field] != None) and (res['latest_battle_time'] != None) and (res['latest_battle_time'] < NOW):
					if (NOW - res[update_field])  < min(MAX_UPDATE_INTERVAL, (res[update_field] - res['latest_battle_time'])/2):
						continue
			stats_update_needed.append(res['_id'])
		
	except Exception as err:
		bu.error('Unexpected error', err)
	bu.debug('Stats to update: ' + str(len(stats_update_needed)))
	return stats_update_needed


## DEPRECIATED
async def chk_account2update(db : motor.motor_asyncio.AsyncIOMotorDatabase, account_id : int, stat_type: str) -> bool:
	"""Check whether the DB has fresh enough stats for the account_id & stat_type"""
	dbc = db[DB_C_ACCOUNTS]
	bu.error('DEPRECIATED function')
	try:
		update_field = UPDATE_FIELD[stat_type]
		res = await dbc.find_one( { '_id' : account_id })
		if res == None:
			bu.debug('account_id: ' + str(account_id) + ' not found fromn account DB. This should not happen.')
			return False
		if ('invalid' in res):
			bu.debug('account_id: ' + str(account_id) + ' is invalid')
			return False
		if (update_field in res) and ('latest_battle_time' in res):
			if (res[update_field] == None) or (res['latest_battle_time'] == None) or (res['latest_battle_time'] > bu.NOW()):
				return True
			elif (bu.NOW() - res[update_field])  < min(MAX_UPDATE_INTERVAL, (res[update_field] - res['latest_battle_time'])/2):
				bu.debug('account_id: ' + str(account_id) + ' has been updated recently')
				return False
			# Do update
			return True
		else:
			return True
	except Exception as err:
		bu.error('Unexpected error', err)
		return False


def print_update_stats(mode: list, error_log : bool = False, stat_logger: RecordLogger = None):
	if stat_logger != None:
		for stat in sorted(stat_logger.get_categories()):
			bu.verbose_std('{:20}: {}'.format(stat, stat_logger.get_value(stat)))
	
	if len(get_stat_modes(mode)) > 0:
		bu.verbose_std('Total ' + str(stats_added) + ' stats updated')
		return True
	else:
		return False


async def update_stats_update_time(db : motor.motor_asyncio.AsyncIOMotorDatabase, account_id: int, stat_type: str, last_battle_time: int = None, inactive: bool = None) -> bool:
	"""Update DB_C_ACCOUNTS table with the update time of the player's stats"""

	# dbc = db[DB_C_ACCOUNTS]
	dbc_update_log = db[DB_C_UPDATE_LOG]
	try:
		#await dbc.update_one( { '_id' : account_id }, { '$set': { 'last_battle_time': last_battle_time, UPDATE_FIELD[stat_type] : bu.NOW(), 'inactive': inactive }} )
		#await dbc.update_one( { '_id' : account_id }, { '$set': { 'last_battle_time': last_battle_time, UPDATE_FIELD[stat_type] : bu.NOW() }} )
		FIELDS = dict()

		if last_battle_time != None:
			FIELDS['last_battle_time'] = last_battle_time
		if inactive != None:
			FIELDS['inactive'] = inactive
		if FIELDS == dict():
			FIELDS = None
		# await dbc.update_one( { '_id' : account_id }, { '$set': FIELDS } )
		await update_account(db, account_id, stat_type, FIELDS)
		
		## Added 2020-12-09 to keep log of updated account IDs
		await dbc_update_log.insert_one({ 'mode': stat_type, 'account_id' : account_id, 'updated': bu.NOW()})
		
		return True
	except Exception as err:
		error_account_id(account_id, 'Unexpected error', exception=err)
		return False	


async def update_tankopedia( db: motor.motor_asyncio.AsyncIOMotorDatabase, filename: str, force: bool):
	"""Update tankopedia in the database"""
	dbc = db[DB_C_TANKS]
	if filename != None:
		async with aiofiles.open(filename, 'rt', encoding="utf8") as fp:
			# Update Tankopedia
			tanks = json.loads(await fp.read())
			inserted = 0
			updated = 0
			for tank_id in tanks['data']:
				try:
					tank = tanks['data'][tank_id]
					tank['_id'] = int(tank_id)
					if not all(field in tank for field in ['tank_id', 'name','nation', 'tier','type' ,'is_premium']):
						bu.error('Missing fields in: ' + str(tank))
						continue
					if force:
						await dbc.replace_one( { 'tank_id' : int(tank_id) } , tank, upsert=force)
						updated += 1
					else:
						await dbc.insert_one(tank)
						inserted += 1
						bu.verbose_std('Added tank: ' + tank['name'])
				except pymongo.errors.DuplicateKeyError:
					pass
				except Exception as err:
					bu.error('Unexpected error', err)
			bu.verbose_std('Added ' + str(inserted) + ' tanks, updated ' + str(updated) + ' tanks')
			## update tank strings
			dbc = db[DB_C_TANK_STR]
			counter_tank_str = 0
			for tank_str in tanks['userStr'].keys():
				try:
					key = tank_str
					value = tanks['userStr'][key]
					res = await dbc.find_one({'_id': key, 'value': value})
					if res == None:
						await dbc.update_one({ '_id': key}, { '$set' : { 'value': value}},  upsert=True)
						counter_tank_str += 1
				except Exception as err:
					bu.error('Unexpected error', err)
			bu.verbose_std('Added/updated ' + str(counter_tank_str) + ' tank strings')	
			return True			
	else:
		bu.error('--file argument not set')
	return False


async def mk_playerQ(queue : asyncio.Queue, account_ids : list):
	"""Create queue of replays to post"""
	for account_id in account_ids:
		if account_id < 31e8:
			await queue.put(account_id)
		else:
			debug_account_id(account_id, 'Chinese account. Cannot retrieve stats, skipping.')
	return None


async def del_account_id(db: motor.motor_asyncio.AsyncIOMotorDatabase, account_id: int):
	"""Remove account_id from the DB"""
	dbc = db[DB_C_ACCOUNTS]
	try: 
		await dbc.delete_one({ '_id': account_id } )		
	except Exception as err:
		error_account_id(account_id, 'Unexpected error', exception=err)
	finally:
		debug_account_id(account_id, 'Removed account_id from the DB')
	return None


async def update_account(db: motor.motor_asyncio.AsyncIOMotorDatabase, account_id: int, stat_type: str = None, fields: dict = None, unset : bool = False) -> bool:
	"""Low-level helper function to update account collection"""
	try:
		dbc = db[DB_C_ACCOUNTS]
		if fields == None:
			FIELDS = fields
		else:
			FIELDS = dict()
		if stat_type != None:
			FIELDS[UPDATE_FIELD[stat_type]] = bu.NOW()
		
		if FIELDS == dict():
			return False
		if unset:
			await dbc.update_one({ '_id': account_id }, { '$unset': FIELDS } )
		else:
			await dbc.update_one({ '_id': account_id }, { '$set': FIELDS } )
		return True
	except Exception as err:
		error_account_id(account_id, 'Error updating account', exception=err)	


async def set_account_invalid(db: motor.motor_asyncio.AsyncIOMotorDatabase, account_id: int, stat_type: str = None):
	"""Set account_id invalid"""
	# dbc = db[DB_C_ACCOUNTS]
	try: 
		FIELDS = { 'invalid': True }
		#await dbc.update_one({ '_id': account_id }, { '$set': {'invalid': True }} )
		await update_account(db, account_id, stat_type, FIELDS )
	except Exception as err:
		error_account_id(account_id, 'Unexpected error', exception=err)
	finally:
		debug_account_id(account_id, 'Marked as invalid')
	return None


async def set_account_valid(db: motor.motor_asyncio.AsyncIOMotorDatabase, account_id: int):
	"""Set account_id valid"""
	#dbc = db[DB_C_ACCOUNTS]
	try: 
		# await dbc.update_one({ '_id': account_id }, { '$unset': {'invalid': "" }} )		
		await update_account(db, account_id, fields={'invalid': "" }, unset=True)
	except Exception as err:
		error_account_id(account_id, 'Unexpected error', exception=err)
	finally:
		debug_account_id(account_id, 'Marked as valid')


## DEPRECIATED
async def BS_player_stat_worker(db : motor.motor_asyncio.AsyncIOMotorDatabase, playerQ : asyncio.Queue, worker_id : int, args : argparse.Namespace):
	"""Async Worker to process the player queue for BlitzStars.com Player stats"""
	dbc = db[DB_C_BS_PLAYER_STATS]
	field = 'player_stats_BS'

	clr_error_log 	= args.run_error_log
	force 			= args.force

	while True:
		account_id = await playerQ.get()
		bu.print_progress()
		try:
			
			url = None
			if (not force) and (len(await chk_accounts2update(db, account_id, field)) == 0):
				debug_account_id(account_id, 'Fresh-enough stats exists in the DB', worker_id)
			else:
				stats = await bs.get_player_stats(account_id)
				if stats == None:
					raise bu.StatsNotFound('BlitzStars player stats not found: account_id ' + str(account_id))
				last_battle_time = -1
				for stat in stats:
					last_battle_time = max(last_battle_time, stat['last_battle_time'])
				try: 
					await dbc.insert_many(stats, ordered=False)
				except pymongo.errors.BulkWriteError as err:
					pass
				finally:
					await update_stats_update_time(db, account_id, field, last_battle_time)
					debug_account_id(account_id, 'Stats added', id=worker_id)		
		except bu.StatsNotFound as err:
			bu.debug(exception=err, id=worker_id)
			await log_error(db, account_id, field, clr_error_log)		
		except Exception as err:
			bu.error('Unexpected error: ' + ((' URL: ' + url) if url!= None else ""), exception=err, id=worker_id)
			await log_error(db, account_id, field, clr_error_log)
		finally:
			if clr_error_log:
				await clear_error_log(db, account_id, field)
			playerQ.task_done()	
	return None


## DEPRECIATED
async def BS_tank_stat_worker(db : motor.motor_asyncio.AsyncIOMotorDatabase, playerQ : asyncio.Queue, worker_id : int, args : argparse.Namespace):
	"""Async Worker to fetch players' tank stats from BlitzStars.com"""
	global stats_added
	
	dbc = db[DB_C_TANK_STATS]   # WG_TankStats
	field = 'tank_stats_BS'

	clr_error_log 	= args.run_error_log
	force 			= args.force

	while True:
		account_id = await playerQ.get()
		bu.print_progress()
		try:
			url = None
			if (not force) and (len(await chk_accounts2update(db, account_id, field)) == 0 ):
				debug_account_id(account_id, 'Fresh-enough stats exists in the DB', id=worker_id)
			else:
					
				stats = await bs.get_player_tank_stats(account_id, cache=False)				
				if (stats == None) or (len(stats) == 0):
					debug_account_id(account_id, 'Did not receive stats from BlitzStars', id=worker_id)
				else:
					stats = await bs.tank_stats2WG(stats)  ## Stats conversion
					tank_stats = []

					for tank_stat in stats:
						#bu.debug(str(tank_stat))
						account_id 			= tank_stat['account_id'] 
						tank_id 			= tank_stat['tank_id']
						last_battle_time 	= tank_stat['last_battle_time']
						tank_stat['_id']  	= mk_id(account_id, tank_id, last_battle_time)
						tank_stats.append(tank_stat)
					try: 
						## Add functionality to filter out those stats that aretoo close to existing stats
						res = await dbc.insert_many(tank_stats, ordered=False)
						tmp = len(res.inserted_ids)
						stats_added += tmp
					except pymongo.errors.BulkWriteError as err:
						tmp = err.details['nInserted']						
						stats_added += tmp								
					finally:
						await update_stats_update_time(db, account_id, field, last_battle_time)
						debug_account_id(account_id, 'Added ' + str(tmp) + ' stats', id=worker_id)	

		except Exception as err:
			error_account_id(account_id, 'Unexpected error: ' + ((' URL: ' + url) if url!= None else ""), exception=err, id=worker_id)
			await log_error(db, account_id, field, clr_error_log)
		finally:
			if clr_error_log:
				await clear_error_log(db, account_id, field)
			playerQ.task_done()				
	return None


async def WG_tank_stat_worker(db : motor.motor_asyncio.AsyncIOMotorDatabase, playerQ : asyncio.Queue, worker_id : int, args : argparse.Namespace):
	"""Async Worker to process the replay queue: WG player tank stats """
	global stats_added
	
	dbc = db[DB_C_TANK_STATS]
	stat_type = 'tank_stats'

	chk_invalid		= args.chk_invalid
	clr_error_log 	= args.run_error_log
	
	stat_logger = RecordLogger()

	while not playerQ.empty():
		try:
			account_id = await playerQ.get()
			debug_account_id(account_id, 'Fetching tank stats', id=worker_id)
			bu.print_progress()
					
			url = None
			added = 0
			inactive = None
			latest_battle = None

			stats = await wg.get_player_tank_stats(account_id, cache=False)
			if stats == None:				
				raise bu.StatsNotFound('WG API return NULL stats for ' + str(account_id))
			tank_stats = []
			latest_battle = 0
			for tank_stat in stats:
				tank_id 			= tank_stat['tank_id']
				last_battle_time 	= tank_stat['last_battle_time']
				tank_stat['_id']  	= mk_id(account_id, tank_id, last_battle_time)
				tank_stats.append(tank_stat)

				if (last_battle_time > latest_battle):
					latest_battle = last_battle_time 
			# RECOMMENDATION TO USE SINGLE INSERTS OVER MANY
			try: 
				res = await dbc.insert_many(tank_stats, ordered=False)
				added = len(res.inserted_ids)
				stats_added += added					
			except pymongo.errors.BulkWriteError as err:
				added = err.details['nInserted']
				stats_added += added										
			finally:
				stat_logger.log('accounts with tank stats')
				stat_logger.log('tank stats')
				if clr_error_log:
					await clear_error_log(db, account_id, stat_type)
				if chk_invalid:
					stat_logger.log('invalid accounts marked valid')
					set_account_valid(db, account_id)
				if added == 0:
					stat_logger.log('account inactive')
					inactive = True
				await update_stats_update_time(db, account_id, stat_type, latest_battle, inactive)
				debug_account_id(account_id, str(added) + 'Tank stats added', id=worker_id)			
		except bu.StatsNotFound as err:
			stat_logger.log('accounts without tank stats')
			log_account_id(account_id, exception=err, id=worker_id)
			await log_error(db, account_id, stat_type, clr_error_log, chk_invalid)
		except Exception as err:
			stat_logger.log('accounts with errors')
			error_account_id(account_id, 'Unexpected error: ' + ((' URL: ' + url) if url!= None else ""), exception=err, id=worker_id)
			await log_error(db, account_id, stat_type, clr_error_log, chk_invalid)
		finally:
			playerQ.task_done()	
	return stat_logger


## NOT IMPLEMENTED YET
async def WG_player_stat_worker(db : motor.motor_asyncio.AsyncIOMotorDatabase, playerQ : asyncio.Queue, worker_id : int, args : argparse.Namespace):
	"""Async Worker to process the replay queue: WG player stats """
	global stats_added
	#### 
	bu.error('NOT IMPLEMENTED YET')
	sys.exit(1)
	####
	dbc = db[DB_C_PLAYER_STATS]
	stat_type = 'player_stats'

	clr_error_log 	= args.run_error_log
	chk_invalid		= args.chk_invalid
	
	while True:
		try:
			account_id = await playerQ.get()
			bu.print_progress()
			debug_account_id(account_id, 'Fetching player stats', id=worker_id)

			url = None
			
			stats = await wg.get_player_stats(account_id, cache=False)
			if stats == None:
				raise bu.StatsNotFound('WG API return NULL stats for ' + str(account_id))
		
			last_battle_time = stats['last_battle_time']
			try: 
				await dbc.insert_one(stats, ordered=False)
				stats_added += 1
		
			finally:
				if clr_error_log:
					await clear_error_log(db, account_id, stat_type)
				if chk_invalid:
					set_account_invalid(db, account_id)
				await update_stats_update_time(db, account_id, stat_type, last_battle_time)
				debug_account_id(account_id, 'Player stats added', id=worker_id)	
	
		except bu.StatsNotFound as err:
			log_account_id(account_id, exception=err, id=worker_id)
			await log_error(db, account_id, stat_type, clr_error_log)
		except Exception as err:
			error_account_id(account_id, 'Unexpected error: ' + ((' URL: ' + url) if url!= None else ""), exception=err, id=worker_id)
			await log_error(db, account_id, stat_type, clr_error_log)
		finally:
			if clr_error_log:
				await clear_error_log(db, account_id, stat_type)
			playerQ.task_done()	
	return None


async def WG_player_achivements_worker(db : motor.motor_asyncio.AsyncIOMotorDatabase, playerQ : asyncio.Queue, worker_id : int, args : argparse.Namespace):
	"""Async Worker to process the replay queue: WG player stats """
	global stats_added
	
	dbc = db[DB_C_PLAYER_ACHIVEMENTS]
	stat_type = 'player_achievements'

	clr_error_log 	= args.run_error_log
	chk_invalid		= args.chk_invalid
	
	players = dict()
	server = None
	account_ids = None
	for server in WG.URL_WG_SERVER.keys():
		players[server] = list()

	while True:
		
		try:
			while not playerQ.empty():
				account_id = await playerQ.get()
				server = wg.get_server(account_id)
				if server != None:
					players[server].append(account_id)
					if len(players[server]) == 100:
						account_ids = players[server]
						players[server] = list()
						break
			
			if (account_ids == None) and playerQ.empty():
				for server in WG.URL_WG_SERVER.keys():
					if len(players[server]) > 0:
						account_ids = players[server]
						players[server] = list()
						break # for
			
			if (account_ids == None) :
				bu.debug('playerQ is empty', id=worker_id)
				break	# Nothing to do. Break from the outer while-loop
			
			bu.debug('Server: ' + server + ' account_ids: ' + str(len(account_ids)), id=worker_id)

		except Exception as err:
			bu.error('Unexpected error in generation of account_id list: ', exception=err, id=worker_id)		
		
		try:
			N_account_ids = len(account_ids)  # needed to keep count on finished tasks
			NOW = bu.NOW()	
			stats = await wg.get_player_achievements(account_ids, cache=False)
			if stats == None:
				raise bu.StatsNotFound('WG API returned NULL stats')
			# players_achivements = []			
			for account_id in account_ids:
				try:
					bu.print_progress()
					if (str(account_id) not in stats) or (stats[str(account_id)] == None):
						raise bu.StatsNotFound("No stats found for account_id = " + str(account_id))
					
					stat 				= stats[str(account_id)]
					stat['account_id'] 	= account_id
					stat['updated'] 	= NOW
					stat['_id'] 		= mk_id(account_id, 0, NOW)
					
					# RECOMMENDATION TO USE SINGLE INSERTS OVER MANY
					await dbc.insert_one(stat)					
					stats_added += 1
					# players_achivements.append(stat)
					# res = await dbc.insert_many(players_achivements, ordered=False)
					# tmp = len(res.inserted_ids)
					# bu.debug(str(tmp) + ' stats added (insert_many() result)', worker_id)
					# stats_added += tmp
					# #stats_added += len(res.inserted_ids)	
					debug_account_id(account_id, 'stats added', id=worker_id)
					if clr_error_log:
						await clear_error_log(db, account_id, stat_type)
					if chk_invalid:
						set_account_valid(db, account_id)
					await update_stats_update_time(db, account_id, stat_type, NOW)
				except bu.StatsNotFound as err:	
					log_account_id(account_id, exception=err, id=worker_id)
					await log_error(db, account_id, stat_type, clr_error_log, chk_invalid)
				except Exception as err:
					error_account_id(account_id, 'Failed to store stats', exception=err, id=worker_id)
					#error_account_id(account_id, 'Failed to store player achievement stats', id=worker_id)
					await log_error(db, account_id, stat_type, clr_error_log, chk_invalid)
					
		except bu.StatsNotFound as err:	
			bu.log('Error fetching player achievement stats', exception=err, id=worker_id)
			for account_id in account_ids:
				await log_error(db, account_id, stat_type, clr_error_log, chk_invalid)
		except Exception as err:
			bu.error('Unexpected error in fetching: ', exception=err, id=worker_id)
			for account_id in account_ids:
				await log_error(db, account_id, stat_type, clr_error_log, chk_invalid)
		finally:			
			# update task queue status	
			for _ in range(N_account_ids):
				playerQ.task_done()
			account_ids = None
	return None


async def log_error(db : motor.motor_asyncio.AsyncIOMotorDatabase, account_id: int, stat_type: str, 
					clr_error_log: bool = False, chk_invalid: bool = False):
	dbc = db[DB_C_ERROR_LOG]
	try:
		if clr_error_log:
			log_account_id(account_id, 'Setting account invalid')
			await set_account_invalid(db, account_id, stat_type)
			await clear_error_log(db, account_id, stat_type)
		elif chk_invalid:
			debug_account_id(account_id, 'Still invalid')
			await clear_error_log(db, account_id, stat_type)
		else:
			log_account_id(account_id, 'Could not fetch stats: ' + stat_type)
			await dbc.insert_one( {'account_id': account_id, 'type': stat_type, 'time': bu.NOW() } )
			#bu.debug('Logging Error: account_id=' + str(account_id) + ' stat_type=' + stat_type)
	except Exception as err:
		bu.error('Unexpected error', err)


async def clear_error_log(db : motor.motor_asyncio.AsyncIOMotorDatabase, account_id: int, stat_type: str):
	"""Delete ErrorLog entry for account_id, stat_type"""
	dbc = db[DB_C_ERROR_LOG]
	await dbc.delete_many({ '$and': [ {'account_id': account_id}, {'type': stat_type }]})


def log_account_id(account_id: int, msg: str = '', id: int = None, exception: Exception = None):
	return bu.log('account_id=' + str(account_id).ljust(10) + ': ' + msg, id=id, exception=exception)


def debug_account_id(account_id: int, msg: str = '', id: int = None, exception: Exception = None, force: bool = False):
	return bu.debug('account_id=' + str(account_id).ljust(10) + ': ' + msg, id=id, exception=exception, force=force)


def error_account_id(account_id: int, msg: str = '', id: int = None, exception: Exception = None):
	return bu.error('account_id=' + str(account_id).ljust(10) + ': ' + msg, id=id, exception=exception)



def mk_id(account_id: int, tank_id: int, last_battle_time: int) -> str:
	return hex(account_id)[2:].zfill(10) + hex(tank_id)[2:].zfill(6) + hex(last_battle_time)[2:].zfill(8)


### main()
if __name__ == "__main__":
   #asyncio.run(main(sys.argv[1:]), debug=True)
   asyncio.run(main(sys.argv[1:]))
