#!/usr/bin/env python3

# Script fetch Blitz player stats and tank stats

import sys, argparse, json, os, inspect, pprint, aiohttp, asyncio, aiofiles, aioconsole
import re, logging, time, xmltodict, collections, pymongo
import motor.motor_asyncio, ssl, configparser, random, datetime
import blitzstatsutils as su
import blitzutils as bu
from blitzutils import BlitzStars, WG, RecordLogger, read_int_list

logging.getLogger("asyncio").setLevel(logging.DEBUG)

N_WORKERS = 60
MAX_RETRIES = 3
CACHE_VALID = 10   # days
MAX_UPDATE_INTERVAL = 4*30*24*60*60 # 4 months
INACTIVE_THRESHOLD 	= 2*30*24*60*60 # 2 months
MIN_INACTIVITY_PERIOD = 7 # days
SLEEP = 0.1
WG_APP_ID = 'cd770f38988839d7ab858d1cbe54bdd0'

FILE_CONFIG = 'blitzstats.ini'
FILE_ACTIVE_PLAYERS='activeinlast30days.json'

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
	WG_RATE_LIMIT	= 10  ## WG standard. Do not edit unless you have your
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
	parser.add_argument('--mode', default='help', nargs='+', choices=get_modes(), help='Choose what to update')
	parser.add_argument('--file', default=None, help='JSON file to read')
	parser.add_argument('--force', action='store_true', default=False, help='Force refreshing the active player list')
	parser.add_argument('--workers', type=int, default=N_WORKERS, help='Number of asynchronous workers')
	parser.add_argument('--cache-valid', type=int, dest='cache_valid', default=CACHE_VALID, help='Do not update stats newer than N Days')
	parser.add_argument('--player-src', dest='player_src', default='db', choices=[ 'db', 'blitzstars', 'file' ], help='Source for the account list. Default: db')
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
		#await su.init_db_indices(db)

		## get active player list ------------------------------
		active_players = {}
		start_time = 0
		rl = RecordLogger('Update stats')
		for mode in get_modes(args.mode):
			
			if mode == su.MODE_TANKOPEDIA:
				await update_tankopedia(db, args.file, args.force)
			else:
				start_time 		= print_date('DB update started: ' + su.get_mode_str(mode))
				active_players  = await get_active_players(db, mode, args)
				accountQ 		= asyncio.Queue()
				Qcreator 		= asyncio.create_task(mk_accountQ(accountQ, active_players))
				workers 		= []
				bu.set_progress_bar('Fetching stats', len(active_players), 200, True)

				if mode == su.MODE_TANK_STATS:
					for i in range(args.workers):
						workers.append(asyncio.create_task(WG_tank_stat_worker( db, accountQ, i, args )))
						bu.debug('WG Tank stat worker started', id=i)	
				
				elif mode == su.MODE_PLAYER_ACHIEVEMENTS: 
					for i in range(args.workers):
						workers.append(asyncio.create_task(WG_player_achivements_worker(db, accountQ, i, args )))
						bu.debug('WG Player achievement stat Worker started', id=i)

				elif mode == su.MODE_PLAYER_STATS:					
					bu.error('Fetching WG player stats NOT IMPLEMENTED YET')

				elif mode == su.MODE_BS_TANK_STATS:
					for i in range(args.workers):
						workers.append(asyncio.create_task(BS_tank_stat_worker(db, accountQ, i, args )))
						bu.debug('BlitzStars Tank stat Worker started', id=i)	
			
				elif mode == su.MODE_BS_PLAYER_STATS:
					for i in range(args.workers):
						workers.append(asyncio.create_task(BS_player_stat_worker(db, accountQ, i, args )))
						bu.debug('BlitzStars Player stat Worker starte', id=i)
					
				## wait queues to finish --------------------------------------
				bu.log('Waiting for the work queue makers to finish')
				await asyncio.wait([Qcreator])
				bu.log('All active players added to the queue. Waiting for stat workers to finish')
				await accountQ.join()		
				bu.finish_progress_bar()

				if len(workers) > 0:
					bu.log('All work queues empty. Cancelling workers')
					for worker in workers:
						worker.cancel()
					bu.log('Waiting for workers to cancel')
					for rl_worker in await asyncio.gather(*workers, return_exceptions=True):
						rl.merge(rl_worker)
				
				bu.print_new_line(True)
				print_date('DB update ended', start_time)
				bu.print_new_line(True)
	except asyncio.CancelledError as err:
		bu.error('Queue got cancelled while still working.')
	except Exception as err:
		bu.error('Unexpected Exception', err)
	finally:
		if (args.sample == 0) and (not args.run_error_log):
			# only for full stats
			for mode in args.mode:
				await su.update_log(db, 'update', stat_type=mode)
		
		wg.print_request_stats()
		bu.print_new_line(True)
		rl.print()		
		await bs.close()
		await wg.close()
		if args.log:
			await bu.close_file_logging()

	return None


def print_date(msg : str = '', start_time : datetime.datetime = None ) -> datetime.datetime:
	try:
		timenow = datetime.datetime.now()
		bu.verbose_std(msg + ': ' + timenow.replace(microsecond=0).isoformat(sep=' '))
		if start_time != None:
			delta = timenow - start_time
			secs = delta.total_seconds()
			hours = int(secs // 3600)
			minutes = int(secs // 60)
			bu.verbose_std('The processing took ' + str(hours) + 'h ' + str(minutes) + 'min')
		return timenow
	except Exception as err:
		bu.error('Unexpected Exception', err)


def get_modes(mode_list: list = None) -> list:
	"""Return modes of fetching stats (i.e. NOT tankopedia)"""
	try:
		modes = set(su.UPDATE_FIELD.keys()) | set([su.MODE_TANKOPEDIA]) - set(['default'])
		if mode_list != None:
			modes = modes & set(mode_list)
		return list(modes)
	except Exception as err:
		bu.error('Unexpected Exception', err)
	return None


def get_stat_modes(mode_list: list = None) -> list:
	"""Return modes of fetching stats (i.e. NOT tankopedia)"""
	try:
		return list( set(get_modes(mode_list)) - set([su.MODE_TANKOPEDIA]) )
	except Exception as err:
		bu.error('Unexpected Exception', err)
	return None


def get_stat_modes_WG(mode_list: list = None) -> list:
	"""Return modes of fetching stats from WG API"""
	try:
		return list(set(get_modes(mode_list)) & set( [ su.MODE_TANK_STATS, su.MODE_PLAYER_STATS, su.MODE_PLAYER_ACHIEVEMENTS] ))
	except Exception as err:
		bu.error('Unexpected Exception', err)
	return None


def get_stat_modes_BS(mode_list: list = None) -> list:
	"""Return modes of fetching stats from BlitzStars"""
	try:
		return list(set(get_modes(mode_list)) & set( [ su.MODE_BS_TANK_STATS, su.MODE_BS_PLAYER_STATS ] ))
	except Exception as err:
		bu.error('Unexpected Exception', err)
	return None


def mk_account(account_id: int, inactive: bool = False ) -> dict:
	return { 'account_id': account_id, 'inactive': inactive }


async def get_active_players(db : motor.motor_asyncio.AsyncIOMotorDatabase, 
							 mode: str, args : argparse.Namespace) -> list:
	"""Get active player list from the sources (DB, BlitzStars)"""
	try:
		sample 			= args.sample
		player_src 		= args.player_src
		run_error_log 	= args.run_error_log

		if run_error_log:
			bu.debug('Getting players from the Error Log: ' + mode)
			players = await get_players_errorlog(db, mode)
		elif player_src == 'blitzstars':
			bu.debug('Getting players from BlitzStars: ' + mode)
			players = await get_active_players_BS(args)
		elif player_src == 'db':
			bu.debug('Getting players from DB: ' + mode)
			if mode in get_stat_modes_BS():
				bu.error('Cannot user DB as the player source for mode=' + mode)
				return None
			players = await get_active_players_DB(db, mode, args)
		elif player_src == 'file':
			players = await get_active_players_file(args.file)

		if sample > 0 and sample < len(players):
			players = random.sample(players, sample)
		random.shuffle(players)  # randomize
		return players
	except Exception as err:
		bu.error('Unexpected Exception', err)
	return None


async def get_active_players_DB(db : motor.motor_asyncio.AsyncIOMotorDatabase, 
								mode: str, args : argparse.Namespace) -> list:
	"""Get list of active accounts from the database"""
	try:
		dbc = db[su.DB_C_ACCOUNTS]
		
		force 			= args.force
		cache_valid 	= args.cache_valid
		sample 			= args.sample
		chk_invalid 	= args.chk_invalid
		update_field 	= su.UPDATE_FIELD[mode]
		DB_SAMPLE_FACTOR = 2 # > share of inactive players
		NOW = bu.NOW()

		match = [ { '_id' : {  '$lt' : WG.ACCOUNT_ID_MAX}}, { 'invalid': { '$exists': chk_invalid }} ]
		if not force:
			match.append( { '$or': [ { update_field : None }, { update_field : { '$lt': NOW - cache_valid}} ] } )
			
		pipeline = [ { '$match' : { '$and' : match } } ]
		if sample > 0:
			pipeline.append({'$sample': {'size' : sample * DB_SAMPLE_FACTOR} })

		cursor = dbc.aggregate(pipeline, allowDiskUse=False)
		
		account_ids = list()
		i = 0		
		tmp_steps = bu.get_progress_step()
		bu.set_progress_step(1000)
		bu.set_counter('Fetching players:')

		async for player in cursor:
			try:
				i += 1
				inactive = False
				if bu.print_progress():
					bu.debug('Accounts read from DB: ' + str(i))
				try: 
					if ('inactive' in player) and player['inactive']:
						inactive = True
						if not (force or chk_invalid):    # enable forced recheck of accounts marked not inactive
							latest_battle_time = min(player['latest_battle_time'], NOW)
							if (NOW - player[update_field]) < min(MAX_UPDATE_INTERVAL, (player[update_field] - latest_battle_time)/2):
								continue
				except:
					# not all fields in the account record. SKIP since the account is inactive
					continue
				account_ids.append(mk_account(player['_id'], inactive))
			except Exception as err:
				bu.error('account_id=' + str(player), err)

		bu.finish_progress_bar()
		bu.set_progress_step(tmp_steps)
		
		bu.log(str(len(account_ids)) + ' read from the DB')
		return account_ids
	except Exception as err:
		bu.error('Unexpected error', err)


async def get_active_players_BS(args : argparse.Namespace)  -> list:
	"""Get active_players from BlitzStars or local cache file"""
	global bs, wg

	force 		= args.force
	cache_valid = args.cache_valid

	active_players = None
	if force or (not (os.path.exists(FILE_ACTIVE_PLAYERS))) or \
		( os.path.isfile(FILE_ACTIVE_PLAYERS) and (bu.NOW() - os.path.getmtime(FILE_ACTIVE_PLAYERS) > cache_valid) ):
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
	account_ids = list()
	for account_id in active_players:
		account_ids.append(mk_account(account_id, False))
	return account_ids


async def get_active_players_file(filename: str) -> list:
	"""Read account_ids from a text file"""
	try:
		players = list()
		account_ids = await read_int_list(filename)
		for account_id in account_ids:
			players.append(mk_account(account_id=account_id))
	except Exception as err:
		bu.error('Unexpected error', err)	
	return players
				

async def get_players_errorlog(db : motor.motor_asyncio.AsyncIOMotorDatabase, mode :str) -> list:
	"""Get list of acccount_ids of the previous failed requests"""
	try:
		dbc = db[su.DB_C_ERROR_LOG]
		account_ids =  set()
		
		cursor = dbc.find({'type': mode}, { 'account_id': 1, '_id': 0 } )
		async for stat in cursor:
			try:
				# inactive = False
				# if ('inactive' in stat):
				# 	inactive = stat['inactive']
				account_ids.add(stat['account_id'])
			except Exception as err:
				bu.error('Unexpected error', err)
		
		res = list()
		for account_id in account_ids:
			res.append(mk_account(account_id, False))
		bu.verbose_std('Re-checking ' + str(len(res)) + ' account_ids')		
		return res
	except Exception as err:
		bu.error('Unexpected error', err)	
	return None


async def update_stats_update_time(db : motor.motor_asyncio.AsyncIOMotorDatabase, 
									account_id: int, stat_type: str, 
									last_battle_time: int = None, inactive: bool = None) -> bool:
	"""Update su.DB_C_ACCOUNTS table with the update time of the player's stats"""

	dbc_update_log = db[su.DB_C_ACCOUNT_LOG]
	try:
		#await dbc.update_one( { '_id' : account_id }, { '$set': { 'last_battle_time': last_battle_time, su.UPDATE_FIELD[stat_type] : bu.NOW(), 'inactive': inactive }} )
		#await dbc.update_one( { '_id' : account_id }, { '$set': { 'last_battle_time': last_battle_time, su.UPDATE_FIELD[stat_type] : bu.NOW() }} )
		FIELDS = dict()

		if last_battle_time != None:
			FIELDS['last_battle_time'] = last_battle_time
		if inactive != None:
			FIELDS['inactive'] = inactive

		# await dbc.update_one( { '_id' : account_id }, { '$set': FIELDS } )
		await update_account(db, account_id, stat_type, set_fields= FIELDS)
		
		## Added 2020-12-09 to keep log of updated account IDs
		await dbc_update_log.insert_one({ 'mode': stat_type, 'account_id' : account_id, 'updated': bu.NOW()})
		
		return True
	except Exception as err:
		error_account_id(account_id, 'Unexpected error', exception=err)
		return False	


async def update_tankopedia( db: motor.motor_asyncio.AsyncIOMotorDatabase, 
								filename: str = None, force: bool = False) -> bool:
	"""Update tankopedia in the database"""
	dbc = db[su.DB_C_TANKOPEDIA]
	if filename == None:
		filename = 'tanks.json'
	async with aiofiles.open(filename, 'rt', encoding="utf8") as fp:
		# Update Tankopedia
		tanks = json.loads(await fp.read())
		inserted = 0
		updated = 0
		tanks['data'] = bu.sort_dict(tanks['data'], number=True) 
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
		dbc = db[su.DB_C_TANK_STR]
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
	

async def mk_accountQ(queue : asyncio.Queue, account_ids : list) -> None:
	"""Create queue of replays to post"""
	for account_id in account_ids:
		try:
			if account_id['account_id'] < 31e8:
				await queue.put(account_id)
			else:
				debug_account_id(account_id, 'Chinese account. Cannot retrieve stats, skipping.')
		except:
			bu.error('No account_id found')
	return None


async def del_account_id(db: motor.motor_asyncio.AsyncIOMotorDatabase, account_id: int) -> None:
	"""Remove account_id from the DB"""
	dbc = db[su.DB_C_ACCOUNTS]
	try: 
		await dbc.delete_one({ '_id': account_id } )		
	except Exception as err:
		error_account_id(account_id, 'Unexpected error', exception=err)
	finally:
		debug_account_id(account_id, 'Removed account_id from the DB')
	return None


async def update_account(db: motor.motor_asyncio.AsyncIOMotorDatabase, 
						  account_id: int, stat_type: str = None, set_fields: dict = dict(), 
						  unset_fields : dict = dict()) -> bool:
	"""Low-level helper function to update account collection"""
	try:
		dbc = db[su.DB_C_ACCOUNTS]
		# if fields != None:
		# 	FIELDS = fields
		# else:
		# 	FIELDS = dict()
		# if (stat_type != None) and not unset:	
		if stat_type != None:
			set_fields[su.UPDATE_FIELD[stat_type]] = bu.NOW()
		else:
			## IS this really sensible
			set_fields[su.UPDATE_FIELD['default']] = bu.NOW()

		if len(unset_fields) > 0:
			await dbc.update_one({ '_id': account_id }, {  '$set': set_fields, '$unset': unset_fields } )
		else:
			await dbc.update_one({ '_id': account_id }, {  '$set': set_fields } )
		return True
	except Exception as err:
		error_account_id(account_id, 'Error updating account', exception=err)	
	return False


async def set_account_invalid(db: motor.motor_asyncio.AsyncIOMotorDatabase, 
								account_id: int, stat_type: str = None) -> None:
	"""Set account_id invalid"""
	try: 
		FIELDS = { 'invalid': True }
		await update_account(db, account_id, stat_type, set_fields=FIELDS )
	except Exception as err:
		error_account_id(account_id, 'Unexpected error', exception=err)
	finally:
		debug_account_id(account_id, 'Marked as invalid')
	return None


async def set_account_valid(db: motor.motor_asyncio.AsyncIOMotorDatabase, account_id: int):
	"""Set account_id valid"""
	try: 
		await update_account(db, account_id, unset_fields={'invalid': "" })
	except Exception as err:
		error_account_id(account_id, 'Unexpected error', exception=err)
	finally:
		debug_account_id(account_id, 'Marked as valid')


## DEPRECIATED
async def BS_player_stat_worker(db : motor.motor_asyncio.AsyncIOMotorDatabase, 
								playerQ : asyncio.Queue, worker_id : int, 
								args : argparse.Namespace):
	"""Async Worker to process the player queue for BlitzStars.com Player stats"""
	dbc = db[su.DB_C_BS_PLAYER_STATS]
	field = 'player_stats_BS'

	clr_error_log 	= args.run_error_log
	
	while True:
		player = await playerQ.get()
		account_id = player['account_id']
		bu.print_progress()
		try:			
			url = None
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
async def BS_tank_stat_worker(db : motor.motor_asyncio.AsyncIOMotorDatabase, 
								playerQ : asyncio.Queue, worker_id : int, 
								args : argparse.Namespace):
	"""Async Worker to fetch players' tank stats from BlitzStars.com"""
	global stats_added
	
	dbc = db[su.DB_C_TANK_STATS]   # WG_TankStats
	field = 'tank_stats_BS'

	clr_error_log 	= args.run_error_log

	while True:
		player  = await playerQ.get()
		account_id = player['account_id']
		bu.print_progress()
		try:
			url = None
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
					tank_stat['_id']  	= su.mk_id(account_id, last_battle_time, tank_id)
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


async def WG_tank_stat_worker(db : motor.motor_asyncio.AsyncIOMotorDatabase, 
								playerQ : asyncio.Queue, worker_id : int, 
								args : argparse.Namespace) -> RecordLogger:
	"""Async Worker to process the replay queue: WG player tank stats """
	global stats_added
	
	dbc = db[su.DB_C_TANK_STATS]
	stat_type = su.MODE_TANK_STATS

	chk_invalid		= args.chk_invalid
	clr_error_log 	= args.run_error_log
	cache_valid		= args.cache_valid
	force 			= args.force
	mark_inactive = True
	if ('sample' in args) or (cache_valid < MIN_INACTIVITY_PERIOD):
		mark_inactive = False
	rl = RecordLogger('Tank stats')

	try:
		while True:
			player = await playerQ.get()
			try:				
				account_id 	= player['account_id']
				inactive 	= player['inactive']
				debug_account_id(account_id, 'Fetching tank stats', id=worker_id)
				bu.print_progress()
						
				url = None
				added = 0			
				latest_battle = None

				stats = await wg.get_player_tank_stats(account_id, cache=False)
				if stats == None:				
					raise bu.StatsNotFound('WG API return NULL stats for ' + str(account_id))
				
				now = bu.NOW()
				tank_stats = []
				latest_battle = 0
				for tank_stat in stats:
					tank_id 			= tank_stat['tank_id']
					last_battle_time 	= tank_stat['last_battle_time']
					if last_battle_time > now:  # do not add "newer" than today (broken last_bvattle_time_field)
						rl.log('invalid')
						continue
					tank_stat['_id']  	= su.mk_id(account_id, last_battle_time, tank_id)
					#tank_stat[su.FIELD_NEW_STATS] = now  						## Needed for stats archiving
					tank_stats.append(tank_stat)
					if (last_battle_time > latest_battle): 
						latest_battle = last_battle_time 

				try: 
					res = await dbc.insert_many(tank_stats, ordered=False)
					added = len(res.inserted_ids)
					stats_added += added					
				except pymongo.errors.BulkWriteError as err:
					added = err.details['nInserted']
					stats_added += added										
				finally:
					rl.log('accounts updated')
					rl.log('added', added)
					if clr_error_log:
						await clear_error_log(db, account_id, stat_type)
					if chk_invalid:
						rl.log('accounts marked valid')
						set_account_valid(db, account_id)
					
					if added == 0:
						rl.log('accounts w/o new stats')
						if (now - latest_battle < INACTIVE_THRESHOLD) or force or (not mark_inactive):
							inactive = None
						else:
							if inactive:
								rl.log('accounts still inactive')
							else:
								rl.log('accounts marked inactive')
								inactive = True
					else:
						rl.log('accounts with new stats')
						if inactive:
							rl.log('accounts marked active')						
						inactive = False
					await update_stats_update_time(db, account_id, stat_type, latest_battle, inactive)
					debug_account_id(account_id, str(added) + ' Tank stats added', id=worker_id)			
			except bu.StatsNotFound as err:
				rl.log('accounts without stats')
				log_account_id(account_id, exception=err, id=worker_id)
				await log_error(db, account_id, stat_type, clr_error_log, chk_invalid)
			except Exception as err:
				rl.log('unknown errors')
				error_account_id(account_id, 'Unexpected error: ' + ((' URL: ' + url) if url!= None else ""), exception=err, id=worker_id)
				await log_error(db, account_id, stat_type, clr_error_log, chk_invalid)
			finally:
				playerQ.task_done()	

	except asyncio.CancelledError:
		bu.debug('Cancelled')
	return rl


## NOT IMPLEMENTED YET
async def WG_player_stat_worker(db : motor.motor_asyncio.AsyncIOMotorDatabase, 
								playerQ : asyncio.Queue, worker_id : int, 
								args : argparse.Namespace):
	"""Async Worker to process the replay queue: WG player stats """
	global stats_added
	#### 
	bu.error('NOT IMPLEMENTED YET')
	sys.exit(1)
	####
	dbc = db[su.DB_C_PLAYER_STATS]
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
					set_account_valid(db, account_id)
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


async def WG_player_achivements_worker(db : motor.motor_asyncio.AsyncIOMotorDatabase, 
										playerQ : asyncio.Queue, worker_id : int, 
										args : argparse.Namespace) -> RecordLogger:
	"""Async Worker to process the replay queue: WG player stats """
	global stats_added
	
	dbc = db[su.DB_C_PLAYER_ACHIVEMENTS]
	stat_type = 'player_achievements'

	clr_error_log 	= args.run_error_log
	chk_invalid		= args.chk_invalid
	
	players = dict()
	server = None
	account_ids = None

	rl = RecordLogger('Player achievements')

	for server in WG.URL_WG_SERVER.keys():
		players[server] = list()

	while True:
		try:
			while not playerQ.empty():
				player = await playerQ.get()
				account_id = player['account_id']
				server = wg.get_server(account_id)
				if server != None:
					players[server].append(account_id)
					if len(players[server]) == 100:
						account_ids = players[server]
						players[server] = list()
						break
				else:
					playerQ.task_done()
			
			if (account_ids == None) and playerQ.empty():
				for server in players:
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
					## Needed for stats archiving. Not yet implemented for player_achievements
					#stat[su.FIELD_NEW_STATS] = True
					stat['_id'] 		= su.mk_id(account_id, NOW)
					
					# RECOMMENDATION TO USE SINGLE INSERTS OVER MANY
					await dbc.insert_one(stat)					
					stats_added += 1
					rl.log('added')
					debug_account_id(account_id, 'stats updated', id=worker_id)
					if clr_error_log:
						await clear_error_log(db, account_id, stat_type)
					if chk_invalid:
						set_account_valid(db, account_id)
					await update_stats_update_time(db, account_id, stat_type)
				except bu.StatsNotFound as err:	
					log_account_id(account_id, exception=err, id=worker_id)
					rl.log('no stats found')
					await log_error(db, account_id, stat_type, clr_error_log, chk_invalid)
				except Exception as err:
					error_account_id(account_id, 'Failed to store stats', exception=err, id=worker_id)
					rl.log('errors')
					#error_account_id(account_id, 'Failed to store player achievement stats', id=worker_id)
					await log_error(db, account_id, stat_type, clr_error_log, chk_invalid)
					
		except bu.StatsNotFound as err:	
			rl.log('no stats found', len(account_ids))
			bu.log('Error fetching player achievement stats', exception=err, id=worker_id)
			for account_id in account_ids:
				await log_error(db, account_id, stat_type, clr_error_log, chk_invalid)
		except Exception as err:
			bu.error('Unexpected error in fetching: ', exception=err, id=worker_id)
			rl.log('errors', len(account_ids))
			for account_id in account_ids:
				await log_error(db, account_id, stat_type, clr_error_log, chk_invalid)
		finally:			
			# update task queue status	
			for _ in range(N_account_ids):
				playerQ.task_done()
			account_ids = None
	return rl


async def log_error(db : motor.motor_asyncio.AsyncIOMotorDatabase, account_id: int, stat_type: str, 
					clr_error_log: bool = False, chk_invalid: bool = False):
	dbc = db[su.DB_C_ERROR_LOG]
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
	dbc = db[su.DB_C_ERROR_LOG]
	await dbc.delete_many({ '$and': [ {'type': stat_type }, {'account_id': account_id}]})


def log_account_id(account_id: int, msg: str = '', id: int = None, exception: Exception = None):
	return bu.log('account_id=' + str(account_id).ljust(10) + ': ' + msg, id=id, exception=exception)


def debug_account_id(account_id: int, msg: str = '', id: int = None, exception: Exception = None, force: bool = False):
	return bu.debug('account_id=' + str(account_id).ljust(10) + ': ' + msg, id=id, exception=exception, force=force)


def error_account_id(account_id: int, msg: str = '', id: int = None, exception: Exception = None):
	return bu.error('account_id=' + str(account_id).ljust(10) + ': ' + msg, id=id, exception=exception)


### main()
if __name__ == "__main__":
   #asyncio.run(main(sys.argv[1:]), debug=True)
   asyncio.run(main(sys.argv[1:]))
