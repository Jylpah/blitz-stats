#!/usr/bin/env python3.8

# Script fetch Blitz player stats and tank stats

import sys, argparse, json, os, inspect, pprint, aiohttp, asyncio, aiofiles, aioconsole
import motor.motor_asyncio, ssl, lxml, re, logging, time, xmltodict, collections, pymongo
import configparser
import blitzutils as bu
from bs4 import BeautifulSoup
from blitzutils import BlitzStars, WG, WoTinspector

logging.getLogger("asyncio").setLevel(logging.DEBUG)

N_WORKERS = 20
MAX_PAGES = 10
MAX_RETRIES = 3
CACHE_VALID = 24*3600*5   # 5 days
SLEEP = 1
REPLAY_N = 0

WG_appID = 'cd770f38988839d7ab858d1cbe54bdd0'

FILE_ACTIVE_PLAYERS='activeinlast30days.json'
FILE_CONFIG = 'blitzstats.ini'

DB_C_ACCOUNTS   = 'WG_Accounts'
DB_C_REPLAYS   = 'Replays'

wi = None
bs = None
WI_STOP_SPIDER = False
WI_old_replay_N = 0
WI_old_replay_limit = 30

## main() -------------------------------------------------------------

async def main(argv):
	# set the directory for the script
	os.chdir(os.path.dirname(sys.argv[0]))

	global wi, bs

	parser = argparse.ArgumentParser(description='Fetch WG account_ids')
	parser.add_argument('--force', action='store_true', default=False, help='Force refreshing the active player list')
	parser.add_argument('--workers', type=int, default=N_WORKERS, help='Number of asynchronous workers')
	arggroup = parser.add_mutually_exclusive_group()
	arggroup.add_argument('-d', '--debug', 		action='store_true', default=False, help='Debug mode')
	arggroup.add_argument('-v', '--verbose', 	action='store_true', default=False, help='Verbose mode')
	arggroup.add_argument('-s', '--silent', 	action='store_true', default=False, help='Silent mode')
	parser.add_argument('-r', '--remove', 		action='store_true', default=False, help='REMOVE account_ids the database. Please give a plain text file with account_ids as cmd line argument.')
	parser.add_argument('-b', '--blitzstars', 	action='store_true', default=False, help='Get account_ids from blitzstars.com')
	parser.add_argument('-w', '--wotinspector', action='store_true', default=False, help='Get account_ids from WoTinspector.com')
	parser.add_argument('--db', 				action='store_true', default=False, help='Get account_ids from the database in case previous runs got interrupted')

	parser.add_argument('--max', '--max_pages', dest='max_pages', type=int, default=MAX_PAGES, help='Maximum number of WoTinspector.com pages to spider')
	parser.add_argument('--start', '--start_page', dest='start_page', type=int, default=1, help='Start page to start spidering of WoTinspector.com')
	parser.add_argument('files', metavar='FILE1 [FILE2 ...]', type=str, nargs='*', help='Files to read. Use \'-\' for STDIN')

	args = parser.parse_args()
	bu.set_log_level(args.silent, args.verbose, args.debug)
	bu.set_progress_step(100)
	
	players = set()
	try:
		bs = BlitzStars()

		## Read config
		config = configparser.ConfigParser()
		config.read(FILE_CONFIG)
		configDB 	= config['DATABASE']
		DB_SERVER 	= configDB.get('db_server', 'localhost')
		DB_PORT 	= configDB.getint('db_port', 27017)
		DB_SSL		= configDB.getboolean('db_ssl', False)
		DB_CERT_REQ = configDB.getint('db_ssl_req', ssl.CERT_NONE)
		DB_AUTH 	= configDB.get('db_auth', 'admin')
		DB_NAME 	= configDB.get('db_name', 'BlitzStats')
		DB_USER		= configDB.get('db_user', None)
		DB_PASSWD 	= configDB.get('db_password', None)
		DB_CERT		= configDB.get('db_ssl_cert_file', None)
		DB_CA		= configDB.get('db_ssl_ca_file', None)
		
		bu.debug('DB_SERVER: ' + DB_SERVER)
		bu.debug('DB_PORT: ' + str(DB_PORT))
		bu.debug('DB_SSL: ' + "True" if DB_SSL else "False")
		bu.debug('DB_AUTH: ' + DB_AUTH)
		bu.debug('DB_NAME: ' + DB_NAME)

		#### Connect to MongoDB
		if (DB_USER==None) or (DB_PASSWD==None):
			client = motor.motor_asyncio.AsyncIOMotorClient(DB_SERVER,DB_PORT, ssl=DB_SSL, ssl_cert_reqs=DB_CERT_REQ, ssl_certfile=DB_CERT, tlsCAFile=DB_CA)
		else:
			client = motor.motor_asyncio.AsyncIOMotorClient(DB_SERVER,DB_PORT, authSource=DB_AUTH, username=DB_USER, password=DB_PASSWD, ssl=DB_SSL, ssl_cert_reqs=DB_CERT_REQ, ssl_certfile=DB_CERT, tlsCAFile=DB_CA)

		db = client[DB_NAME]
		await db[DB_C_ACCOUNTS].create_index([ ('last_battle_time', pymongo.DESCENDING) ], background=True)	
		await db[DB_C_REPLAYS].create_index([('data.summary.battle_start_timestamp', pymongo.ASCENDING)], background=True)	
		await db[DB_C_REPLAYS].create_index([('data.summary.battle_start_timestamp', pymongo.ASCENDING), ('data.summary.vehicle_tier', pymongo.ASCENDING)], background=True)	

		if not args.remove:
			if args.blitzstars:
				players.update(await get_players_BS(args.force))
		
			if args.wotinspector:
				players.update(await get_players_WI(db, args))

			if args.db:
				players.update(await get_players_DB(db, args))
			
			if len(args.files) > 0:
				bu.debug(str(args.files))
				players.update(await get_players_replays(args.files, args.workers))

			await update_account_ids(db, players)
		
		else:
			await remove_account_ids(db, args.files)

	except asyncio.CancelledError as err:
		bu.error('Queue gets cancelled while still working.')
	except Exception as err:
		bu.error('Unexpected Exception: ' + str(type(err)) + ' : '+ str(err))
	finally:
		await bs.close()

	return None


async def remove_account_ids(db: motor.motor_asyncio.AsyncIOMotorDatabase, files: list):
	"""Remove account_ids from the DB"""
	dbc = db[DB_C_ACCOUNTS]

	account_ids = set()
	for file in files:
		account_ids.update(await bu.read_int_list(file))
	
	account_id_list = list(account_ids)
	bu.verbose_std('Deleting ' + str(len(account_id_list)) + ' account_ids')

	deleted_count = 0
	BATCH = 500
	while len(account_id_list) > 0:
		if len(account_id_list) >= BATCH:
			end = BATCH
		else:
			end = len(account_id_list)
		try: 
			res = await dbc.delete_many({ '_id': { '$in': account_id_list[:end] } } )
			deleted_count += res.deleted_count
		except Exception as err:
			bu.error('Unexpected Exception: ' + str(type(err)) + ' : '+ str(err))
		del(account_id_list[:end])
	
	bu.verbose_std(str(deleted_count) + ' account_ids deleted from the database')

	return None


async def update_account_ids(db: motor.motor_asyncio.AsyncIOMotorDatabase, account_ids: set):
		"""Update / add account_ids to the database"""

		dbc = db[DB_C_ACCOUNTS]
				
		player_list = list()
		for account_id in account_ids:
			if account_id < 3e9:
				player_list.append(mk_player_JSON(account_id))
		
		count_old = await dbc.count_documents({})
		BATCH = 500
		while len(player_list) > 0:
			if len(player_list) >= BATCH:
				end = BATCH
			else:
				end = len(player_list)
			try:
				bu.print_progress()
				bu.debug('Inserting account_ids to DB')
				await dbc.insert_many(player_list[:end], ordered=False)
			except pymongo.errors.BulkWriteError as err:
				pass
			except Exception as err:
				bu.error('Unexpected Exception: ' + str(type(err)) + ' : ' + str(err))
			del(player_list[:end])

		count = await dbc.count_documents({})
		bu.print_new_line()
		bu.verbose_std(str(count - count_old) + ' new account_ids added to the database')
		bu.verbose_std(str(count) + ' account_ids in the database')

def mk_player_JSON(account_id: int): 
	player = {}
	player['_id'] = account_id
	player['updated'] = NOW()
	player['last_battle_time'] = None
	return player

async def get_players_DB(db, args):
	dbc = db[DB_C_REPLAYS]
	players = set()

	cursor = dbc.find({}, { 'data.summary.allies' : 1, 'data.summary.enemies' : 1, '_id' : 0 } )
	async for replay in cursor:
		bu.print_progress()
		
		try:
			players.update(replay['data']['summary']['allies'])
			players.update(replay['data']['summary']['enemies'])			
		except Exception as err:
			bu.error('Unexpected error: ' + str(type(err)) + ' : ' + str(err))
	return players

async def get_players_replays(files : list, workers: int):
	replayQ  = asyncio.Queue()	
	reader_tasks = []
	# Make replay Queue
	scanner_task = asyncio.create_task(mk_replayQ(replayQ, files))

	# Start tasks to process the Queue
	for i in range(workers):
		reader_tasks.append(asyncio.create_task(replay_reader(replayQ, i)))
		bu.debug('Task ' + str(i) + ' started')

	bu.debug('Waiting for the replay scanner to finish')
	await asyncio.wait([scanner_task])
	bu.debug('Scanner finished. Waiting for replay readers to finish the queue')
	await replayQ.join()
	bu.debug('Replays read. Cancelling Readers and analyzing results')
	for task in reader_tasks:
		task.cancel()	
	players = set()
	for res in await asyncio.gather(*reader_tasks):
		players.update(res)
	
	return players

async def replay_reader(queue: asyncio.Queue, readerID: int):
	"""Async Worker to process the replay queue"""

	players = set()
	try:
		while True:
			item = await queue.get()
			filename = item[0]
			replayID = item[1]

			try:
				if os.path.exists(filename) and os.path.isfile(filename):
					async with aiofiles.open(filename) as fp:
						replay_json = json.loads(await fp.read())
						tmp = await parse_account_ids(replay_json)
						if tmp != None:
							players.update(tmp)
						else:
							bu.error('Replay[' + str(replayID) + ']: ' + filename + ' is invalid. Skipping.' )
			except Exception as err:
				bu.error(str(err))
			bu.debug('Marking task ' + str(replayID) + ' done')
			queue.task_done()
	except asyncio.CancelledError:		
		return players
	return None

async def parse_account_ids(json_resp: dict): 
	players = set()
	try:
		if json_resp.get('status', 'error') != 'ok':
			raise Exception('Replay file is invalid')
		players.update(json_resp['data']['summary']['allies'])
		players.update(json_resp['data']['summary']['enemies'])	
	except Exception as err:
		bu.error(str(err))
		return None
	return players

async def mk_replayQ(queue : asyncio.Queue, files : list):
	"""Create queue of replays to post"""
	p_replayfile = re.compile('.*\\.wotbreplay\\.json$')

	if files[0] == '-':
		bu.debug('reading replay file list from STDIN')
		stdin, _ = await aioconsole.get_standard_streams()
		while True:
			line = (await stdin.readline()).decode('utf-8').rstrip()
			if not line: 
				break
			else:
				if (p_replayfile.match(line) != None):
					await queue.put(await mk_replayQ_item(line))
	else:
		for fn in files:
			if fn.endswith('"'):
				fn = fn[:-1]  
			if os.path.isfile(fn) and (p_replayfile.match(fn) != None):
				await queue.put(await mk_replayQ_item(fn))
			elif os.path.isdir(fn):
				with os.scandir(fn) as dirEntry:
					for entry in dirEntry:
						if entry.is_file() and (p_replayfile.match(entry.name) != None): 
							bu.debug(entry.name)
							await queue.put(await mk_replayQ_item(entry.path))
			bu.debug('File added to queue: ' + fn)
	bu.debug('Finished')
	return None

async def mk_replayQ_item(filename : str) -> list:
	"""Make an item to replay queue"""
	global REPLAY_N
	REPLAY_N +=1
	bu.print_progress()
	return [filename, REPLAY_N]

async def get_players_WI(db : motor.motor_asyncio.AsyncIOMotorDatabase, args: argparse.Namespace):
	"""Get active players from wotinspector.com replays"""
	global wi

	workers 	= args.workers
	max_pages 	= args.max_pages
	start_page 	= args.start_page
	force 		= args.force
	players 	= set()
	replayQ 	= asyncio.Queue()
	wi 			= WoTinspector(rate_limit=25)
	
	# Start tasks to process the Queue
	tasks = []

	for i in range(workers):
		tasks.append(asyncio.create_task(WI_replay_fetcher(db, replayQ, i, force)))
		bu.debug('Replay Fetcher ' + str(i) + ' started')

	bu.set_progress_bar('Spidering replays', max_pages, step = 1, id = "spider")		

	for page in range(start_page,(start_page + max_pages)):
		if WI_STOP_SPIDER: 
			bu.debug('Stopping spidering WoTispector.com')
			break
		# url = wi.get_url_replay_listing(page)
		bu.print_progress(id = "spider")
		try:
			resp = await wi.get_replay_listing(page)
			if resp.status != 200:
				bu.error('Could not retrieve wotinspector.com')
				continue	
			bu.debug('HTTP request OK')
			html = await resp.text()
			links = wi.get_replay_links(html)
			if len(links) == 0: 
				break
			for link in links:
				await replayQ.put(link)
			# await asyncio.sleep(SLEEP)
		except aiohttp.ClientError as err:
			bu.error("Could not retrieve replays.WoTinspector.com page " + str(page))
			bu.error(str(err))
	
	n_replays = replayQ.qsize()
	bu.set_progress_bar('Fetching replays', n_replays, step = 5, id = 'replays')


	bu.debug('Replay links read. Replay Fetchers to finish')
	await replayQ.join()
	bu.finish_progress_bar()
	bu.debug('Replays fetched. Cancelling fetcher workers')
	for task in tasks:
		task.cancel()	
	for res in await asyncio.gather(*tasks):
		players.update(res)		
	await wi.close()

	return players

async def WI_old_replay_found(queue : asyncio.Queue):
	global WI_old_replay_N
	WI_old_replay_N +=1
	if WI_old_replay_N == WI_old_replay_limit:
		bu.verbose_std(str(WI_old_replay_limit) + ' old replays spidered. Stopping')
		await empty_queue(queue, 'Replay Queue') 
	return True

async def WI_replay_fetcher(db : motor.motor_asyncio.AsyncIOMotorDatabase, queue : asyncio.Queue, workerID : int, force : bool):
	players = set()
	dbc = db[DB_C_REPLAYS]
	msg_str = 'Replay Fetcher[' + str(workerID) + ']: '
	
	try:
		while True:
			replay_link = await queue.get()
			bu.print_progress(id = 'replays')
			replay_id = wi.get_replay_id(replay_link)
			res = await dbc.find_one({'_id': replay_id})
			if res != None:
				bu.debug(msg_str + 'Replay already in the DB: ' + str(replay_id) )
				queue.task_done()
				if force:
					continue
				else: 
					await WI_old_replay_found(queue)					
					continue
			url = wi.URL_WI + replay_link
			json_resp = await wi.get_replay_JSON(replay_id)
			
			if json_resp == None:
				bu.error(msg_str + 'Could not fetch valid Replay JSON: ' + url)
				queue.task_done()
				continue
			json_resp['_id'] = replay_id
			try:
				await dbc.insert_one(json_resp)
				bu.debug(msg_str + 'Replay added to database')
			except Exception as err:
				bu.error(msg_str + 'Unexpected Exception: ' + str(type(err)) + ' : ' + str(err)) 
			players.update(await parse_account_ids(json_resp))
			bu.debug(msg_str + 'Processed replay: ' + url )
			queue.task_done()
			await asyncio.sleep(SLEEP)
	
	except asyncio.CancelledError:		
		return players
	except Exception as err:
		bu.error('Replay Fetcher[' + str(workerID) + ']: Unexpected Exception: ' + str(type(err)) + ' : ' + str(err)) 

async def empty_queue(queue : asyncio.Queue, Qname = ''):
	"""Empty the task queue"""
	global WI_STOP_SPIDER
	try:
		bu.debug('Emptying queue: ' + Qname)
		WI_STOP_SPIDER = True
		while True:
			queue.get_nowait()
			queue.task_done()
	except asyncio.QueueEmpty:
		bu.debug('Queue empty: ' + Qname)
	return None

async def get_players_BS(force = False):
	"""Get active player list from BlitzStars.com"""
	active_players = set()
	if force or not (os.path.exists(FILE_ACTIVE_PLAYERS) and os.path.isfile(FILE_ACTIVE_PLAYERS)) or (NOW() - os.path.getmtime(FILE_ACTIVE_PLAYERS) > CACHE_VALID):
		url = bs.get_url_active_players()
		bu.verbose('Retrieving active players file from BlitzStars.com')
		player_list = await bu.get_url_JSON(bs.session, url)
		await bu.save_JSON(FILE_ACTIVE_PLAYERS, player_list)
		active_players.update(player_list)
	else:
		async with aiofiles.open(FILE_ACTIVE_PLAYERS, 'rt') as f:
			active_players.update(json.loads(await f.read()))
	return active_players

async def mk_playerQ(queue : asyncio.Queue, account_id_list : list):
	"""Create queue of replays to post"""
	for account_id in account_id_list:
		bu.debug('Adding account_id: ' + str(account_id) + ' to the queue')
		await queue.put(account_id)

	return None

def NOW() -> int:
	return int(time.time())

def mk_id(account_id: int, tank_id: int, last_battle_time: int):
	return hex(account_id)[2:].zfill(10) + hex(tank_id)[2:].zfill(6) + hex(last_battle_time)[2:].zfill(8)

    ### main()
if __name__ == "__main__":
   #asyncio.run(main(sys.argv[1:]), debug=True)
   asyncio.run(main(sys.argv[1:]))
