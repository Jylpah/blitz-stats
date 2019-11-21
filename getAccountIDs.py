#!/usr/bin/python3.7

# Script fetch Blitz player stats and tank stats

import sys, argparse, json, os, inspect, pprint, aiohttp, asyncio, aiofiles, aioconsole
import motor.motor_asyncio, ssl, lxml, re, logging, time, xmltodict, collections, pymongo
import configparser
import blitzutils as bu
from bs4 import BeautifulSoup
from blitzutils import BlitzStars, WG, WoTinspector

logging.getLogger("asyncio").setLevel(logging.DEBUG)

N_WORKERS = 5
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
WIoldReplayN = 0
WIoldReplayLimit = 30

## main() -------------------------------------------------------------

async def main(argv):
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
	bu.setSilent(args.silent)
	bu.setVerbose(args.verbose)
	bu.setDebug(args.debug)
	
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
		DB_USER		= configDB.get('db_user', 'mongouser')
		DB_PASSWD 	= configDB.get('db_password', "PASSWORD")
		DB_CERT		= configDB.get('db_ssl_cert_file', None)
		DB_CA		= configDB.get('db_ssl_ca_file', None)
		
		bu.debug('DB_SERVER: ' + DB_SERVER)
		bu.debug('DB_PORT: ' + str(DB_PORT))
		bu.debug('DB_SSL: ' + "True" if DB_SSL else "False")
		bu.debug('DB_AUTH: ' + DB_AUTH)
		bu.debug('DB_NAME: ' + DB_NAME)

		#### Connect to MongoDB
		client = motor.motor_asyncio.AsyncIOMotorClient(DB_SERVER,DB_PORT, authSource=DB_AUTH, username=DB_USER, password=DB_PASSWD, ssl=DB_SSL, ssl_cert_reqs=DB_CERT_REQ, ssl_certfile=DB_CERT, tlsCAFile=DB_CA)

		db = client[DB_NAME]
		await db[DB_C_ACCOUNTS].create_index([ ('last_battle_time', pymongo.DESCENDING) ], background=True)	

		if not args.remove:
			if args.blitzstars:
				players.update(await getPlayersBS(args.force))
		
			if args.wotinspector:
				players.update(await getPlayersWI(db, args))

			if args.db:
				players.update(await getPlayersDB(db, args))
			
			if len(args.files) > 0:
				bu.debug(str(args.files))
				players.update(await getPlayersReplays(args.files, args.workers))

			await updateAccountIDs(db, players)
		
		else:
			await removeAccountIDs(db, args.files)

	except asyncio.CancelledError as err:
		bu.error('Queue gets cancelled while still working.')
	except Exception as err:
		bu.error('Unexpected Exception: ' + str(type(err)) + ' : '+ str(err))
	finally:
		await bs.close()

	return None


async def removeAccountIDs(db: motor.motor_asyncio.AsyncIOMotorDatabase, files: list):
	"""Remove account_ids from the DB"""
	dbc = db[DB_C_ACCOUNTS]

	account_ids = set()
	for file in files:
		account_ids.update(await bu.readPlainList(file))
	
	accountID_list = list(account_ids)
	bu.verbose_std('Deleting ' + str(len(accountID_list)) + ' account_ids')

	deleted_count = 0
	BATCH = 500
	while len(accountID_list) > 0:
		if len(accountID_list) >= BATCH:
			end = BATCH
		else:
			end = len(accountID_list)
		try: 
			res = await dbc.delete_many({ '_id': { '$in': accountID_list[:end] } } )
			deleted_count += res.deleted_count
		except Exception as err:
			bu.error('Unexpected Exception: ' + str(type(err)) + ' : '+ str(err))
		del(accountID_list[:end])
	
	bu.verbose_std(str(deleted_count) + ' account_ids deleted from the database')

	return None


async def updateAccountIDs(db: motor.motor_asyncio.AsyncIOMotorDatabase, account_ids: set):
		"""Update / add account_ids to the database"""

		dbc = db[DB_C_ACCOUNTS]
				
		player_list = list()
		for account_id in account_ids:
			player_list.append(mkPlayerJSON(account_id))
		
		count_old = await dbc.count_documents({})
		BATCH = 500
		while len(player_list) > 0:
			if len(player_list) >= BATCH:
				end = BATCH
			else:
				end = len(player_list)
			try:
				bu.printWaiter()
				bu.debug('Inserting account_ids to DB')
				await dbc.insert_many(player_list[:end], ordered=False)
			except pymongo.errors.BulkWriteError as err:
				pass
			except Exception as err:
				bu.error('Unexpected Exception: ' + str(type(err)) + ' : ' + str(err))
			del(player_list[:end])

		count = await dbc.count_documents({})
		bu.printNewline()
		bu.verbose_std(str(count - count_old) + ' new account_ids added to the database')
		bu.verbose_std(str(count) + ' account_ids in the database')

def mkPlayerJSON(account_id: int): 
	player = {}
	player['_id'] = account_id
	player['updated'] = NOW()
	player['last_battle_time'] = None
	return player

async def getPlayersDB(db, args):
	dbc = db[DB_C_REPLAYS]
	players = set()

	i = 0
	cursor = dbc.find({}, { 'data.summary.allies' : 1, 'data.summary.enemies' : 1, '_id' : 0 } )
	async for replay in cursor:
		i += 1
		if (i % 100) == 0 : 
			bu.printWaiter()
			bu.debug('Replays read from DB: ' + str(i))
		try:
			players.update(replay['data']['summary']['allies'])
			players.update(replay['data']['summary']['enemies'])			
		except Exception as err:
			bu.error('Unexpected error: ' + str(type(err)) + ' : ' + str(err))
	return players

async def getPlayersReplays(files : list, workers: int):
	replayQ  = asyncio.Queue()	
	reader_tasks = []
	# Make replay Queue
	scanner_task = asyncio.create_task(mkReplayQ(replayQ, files))

	# Start tasks to process the Queue
	for i in range(workers):
		reader_tasks.append(asyncio.create_task(replayReader(replayQ, i)))
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

async def replayReader(queue: asyncio.Queue, readerID: int):
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
						tmp = await parseAccountIDs(replay_json)
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

async def parseAccountIDs(json_resp: dict): 
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

async def mkReplayQ(queue : asyncio.Queue, files : list):
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
					await queue.put(await mkReaderQitem(line))
	else:
		for fn in files:
			if fn.endswith('"'):
				fn = fn[:-1]  
			if os.path.isfile(fn) and (p_replayfile.match(fn) != None):
				await queue.put(await mkReaderQitem(fn))
			elif os.path.isdir(fn):
				with os.scandir(fn) as dirEntry:
					for entry in dirEntry:
						if entry.is_file() and (p_replayfile.match(entry.name) != None): 
							bu.debug(entry.name)
							await queue.put(await mkReaderQitem(entry.path))
			bu.debug('File added to queue: ' + fn)
	bu.debug('Finished')
	return None

async def mkReaderQitem(filename : str) -> list:
	"""Make an item to replay queue"""
	global REPLAY_N
	REPLAY_N +=1
	bu.printWaiter()
	return [filename, REPLAY_N]

async def getPlayersWI(db : motor.motor_asyncio.AsyncIOMotorDatabase, args: argparse.Namespace):
	"""Get active players from wotinspector.com replays"""
	global wi

	workers 	= args.workers
	max_pages 	= args.max_pages
	start_page 	= args.start_page
	force 		= args.force
	players 	= set()
	replayQ 	= asyncio.Queue()
	wi 			= WoTinspector()
	session 	= wi.session
	
	# Start tasks to process the Queue
	tasks = []
	for i in range(workers):
		tasks.append(asyncio.create_task(WIreplayFetcher(db, replayQ, i, force)))
		bu.debug('Replay Fetcher ' + str(i) + ' started')
			
	for page in range(start_page,(start_page + max_pages)):
		if WI_STOP_SPIDER: 
			bu.debug('Stopping spidering WoTispector.com')
			break
		url = wi.getUrlReplayListing(page)
		bu.printWaiter()
		try:
			async with session.get(url) as resp:
				if resp.status != 200:
					bu.error('Could not retrieve wotinspector.com')
					continue	
				bu.debug('HTTP request OK')
				html = await resp.text()
				links = wi.getReplayLinks(html)
				if len(links) == 0: 
					break
				for link in links:
					await replayQ.put(link)
				await asyncio.sleep(2*SLEEP)
		except aiohttp.ClientError as err:
			bu.error("Could not retrieve URL: " + url)
			bu.error(str(err))

	bu.debug('Replay links read. Replay Fetchers to finish')
	await replayQ.join()
	bu.debug('Replays fetched. Cancelling fetcher workers')
	for task in tasks:
		task.cancel()	
	for res in await asyncio.gather(*tasks):
		players.update(res)		
	await wi.close()

	return players

async def WIoldReplayFound(queue : asyncio.Queue):
	global WIoldReplayN
	WIoldReplayN +=1
	if WIoldReplayN == WIoldReplayLimit:
		bu.verbose_std(str(WIoldReplayLimit) + ' old replays spidered. Stopping')
		await emptyQueue(queue, 'Replay Queue') 
	return True

async def WIreplayFetcher(db : motor.motor_asyncio.AsyncIOMotorDatabase, queue : asyncio.Queue, workerID : int, force : bool):
	players = set()
	dbc = db[DB_C_REPLAYS]
	msg_str = 'Replay Fetcher[' + str(workerID) + ']: '
	try:
		while True:
			replay_link = await queue.get()
			bu.printWaiter()
			replay_id = wi.getReplayID(replay_link)
			res = await dbc.find_one({'_id': replay_id})
			if res != None:
				bu.debug(msg_str + 'Replay already in the DB: ' + str(replay_id) )
				queue.task_done()
				if force:
					continue
				else: 
					await WIoldReplayFound(queue)					
					continue
			url = wi.URL_WI + replay_link
			json_resp = await wi.getReplayJSON(replay_id)
			
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
			players.update(await parseAccountIDs(json_resp))
			bu.debug(msg_str + 'Processed replay: ' + url )
			queue.task_done()
			await asyncio.sleep(SLEEP)
	
	except asyncio.CancelledError:		
		return players
	except Exception as err:
		bu.error('Replay Fetcher[' + str(workerID) + ']: Unexpected Exception: ' + str(type(err)) + ' : ' + str(err)) 

async def emptyQueue(queue : asyncio.Queue, Qname = ''):
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

async def getPlayersBS(force = False):
	"""Get active player list from BlitzStars.com"""
	active_players = set()
	if force or not (os.path.exists(FILE_ACTIVE_PLAYERS) and os.path.isfile(FILE_ACTIVE_PLAYERS)) or (NOW() - os.path.getmtime(FILE_ACTIVE_PLAYERS) > CACHE_VALID):
		url = bs.getUrlActivePlayers()
		bu.verbose('Retrieving active players file from BlitzStars.com')
		player_list = await bu.getUrlJSON(bs.session, url)
		await bu.saveJSON(FILE_ACTIVE_PLAYERS, player_list)
		active_players.update(player_list)
	else:
		async with aiofiles.open(FILE_ACTIVE_PLAYERS, 'rt') as f:
			active_players.update(json.loads(await f.read()))
	return active_players

async def mkPlayerQ(queue : asyncio.Queue, accountID_list : list):
	"""Create queue of replays to post"""
	for accountID in accountID_list:
		bu.debug('Adding account_id: ' + str(accountID) + ' to the queue')
		await queue.put(accountID)

	return None

def NOW() -> int:
	return int(time.time())

def mkID(accountID: int, tankID: int, last_battle_time: int):
	return hex(accountID)[2:].zfill(10) + hex(tankID)[2:].zfill(6) + hex(last_battle_time)[2:].zfill(8)

    ### main()
if __name__ == "__main__":
   #asyncio.run(main(sys.argv[1:]), debug=True)
   asyncio.run(main(sys.argv[1:]))
