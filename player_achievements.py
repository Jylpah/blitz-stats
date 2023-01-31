from argparse import ArgumentParser, Namespace, SUPPRESS
from configparser import ConfigParser
from typing import Optional, Any, Iterable, cast
import logging
from asyncio import create_task, run, gather, Queue, CancelledError, Task, sleep, Condition
from aiofiles import open
from os.path import isfile
from os import getpid
from math import ceil
from asyncstdlib import enumerate
from alive_progress import alive_bar		# type: ignore

from multiprocessing import Manager, cpu_count
from multiprocessing.pool import Pool, AsyncResult 
import queue

from backend import Backend, OptAccountsInactive, BSTableType, \
	ACCOUNTS_Q_MAX, MIN_UPDATE_INTERVAL, get_sub_type
from models import BSAccount, BSBlitzRelease, StatsTypes
from accounts import split_accountQ_by_region, create_accountQ
from releases import release_mapper

from pyutils import BucketMapper, IterableQueue, QueueDone, \
	EventCounter, JSONExportable, AsyncQueue, \
	get_url, get_url_JSON_model, epoch_now, alive_bar_monitor, \
	is_alphanum
from blitzutils.models import Region, WGPlayerAchievementsMain, \
	WGPlayerAchievementsMaxSeries
from blitzutils.wg import WGApi 

logger = logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug

# Constants

WORKERS_WGAPI 				: int = 40
WORKERS_IMPORTERS			: int = 5
PLAYER_ACHIEVEMENTS_Q_MAX 	: int = 5000

# Globals

db 			: Backend
readQ 		: AsyncQueue[list[Any] | None]
in_model	: type[JSONExportable]
mp_options	: dict[str, Any] = dict()

########################################################
# 
# add_args_ functions  
#
########################################################

def add_args(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')		
		parsers = parser.add_subparsers(dest='player_achievements_cmd', 	
										title='player-achievements commands',
										description='valid commands',
										metavar='fetch | prune | import | export')
		parsers.required = True
		
		fetch_parser = parsers.add_parser('fetch', aliases=['get'], help="player-achievements fetch help")
		if not add_args_fetch(fetch_parser, config=config):
			raise Exception("Failed to define argument parser for: player-achievements fetch")
		
		prune_parser = parsers.add_parser('prune', help="player-achievements prune help")
		if not add_args_prune(prune_parser, config=config):
			raise Exception("Failed to define argument parser for: player-achievements prune")

		import_parser = parsers.add_parser('import', help="player-achievements import help")
		if not add_args_import(import_parser, config=config):
			raise Exception("Failed to define argument parser for: player-achievements import")

		export_parser = parsers.add_parser('export', help="player-achievements export help")
		if not add_args_export(export_parser, config=config):
			raise Exception("Failed to define argument parser for: player-achievements export")
		debug('Finished')	
		return True
	except Exception as err:
		error(f'{err}')
	return False


def add_args_fetch(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		WG_RATE_LIMIT 	: float = 10
		WORKERS_WGAPI 	: int 	= 10
		WG_APP_ID		: str 	= WGApi.DEFAULT_WG_APP_ID
		
		if config is not None and 'WG' in config.sections():
			configWG 		= config['WG']
			WG_RATE_LIMIT	= configWG.getfloat('rate_limit', WG_RATE_LIMIT)
			WORKERS_WGAPI	= configWG.getint('api_workers', WORKERS_WGAPI)			
			WG_APP_ID		= configWG.get('wg_app_id', WGApi.DEFAULT_WG_APP_ID)

		parser.add_argument('--workers', type=int, default=WORKERS_WGAPI, help='Set number of asynchronous workers')
		parser.add_argument('--wg-app-id', type=str, default=WG_APP_ID, help='Set WG APP ID')
		parser.add_argument('--rate-limit', type=float, default=WG_RATE_LIMIT, metavar='RATE_LIMIT',
							help='Rate limit for WG API')
		parser.add_argument('--region', type=str, nargs='*', choices=[ r.value for r in Region.API_regions() ], 
							default=[ r.value for r in Region.API_regions() ], help='Filter by region (default: eu + com + asia + ru)')
		parser.add_argument('--force', action='store_true', default=False, 
							help='Overwrite existing file(s) when exporting')
		parser.add_argument('--sample', type=float, default=0, metavar='SAMPLE',
							help='Fetch stats for SAMPLE of accounts. If 0 < SAMPLE < 1, SAMPLE defines a %% of users')
		parser.add_argument('--cache_valid', type=int, default=None, metavar='DAYS',
							help='Fetch only accounts with stats older than DAYS')		
		parser.add_argument('--distributed', '--dist',type=str, dest='distributed', metavar='I:N', 
							default=None, help='Distributed fetching for accounts: id %% N == I')
		parser.add_argument('--accounts', type=str, default=[], nargs='*', metavar='ACCOUNT_ID [ACCOUNT_ID1 ...]',
							help='Fetch player achievements for the listed ACCOUNT_ID(s)')
		parser.add_argument('--file',type=str, metavar='FILENAME', default=None, 
							help='Read account_ids from FILENAME one account_id per line')

		return True
	except Exception as err:
		error(f'{err}')
	return False


def add_args_prune(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	debug('starting')
	parser.add_argument('release', type=str, metavar='RELEASE',  
						help='prune player achievements for a RELEASE')
	parser.add_argument('--region', type=str, nargs='*', choices=[ r.value for r in Region.API_regions() ], 
							default=[ r.value for r in Region.API_regions() ], 
							help='filter by region (default: eu + com + asia + ru)')
	parser.add_argument('--commit', action='store_true', default=False, 
							help='execute pruning stats instead of listing duplicates')
	parser.add_argument('--sample', type=int, default=0, metavar='SAMPLE',
						help='sample size')
	return True


def add_args_import(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	debug('starting')
	try:
		debug('starting')
		
		import_parsers = parser.add_subparsers(dest='import_backend', 	
												title='player-achievements import backend',
												description='valid backends', 
												metavar=', '.join(Backend.list_available()))
		import_parsers.required = True

		for backend in Backend.get_registered():
			import_parser =  import_parsers.add_parser(backend.driver, help=f'player-achievements import {backend.driver} help')
			if not backend.add_args_import(import_parser, config=config):
				raise Exception(f'Failed to define argument parser for: player-achievements import {backend.driver}')
		parser.add_argument('--workers', type=int, default=0, help='Set number of asynchronous workers')
		parser.add_argument('--import-model', metavar='IMPORT-TYPE', type=str, required=True,
							choices=['WGPlayerAchievementsMaxSeries', 'WGPlayerAchievementsMain'], 
							help='Data format to import. Default is blitz-stats native format.')
		parser.add_argument('--sample', type=float, default=0, 
							help='Sample size. 0 < SAMPLE < 1 : %% of stats, 1<=SAMPLE : Absolute number')
		parser.add_argument('--force', action='store_true', default=False, 
							help='Overwrite existing stats when importing')
		parser.add_argument('--no-release-map', action='store_true', default=False, 
							help='Do not map releases when importing')
		parser.add_argument('--last', action='store_true', default=False, help=SUPPRESS)

		return True
	except Exception as err:
		error(f'{err}')
	return False

def add_args_export(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	debug('starting')
	return True


###########################################
# 
# cmd_ functions  
#
###########################################

async def cmd(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		if args.player_achievements_cmd == 'fetch':
			return await cmd_fetch(db, args)

		# elif args.player_achievements_cmd == 'export':
		# 	return await cmd_export(db, args)

		elif args.player_achievements_cmd == 'import':
			return await cmd_importMP(db, args)
		
		elif args.player_achievements_cmd == 'prune':
			return await cmd_prune(db, args)

		else:
			raise ValueError(f'Unsupported command: player-achievements { args.player_achievements_cmd}')
			
	except Exception as err:
		error(f'{err}')
	return False

async def cmd_fetch(db: Backend, args : Namespace) -> bool:
	"""Fetch player achievements"""
	assert 'wg_app_id' in args and type(args.wg_app_id) is str, "'wg_app_id' must be set and string"
	assert 'rate_limit' in args and (type(args.rate_limit) is float or \
			type(args.rate_limit) is int), "'rate_limit' must set and a number"	
	assert 'region' in args and type(args.region) is list, "'region' must be set and a list"
	
	debug('starting')
	
	wg 	: WGApi = WGApi(WG_app_id=args.wg_app_id, rate_limit=args.rate_limit)

	try:
		stats 	 	: EventCounter								= EventCounter('player-achievements fetch')
		regions	 	: set[Region]								= { Region(r) for r in args.region }
		regionQs 	: dict[str, IterableQueue[BSAccount]]		= dict()
		accountQ	: IterableQueue[BSAccount] 					= IterableQueue(maxsize=ACCOUNTS_Q_MAX)
		retryQ  	: IterableQueue[BSAccount] 					= IterableQueue() 
		statsQ	 	: Queue[list[WGPlayerAchievementsMaxSeries]]= Queue(maxsize=PLAYER_ACHIEVEMENTS_Q_MAX)

		tasks : list[Task] = list()
		tasks.append(create_task(fetch_player_achievements_backend_worker(db, statsQ)))

		# Process accountQ
		accounts : int 
		if len(args.accounts) > 0:
			accounts = len(args.accounts)
		else:	
			accounts = await db.accounts_count(StatsTypes.player_achievements, 
												regions=regions, 
												inactive=OptAccountsInactive.no,  
												sample=args.sample, 
												cache_valid=args.cache_valid)		
		for r in regions:
			regionQs[r.name] = IterableQueue(maxsize=ACCOUNTS_Q_MAX)			
			for _ in range(ceil(min([args.workers, accounts])/len(regions))):
				tasks.append(create_task(fetch_player_achievements_api_region_worker(wg_api=wg, region=r, 
																					accountQ=regionQs[r.name], 
																					statsQ=statsQ, 
																					retryQ=retryQ)))
		task_bar : Task = create_task(alive_bar_monitor(list(regionQs.values()), total=accounts, 
														title="Fetching player achievement"))
		tasks.append(create_task(split_accountQ_by_region(accountQ, regionQs)))
		stats.merge_child(await create_accountQ(db, args, accountQ, StatsTypes.player_achievements))
		
		# waiting for region queues to finish
		for rname, Q in regionQs.items():
			debug(f'waiting for region queue to finish: {rname} size={Q.qsize()}')
			await Q.join()
		task_bar.cancel()

		print(f'retryQ: size={retryQ.qsize()},  is_finished={retryQ.is_finished}')
		if not retryQ.empty():
			regionQs = dict()
			accounts = retryQ.qsize()
			for r in regions:
				# do not fill the old queues or it will never complete				
				regionQs[r.name] = IterableQueue(maxsize=ACCOUNTS_Q_MAX) 
				for _ in range(ceil(min([args.workers, accounts])/len(regions))):
					tasks.append(create_task(fetch_player_achievements_api_region_worker(wg_api=wg, region=r, 
																					accountQ=regionQs[r.name],																					
																					statsQ=statsQ)))
			task_bar = create_task(alive_bar_monitor(list(regionQs.values()), total=accounts, 
														title="Re-trying player achievement"))
			await split_accountQ_by_region(retryQ, regionQs)
			for rname, Q in regionQs.items():
				debug(f'waiting for region re-try queue to finish: {rname} size={Q.qsize()}')
				await Q.join()
			task_bar.cancel()
		else:
			debug('no accounts in retryQ')
			
		await statsQ.join()
		await stats.gather_stats(tasks=tasks)

		message(stats.print(do_print=False, clean=True))
		return True
	except Exception as err:
		error(f'{err}')
	finally:
	
		wg_stats : dict[str, str] | None = wg.print_server_stats()
		if wg_stats is not None and logger.level <= logging.WARNING:
			message('WG API stats:')
			for server in wg_stats:
				message(f'{server.capitalize():7s}: {wg_stats[server]}')
		await wg.close()
	return False


async def fetch_player_achievements_api_region_worker(wg_api: WGApi, region: Region,  
														accountQ: IterableQueue[BSAccount], 
														statsQ:   Queue[list[WGPlayerAchievementsMaxSeries]],
														retryQ:   IterableQueue[BSAccount] | None = None) -> EventCounter:
	"""Fetch stats from a single region"""
	debug('starting')

	stats 	: EventCounter
	if retryQ is None:
		stats 		= EventCounter(f're-try {region.name}')
	else:
		stats 		= EventCounter(f'fetch {region.name}')
		await retryQ.add_producer()

	Q_finished 	= False
	
	try:
		while not Q_finished:
			accounts 	: dict[int, BSAccount]	= dict()
			account_ids : list[int] 			= list()
			
			try:				
				for _ in range(100):
					try:
						account : BSAccount = await accountQ.get()
						debug(f'read: {account}')
						accounts[account.id] = account
					except (QueueDone, CancelledError):
						Q_finished = True
						break
					except Exception as err:
						error(f'Failed to read account from queue: {err}')

				account_ids = [ a for a in accounts.keys() ]
				debug(f'account_ids={account_ids}')
				if len(account_ids) > 0:
					if (res:= await wg_api.get_player_achievements(account_ids, region)) is None:
						res = list()											
					else:
						await statsQ.put(res)
						if retryQ is not None:
							# retry accounts without stats
							for ms in res:
								accounts.pop(ms.account_id)						
					stats.log('stats found', len(res))
					if retryQ is None:
						stats.log('no stats', len(account_ids) - len(res))
					else:
						stats.log('re-tries', len(account_ids) - len(res))
						for a in accounts.values():
							await retryQ.put(a)
			except Exception as err:
				error(f'{err}')
			finally:
				for _ in range(len(account_ids)):
					accountQ.task_done()

	except Exception as err:
		error(f'{err}')
	if retryQ is not None:
		await retryQ.finish()
	return stats


async def fetch_player_achievements_backend_worker(db: Backend, 
													statsQ: Queue[list[WGPlayerAchievementsMaxSeries]]) -> EventCounter:
	"""Async worker to add player achievements to backend. Assumes batch is for the same account"""
	debug('starting')
	stats 		: EventCounter = EventCounter(f'{db.driver}')
	added 		: int
	not_added 	: int
	
	try:
		releases : BucketMapper[BSBlitzRelease] = await release_mapper(db)
		while True:
			added 			= 0
			not_added 		= 0
			player_achievements : list[WGPlayerAchievementsMaxSeries] = await statsQ.get()
			try:
				if len(player_achievements) > 0:
					debug(f'Read {len(player_achievements)} from queue')
					rel : BSBlitzRelease | None = releases.get(epoch_now())
					if rel is not None:
						for pa in player_achievements:
							pa.release = rel.release
					added, not_added = await db.player_achievements_insert(player_achievements)
			except Exception as err:
				error(f'{err}')
			finally:
				stats.log('added', added)
				stats.log('not found', not_added)
				debug(f'{added} player achievements added, {not_added} old player achievements found')
				statsQ.task_done()	
	except CancelledError as err:
		debug(f'Cancelled')	
	except Exception as err:
		error(f'{err}')
	return stats

########################################################
# 
# cmd_import()
#
########################################################


async def cmd_import(db: Backend, args : Namespace) -> bool:
	"""Import player achievements from other backend"""	
	try:
		assert is_alphanum(args.import_model), f'invalid --import-model: {args.import_model}'
		debug('starting')
		player_achievementsQ	: Queue[list[WGPlayerAchievementsMaxSeries]]= Queue(PLAYER_ACHIEVEMENTS_Q_MAX)
		rel_mapQ				: Queue[WGPlayerAchievementsMaxSeries]		= Queue()
		stats 			: EventCounter 	= EventCounter('player-achievements import')
		WORKERS 	 	: int 							= args.workers
		import_db   	: Backend | None 				= None
		import_backend 	: str 							= args.import_backend
		import_model 	: type[JSONExportable] | None 	= None
		map_releases	: bool 							= not args.no_release_map

		# if (import_model := get_sub_type(args.import_model, JSONExportable)) is None:
		# 	assert False, "--import-model has to be subclass of JSONExportable" 

		release_map : BucketMapper[BSBlitzRelease] = await release_mapper(db)
		workers : list[Task] = list()
		debug('args parsed')
		
		if (import_db := Backend.create_import_backend(driver=import_backend, 
														args=args, 
														import_type=BSTableType.PlayerAchievements, 
														copy_from=db,
														config_file=args.import_config)) is None:
			raise ValueError(f'Could not init {import_backend} to import player achievements from')
		# debug(f'import_db: {await import_db._client.server_info()}')
		message(f'Import from: {import_db.backend}.{import_db.table_player_achievements}')
				
		message('Counting player achievements to import ...')
		N : int = await import_db.player_achievements_count(sample=args.sample)
		message(f'Importing {N} player achievements')

		for _ in range(WORKERS):
			workers.append(create_task(db.player_achievements_insert_worker(player_achievementsQ=player_achievementsQ, 
																	force=args.force)))
		rel_map_worker : Task = create_task(player_achievements_map_releases_worker(release_map, inputQ=rel_mapQ, 
																outputQ=player_achievementsQ, 
																map_releases=map_releases))		

		with alive_bar(N, title="Importing player achievements ", 
						enrich_print=False, refresh_secs=1) as bar:
			async for pa in import_db.player_achievement_export(sample=args.sample):
				await rel_mapQ.put(pa)
				bar()
		
		await rel_mapQ.join()
		rel_map_worker.cancel()
		await stats.gather_stats([rel_map_worker])
		await player_achievementsQ.join()
		await stats.gather_stats(workers)
		message(stats.print(do_print=False, clean=True))
		return True
	except Exception as err:
		error(f'{err}')	
	return False


async def cmd_importMP(db: Backend, args : Namespace) -> bool:
	"""Import player achievements from other backend"""	
	try:
		debug('starting')		
		stats 			: EventCounter 					= EventCounter('player-achievements import')
		import_db   	: Backend | None 				= None
		import_backend 	: str 							= args.import_backend
		import_model 	: type[JSONExportable] | None 	= None
		WORKERS 	 	: int 							= args.workers

		if WORKERS == 0:
			WORKERS = max( [cpu_count() - 1, 1 ])
		
		if (import_model := get_sub_type(args.import_model, JSONExportable)) is None:
			raise ValueError("--import-model has to be subclass of JSONExportable")

		if (import_db := Backend.create_import_backend(driver=import_backend, 
														args=args, 
														import_type=BSTableType.PlayerAchievements, 
														copy_from=db,
														config_file=args.import_config)) is None:
			raise ValueError(f'Could not init {import_backend} to import player achievements from')

		with Manager() as manager:
			
			readQ	: queue.Queue[list[Any] | None] = manager.Queue(PLAYER_ACHIEVEMENTS_Q_MAX)
			options : dict[str, Any] 				= dict()
			options['force'] 						= args.force
			# options['map_releases'] 				= not args.no_release_map
			
			with Pool(processes=WORKERS, initializer=import_mp_init, 
					  initargs=[ db.config, readQ, import_model, options ]) as pool:

				debug(f'starting {WORKERS} workers')
				results : AsyncResult = pool.map_async(import_mp_worker_start, range(WORKERS))
				pool.close()

				message('Counting player achievements to import ...')
				N : int = await import_db.player_achievements_count(sample=args.sample)
				
				with alive_bar(N, title="Importing player achievements ", 
								enrich_print=False, refresh_secs=1) as bar:					
					async for objs in import_db.objs_export(table_type=BSTableType.PlayerAchievements, 
															sample=args.sample):
						read : int = len(objs)
						# debug(f'read {read} player achievements objects')
						readQ.put(objs)
						stats.log(f'{db.driver}: stats read', read)
						bar(read)	
				
				debug(f'Finished exporting {import_model} from {import_db.table_uri(BSTableType.PlayerAchievements)}')
				for _ in range(WORKERS):
					readQ.put(None) # add sentinel
				
				for res in results.get():
					stats.merge_child(res)
				pool.join()
		
		message(stats.print(do_print=False, clean=True))
		return True
	except Exception as err:
		error(f'{err}')	
	return False


def import_mp_init( backend_config	: dict[str, Any],					
					inputQ 			: queue.Queue, 
					import_model	: type[JSONExportable],
					options 		: dict[str, Any]):
	"""Initialize static/global backend into a forked process"""
	global db, readQ, in_model, mp_options
	debug(f'starting (PID={getpid()})')

	if (tmp_db := Backend.create(**backend_config)) is None:
		raise ValueError('could not create backend')	
	db 					= tmp_db
	readQ 				= AsyncQueue.from_queue(inputQ)
	in_model 			= import_model	
	mp_options			= options
	debug('finished')


def import_mp_worker_start(id: int = 0) -> EventCounter:
	"""Forkable player achievements import worker"""
	debug(f'starting import worker #{id}')
	return run(import_mp_worker(id), debug=False)


async def  import_mp_worker(id: int = 0) -> EventCounter:
	"""Forkable player achievements import worker"""
	debug(f'#{id}: starting')
	stats : EventCounter = EventCounter('importer')
	workers 	: list[Task] 				= list()
	try: 
		global db, readQ, in_model, mp_options
		THREADS 			: int 									= 4		
		import_model		: type[JSONExportable] 					= in_model
		rel_mapper 			: BucketMapper[BSBlitzRelease]  		=  await release_mapper(db)
		player_achievementsQ: Queue[list[WGPlayerAchievementsMaxSeries]] = Queue(100)
		force 		: bool 											= mp_options['force']
		player_achievements : list[WGPlayerAchievementsMaxSeries]
		# rel_map		: bool								= mp_options['map_releases']

		for _ in range(THREADS):
			workers.append(create_task(db.player_achievements_insert_worker(player_achievementsQ=player_achievementsQ, 
																			force=force)))		
		errors : int = 0
		mapped : int = 0
		while (objs := await readQ.get()) is not None:
			debug(f'#{id}: read {len(objs)} objects')
			try:
				read : int = len(objs)
				debug(f'read {read} documents')
				stats.log('stats read', read)	
				player_achievements = WGPlayerAchievementsMaxSeries.transform_objs(objs=objs, in_type=import_model)
				errors = len(objs) - len(player_achievements)		
				stats.log('player achievements read', len(player_achievements))				
				stats.log('format errors', errors)

				player_achievements, mapped, errors = map_releases(player_achievements, rel_mapper)
				stats.log('release mapped', mapped)
				stats.log('not release mapped', read - mapped)
				stats.log('release map errors', errors)

				await player_achievementsQ.put(player_achievements)
			except Exception as err:
				error(f'{err}')
			finally:
				readQ.task_done()		
		debug(f'#{id}: finished reading objects')
		readQ.task_done()	
		await player_achievementsQ.join() 		# add sentinel for other workers
		await stats.gather_stats(workers)	
	except CancelledError:
		pass
	except Exception as err:
		error(f'{err}')	
	return stats


def map_releases(player_achievements: list[WGPlayerAchievementsMaxSeries], 
				release_map: BucketMapper[BSBlitzRelease]) -> tuple[list[WGPlayerAchievementsMaxSeries], int, int]:
	debug('starting')
	mapped: int = 0
	errors: int = 0
	res : list[WGPlayerAchievementsMaxSeries] = list()
	for player_achievement in player_achievements:		
		try:			
			if (release := release_map.get(player_achievement.added)) is not None:
				player_achievement.release = release.release
				mapped += 1									
		except Exception as err:
			error(f'{err}')
			errors += 1
		res.append(player_achievement)
	return res, mapped, errors


async def player_achievements_map_releases_worker(release_map: BucketMapper[BSBlitzRelease], 
													inputQ: Queue[WGPlayerAchievementsMaxSeries], 
													outputQ: Queue[list[WGPlayerAchievementsMaxSeries]], 
													map_releases: bool = True) -> EventCounter:
	"""Map player achievements to releases and pack those to list[WGPlayerAchievementsMaxSeries] queue.
		map_all is None means no release mapping is done"""
	
	debug(f'starting: map_releases={map_releases}')
	stats 			: EventCounter = EventCounter('Release mapper')
	IMPORT_BATCH	: int = 500
	release 		: BSBlitzRelease | None
	pa_list 		: list[WGPlayerAchievementsMaxSeries] = list()

	try:		
		# await outputQ.add_producer()
		while True:
			pa = await inputQ.get()
			# debug(f'read: {pa}')
			try:
				if map_releases:
					if (release := release_map.get(pa.added)) is not None:
						pa.release = release.release
						# debug(f'mapped: release={release.release}')
						stats.log('mapped')
					else:
						error(f'Could not map release: added={pa.added}')
						stats.log('errors')
				pa_list.append(pa)
				if len(pa_list) == IMPORT_BATCH:
					await outputQ.put(pa_list)
					# debug(f'put {len(pa_list)} items to outputQ')
					stats.log('read', len(pa_list))
					pa_list = list()				
			except Exception as err:
				error(f'Processing input queue: {err}')
			finally: 
				inputQ.task_done()
	except CancelledError:
		debug(f'Cancelled: pa_list has {len(pa_list)} items')
		if len(pa_list) > 0:
			await outputQ.put(pa_list)
			# debug(f'{len(pa_list)} items added to outputQ')
			stats.log('read', len(pa_list))			
	except Exception as err:
		error(f'{err}')	
	# await outputQ.finish()
	return stats

########################################################
# 
# cmd_prune()
#
########################################################


async def cmd_prune(db: Backend, args : Namespace) -> bool:
	"""prune  player achievements"""	
	debug('starting')
	try:
		stats 		: EventCounter 		= EventCounter(' player-achievements prune')
		regions		: set[Region] 		= { Region(r) for r in args.region }
		sample 		: int 				= args.sample
		release 	: BSBlitzRelease  	= BSBlitzRelease(release=args.release)
		commit 		: bool 				= args.commit

		progress_str : str 				= 'Finding duplicates ' 
		if commit:
			message(f'Pruning  player achievements from {db.table_uri(BSTableType.PlayerAchievements)}')
			message('Press CTRL+C to cancel in 3 secs...')
			await sleep(3)
			progress_str = 'Pruning duplicates '
		
		with alive_bar(len(regions), title=progress_str, refresh_secs=1) as bar:
			for region in regions:
				async for dup in db.player_achievements_duplicates(release, regions={region}, sample=sample):
					stats.log('duplicates found')
					if commit:
						# verbose(f'deleting duplicate: {dup}')
						if await db.player_achievement_delete(account=BSAccount(id=dup.account_id), 
															  added=dup.added):
							verbose(f'deleted duplicate: {dup}')
							stats.log('duplicates deleted')
						else:
							error(f'failed to delete duplicate: {dup}')
							stats.log('deletion errors')
					else:
						verbose(f'duplicate:  {dup}')
						async for newer in db.player_achievements_get(release=release, regions={region}, 
																accounts=[BSAccount(id=dup.account_id)], 																
																since=dup.added + 1):
							verbose(f'newer stat: {newer}')
				bar()

		stats.print()
		return True
	except Exception as err:
		error(f'{err}')
	return False