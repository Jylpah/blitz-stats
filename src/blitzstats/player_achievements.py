from argparse import ArgumentParser, Namespace, SUPPRESS
from configparser import ConfigParser
from typing import Optional, Any, Iterable, cast
import logging
from asyncio import create_task, run, gather, Queue, CancelledError, Task, sleep, Condition
from aiofiles import open
from os.path import isfile
from os import getpid
from math import ceil
from sortedcollections import NearestDict 		# type: ignore

from asyncstdlib import enumerate
from alive_progress import alive_bar		# type: ignore

from multiprocessing import Manager, cpu_count
from multiprocessing.pool import Pool, AsyncResult 
import queue

from pyutils import IterableQueue, QueueDone, EventCounter, \
					JSONExportable, AsyncQueue
from pyutils.utils import epoch_now, alive_bar_monitor, is_alphanum
from blitzutils import Region, WGPlayerAchievementsMain, \
	WGPlayerAchievementsMaxSeries, WGApi 

from .backend import Backend, OptAccountsInactive, BSTableType, \
	ACCOUNTS_Q_MAX, MIN_UPDATE_INTERVAL, get_sub_type
from .models import BSAccount, BSBlitzRelease, StatsTypes
from .accounts import split_accountQ, split_accountQ_batch, create_accountQ, \
					create_accountQ_batch, accounts_parse_args
from .releases import release_mapper

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
		WG_WORKERS 		: int 	= 10
		WG_APP_ID		: str 	= WGApi.DEFAULT_WG_APP_ID
		
		# Lesta / RU
		LESTA_RATE_LIMIT: float = 10
		LESTA_WORKERS 	: int 	= 10
		LESTA_APP_ID 	: str 	= WGApi.DEFAULT_LESTA_APP_ID
		NULL_RESPONSES 	: int 	= 20
		
		if config is not None and 'WG' in config.sections():
			configWG 		= config['WG']
			WG_RATE_LIMIT	= configWG.getfloat('rate_limit', WG_RATE_LIMIT)
			WG_WORKERS		= configWG.getint('api_workers', WG_WORKERS)			
			WG_APP_ID		= configWG.get('app_id', WG_APP_ID)

		if config is not None and 'LESTA' in config.sections():
			configRU 		= config['LESTA']
			LESTA_RATE_LIMIT	= configRU.getfloat('rate_limit', LESTA_RATE_LIMIT)
			LESTA_WORKERS		= configRU.getint('api_workers', LESTA_WORKERS)			
			LESTA_APP_ID		= configRU.get('app_id', LESTA_APP_ID)

		parser.add_argument('--wg-workers', type=int, default=WG_WORKERS, help='Set number of asynchronous workers')
		parser.add_argument('--wg-app-id', type=str, default=WG_APP_ID, help='Set WG APP ID')
		parser.add_argument('--wg-rate-limit', type=float, default=WG_RATE_LIMIT, 
		      				metavar='RATE_LIMIT', help='Rate limit for WG API')
		parser.add_argument('--ru-app-id', type=str, default=LESTA_APP_ID, metavar='APP_ID',
							help='Set Lesta (RU) APP ID')
		parser.add_argument('--ru-rate-limit', type=float, default=LESTA_RATE_LIMIT, metavar='RATE_LIMIT',
							help='Rate limit for Lesta (RU) API')
		parser.add_argument('--regions', '--region', type=str, nargs='*', 
		      				choices=[ r.value for r in Region.API_regions() ], 
							default=[ r.value for r in Region.API_regions() ], 
							help='Filter by region (default: eu + com + asia + ru)')
		parser.add_argument('--inactive', type=str, choices=[ o.value for o in OptAccountsInactive ], 
								default=OptAccountsInactive.both.value, 
								help='Include inactive accounts')
		parser.add_argument('--active-since', type=str, default=None, metavar='RELEASE/DAYS',
							help='Fetch stats for accounts that have been active since RELEASE/DAYS')
		parser.add_argument('--inactive-since', type=str,  default=None, metavar='RELEASE',
							help='Fetch stats for accounts that have been inactive since RELEASE/DAYS')	
		parser.add_argument('--cache_valid', type=float, default=1	, metavar='DAYS',
							help='Fetch only accounts with stats older than DAYS')		
		parser.add_argument('--distributed', '--dist',type=str, dest='distributed', metavar='I:N', 
							default=None, help='Distributed stats fetching for accounts: id %% N == I')
		parser.add_argument('--accounts', type=str, default=[], nargs='*', 
							metavar='ACCOUNT_ID [ACCOUNT_ID1 ...]',
							help='Fetch stats for the listed ACCOUNT_ID(s)')
		parser.add_argument('--force', action='store_true', default=False, 
							help='Overwrite existing file(s) when exporting')
		parser.add_argument('--sample', type=float, default=0, metavar='SAMPLE',
							help='Fetch stats for SAMPLE of accounts. If 0 < SAMPLE < 1, SAMPLE defines a %% of users')
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
	parser.add_argument('--regions', '--region', type=str, nargs='*', choices=[ r.value for r in Region.API_regions() ], 
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
	debug('starting')
	wg 	: WGApi = WGApi(app_id=args.wg_app_id, 
						ru_app_id= args.ru_app_id,
						rate_limit=args.wg_rate_limit, 
						ru_rate_limit = args.ru_rate_limit,)

	try:
		stats 	 : EventCounter								= EventCounter('player-achievements fetch', totals='total')
		regions	 : set[Region]								= { Region(r) for r in args.regions }
		regionQs : dict[str, IterableQueue[list[BSAccount]]]= dict()
		retryQ	 : IterableQueue[BSAccount] 				= IterableQueue() 
		statsQ	 : Queue[list[WGPlayerAchievementsMaxSeries]]= Queue(maxsize=PLAYER_ACHIEVEMENTS_Q_MAX)

		tasks : list[Task] = list()
		tasks.append(create_task(fetch_backend_worker(db, statsQ)))

		# Process accountQ
		accounts : int 
		if len(args.accounts) > 0:
			accounts = len(args.accounts)
		else:
			accounts_args : dict[str, Any] | None
			if (accounts_args := await accounts_parse_args(db, args)) is None:
				raise ValueError(f'could not parse account args: {args}')	
			accounts = await db.accounts_count(StatsTypes.player_achievements, 
												**accounts_args)		
		
		if args.sample > 1:
			args.sample = int(args.sample / len(regions))

		for region in regions:
			regionQs[region.name] = IterableQueue(maxsize=ACCOUNTS_Q_MAX)			
			tasks.append(create_task(create_accountQ_batch(db, args, region, 
															accountQ=regionQs[region.name],
															stats_type=StatsTypes.player_achievements)))			
			for _ in range(ceil(min([args.wg_workers, accounts]))):
				tasks.append(create_task(fetch_api_region_worker(wg_api=wg, region=region, 
																					accountQ=regionQs[region.name], 
																					statsQ=statsQ, 
																					retryQ=retryQ)))
		task_bar : Task = create_task(alive_bar_monitor(list(regionQs.values()), 
														total=accounts, 
														batch=100,
														title="Fetching player achievement"))
		# tasks.append(create_task(split_accountQ(accountQ, regionQs)))
		# stats.merge_child(await create_accountQ(db, args, accountQ, StatsTypes.player_achievements))
		
		# waiting for region queues to finish
		for rname, Q in regionQs.items():
			debug(f'waiting for region queue to finish: {rname} size={Q.qsize()}')
			await Q.join()
		task_bar.cancel()

		print(f'retryQ: size={retryQ.qsize()},  is_filled={retryQ.is_filled}')
		if not retryQ.empty():
			regionQs = dict()
			accounts = retryQ.qsize()
			for region in regions:
				# do not fill the old queues or it will never complete				
				regionQs[region.name] = IterableQueue(maxsize=ACCOUNTS_Q_MAX) 
				for _ in range(ceil(min([args.wg_workers, accounts]))):
					tasks.append(create_task(fetch_api_region_worker(wg_api=wg, region=region, 
																						accountQ=regionQs[region.name],																					
																						statsQ=statsQ)))
			task_bar = create_task(alive_bar_monitor(list(regionQs.values()), total=accounts, 
														title="Re-trying player achievement"))
			await split_accountQ_batch(retryQ, regionQs)
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
		wg.print()
		await wg.close()
	return False


async def fetch_api_region_worker(wg_api: WGApi, region: Region,  
														accountQ: IterableQueue[list[BSAccount]], 
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

	try:
		while True:
			accounts 	: dict[int, BSAccount]	= dict()
			account_ids : list[int] 			= list()
					
			for account in await accountQ.get():
				accounts[account.id] = account
			try:				
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
				accountQ.task_done()

	except QueueDone:
		debug('accountQ finished')
	except Exception as err:
		error(f'{err}')
	if retryQ is not None:
		await retryQ.finish()
	return stats


async def fetch_backend_worker(db: Backend, 
								statsQ: Queue[list[WGPlayerAchievementsMaxSeries]]) -> EventCounter:
	"""Async worker to add player achievements to backend. Assumes batch is for the same account"""
	debug('starting')
	stats 		: EventCounter = EventCounter(f'{db.driver}')
	added 		: int
	not_added 	: int
	
	try:
		releases : NearestDict[int, BSBlitzRelease] = await release_mapper(db)
		while True:
			added 			= 0
			not_added 		= 0
			player_achievements : list[WGPlayerAchievementsMaxSeries] = await statsQ.get()
			try:
				if len(player_achievements) > 0:
					debug(f'Read {len(player_achievements)} from queue')
					rel : BSBlitzRelease | None = releases[epoch_now()]
					if rel is not None:
						for pac in player_achievements:
							pac.release = rel.release
					added, not_added = await db.player_achievements_insert(player_achievements)
					for pac in player_achievements:
						if (account := await db.account_get(account_id=pac.account_id)) is not None:
							account.stats_updated(StatsTypes.player_achievements)
			except Exception as err:
				error(f'{err}')
			finally:
				stats.log('stats added', added)
				stats.log('old stats', not_added)
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
		map_releases	: bool 							= not args.no_release_map

		releases : NearestDict[int, BSBlitzRelease] = await release_mapper(db)
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
		rel_map_worker : Task = create_task(player_releases_worker(releases, inputQ=rel_mapQ, 
																outputQ=player_achievementsQ, 
																map_releases=map_releases))		

		with alive_bar(N, title="Importing player achievements ", 
						enrich_print=False, refresh_secs=1) as bar:
			async for pac in import_db.player_achievement_export(sample=args.sample):
				await rel_mapQ.put(pac)
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
		releases 			: NearestDict[int, BSBlitzRelease]		=  await release_mapper(db)
		player_achievementsQ: Queue[list[WGPlayerAchievementsMaxSeries]] = Queue(100)
		force 				: bool 											= mp_options['force']
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

				player_achievements, mapped, errors = map_releases(player_achievements, releases)
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
				releases: NearestDict[int, BSBlitzRelease]
				) -> tuple[list[WGPlayerAchievementsMaxSeries], int, int]:
	debug('starting')
	mapped: int = 0
	errors: int = 0
	res : list[WGPlayerAchievementsMaxSeries] = list()
	for player_achievement in player_achievements:		
		try:			
			if (release := releases[player_achievement.added]) is not None:
				player_achievement.release = release.release
				mapped += 1									
		except Exception as err:
			error(f'{err}')
			errors += 1
		res.append(player_achievement)
	return res, mapped, errors


async def player_releases_worker(releases: NearestDict[int, BSBlitzRelease], 
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
			pac = await inputQ.get()
			# debug(f'read: {pac}')
			try:
				if map_releases:
					if (release := releases[(pac.added)]) is not None:
						pac.release = release.release
						# debug(f'mapped: release={release.release}')
						stats.log('mapped')
					else:
						error(f'Could not map release: added={pac.added}')
						stats.log('errors')
				pa_list.append(pac)
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
		regions		: set[Region] 		= { Region(r) for r in args.regions }
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
						if await db.player_achievement_delete(account=BSAccount(id=dup.account_id, 
							      													region=region), 
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