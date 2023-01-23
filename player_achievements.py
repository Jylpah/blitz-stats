from argparse import ArgumentParser, Namespace, SUPPRESS
from configparser import ConfigParser
from typing import Optional, Any, Iterable, cast
import logging
from asyncio import create_task, gather, Queue, CancelledError, Task, sleep, Condition
from aiofiles import open
from os.path import isfile
from math import ceil
from asyncstdlib import enumerate
from alive_progress import alive_bar		# type: ignore

from backend import Backend, OptAccountsInactive, BSTableType, \
	ACCOUNTS_Q_MAX, MIN_UPDATE_INTERVAL, get_sub_type
from models import BSAccount, BSBlitzRelease, StatsTypes
from accounts import split_accountQ_by_region, create_accountQ
from releases import release_mapper

from pyutils import BucketMapper, IterableQueue, QueueDone, \
	EventCounter, JSONExportable, \
	get_url, get_url_JSON_model, epoch_now, alive_bar_monitor, \
	is_alphanum
from blitzutils.models import Region, WGplayerAchievementsMain, \
	WGplayerAchievementsMaxSeries
from blitzutils.wg import WGApi 

logger = logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug

# Constants

WORKERS_WGAPI 				: int = 40
WORKERS_IMPORTERS			: int = 1
PLAYER_ACHIEVEMENTS_Q_MAX 	: int = 5000

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

		parser.add_argument('--threads', type=int, default=WORKERS_WGAPI, help='Set number of asynchronous threads')
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
		parser.add_argument('--accounts', type=int, default=[], nargs='*', metavar='ACCOUNT_ID [ACCOUNT_ID1 ...]',
							help='Fetch player achievements for the listed ACCOUNT_ID(s)')
		parser.add_argument('--file',type=str, metavar='FILENAME', default=None, 
							help='Read account_ids from FILENAME one account_id per line')

		return True
	except Exception as err:
		error(f'{err}')
	return False


def add_args_prune(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	debug('starting')
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
		parser.add_argument('--threads', type=int, default=WORKERS_IMPORTERS, help='Set number of asynchronous threads')
		parser.add_argument('--import-model', metavar='IMPORT-TYPE', type=str, 
							default='WGplayerAchievementsMaxSeries', 
							choices=['WGplayerAchievementsMaxSeries', 'WGplayerAchievementsMain'], 
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
			return await cmd_import(db, args)

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
		statsQ	 	: Queue[list[WGplayerAchievementsMaxSeries]]= Queue(maxsize=PLAYER_ACHIEVEMENTS_Q_MAX)

		tasks : list[Task] = list()
		tasks.append(create_task(fetch_player_achievements_backend_worker(db, statsQ)))

		accounts : int = await db.accounts_count(StatsTypes.player_achievements, 
												regions=regions, 
												inactive=OptAccountsInactive.no,  
												sample=args.sample, 
												cache_valid=args.cache_valid)		
		for r in regions:
			regionQs[r.name] = IterableQueue(maxsize=ACCOUNTS_Q_MAX)			
			for _ in range(ceil(min([args.threads, accounts])/len(regions))):
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
				for _ in range(ceil(min([args.threads, accounts])/len(regions))):
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
														statsQ:   Queue[list[WGplayerAchievementsMaxSeries]],
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
													statsQ: Queue[list[WGplayerAchievementsMaxSeries]]) -> EventCounter:
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
			player_achievements : list[WGplayerAchievementsMaxSeries] = await statsQ.get()
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
		player_achievementsQ	: Queue[list[WGplayerAchievementsMaxSeries]]= Queue(PLAYER_ACHIEVEMENTS_Q_MAX)
		rel_mapQ				: Queue[WGplayerAchievementsMaxSeries]		= Queue()
		stats 			: EventCounter 	= EventCounter('player-achievements import')
		WORKERS 	 	: int 							= args.threads
		import_db   	: Backend | None 				= None
		import_backend 	: str 							= args.import_backend
		import_model 	: type[JSONExportable] | None 	= None
		map_releases	: bool 							= not args.no_release_map

		if (import_model := get_sub_type(args.import_model, JSONExportable)) is None:
			assert False, "--import-model has to be subclass of JSONExportable" 

		release_map : BucketMapper[BSBlitzRelease] = await release_mapper(db)
		workers : list[Task] = list()
		debug('args parsed')
		
		if (import_db := Backend.create_import_backend(driver=import_backend, 
														args=args, 
														import_type=BSTableType.PlayerAchievements, 
														copy_from=db,
														config_file=args.import_config)) is None:
			raise ValueError(f'Could not init {import_backend} to import releases from')
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

		with alive_bar(N, title="Importing player achievements ", enrich_print=False, refresh_secs=0.5) as bar:
			async for pa in import_db.player_achievements_export(model=import_model, 
																sample=args.sample):
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


async def player_achievements_map_releases_worker(release_map: BucketMapper[BSBlitzRelease], 
													inputQ: Queue[WGplayerAchievementsMaxSeries], 
													outputQ: Queue[list[WGplayerAchievementsMaxSeries]], 
													map_releases: bool = True) -> EventCounter:
	"""Map player achievements to releases and pack those to list[WGplayerAchievementsMaxSeries] queue.
		map_all is None means no release mapping is done"""
	
	debug(f'starting: map_releases={map_releases}')
	stats 			: EventCounter = EventCounter('Release mapper')
	IMPORT_BATCH	: int = 500
	release 		: BSBlitzRelease | None
	pa_list 		: list[WGplayerAchievementsMaxSeries] = list()

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