from argparse import ArgumentParser, Namespace
from configparser import ConfigParser
from typing import Optional, Iterable, cast
import logging
from asyncio import create_task, gather, Queue, CancelledError, Task, sleep, Condition
from aiofiles import open
from os.path import isfile
from math import ceil
from asyncstdlib import enumerate
from alive_progress import alive_bar		# type: ignore

from backend import Backend, OptAccountsInactive, ACCOUNTS_Q_MAX, MIN_UPDATE_INTERVAL
from models import BSAccount, BSBlitzRelease, StatsTypes
from accounts import split_accountQ_by_region, create_accountQ
from releases import release_mapper

from pyutils import BucketMapper, IterableQueue, QueueDone, EventCounter, \
					get_url, get_url_JSON_model, epoch_now, alive_bar_monitor
from blitzutils.models import Region, WGApiWoTBlitzPlayerAchievements, WGplayerAchievementsMaxSeries
from blitzutils.wg import WGApi 

logger = logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug

# Constants

WORKERS_WGAPI 				: int = 40
PLAYER_ACHIEVEMENTS_Q_MAX 	: int = 5000

########################################################
# 
# add_args_ functions  
#
########################################################

def add_args_player_achievements(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')		
		parsers = parser.add_subparsers(dest='player_achievements_cmd', 	
										title='player-achievements commands',
										description='valid commands',
										metavar='fetch | prune | import | export')
		parsers.required = True
		
		fetch_parser = parsers.add_parser('fetch', aliases=['get'], help="player-achievements fetch help")
		if not add_args_player_achievements_fetch(fetch_parser, config=config):
			raise Exception("Failed to define argument parser for: player-achievements fetch")
		
		prune_parser = parsers.add_parser('prune', help="player-achievements prune help")
		if not add_args_player_achievements_prune(prune_parser, config=config):
			raise Exception("Failed to define argument parser for: player-achievements prune")

		import_parser = parsers.add_parser('import', help="player-achievements import help")
		if not add_args_player_achievements_import(import_parser, config=config):
			raise Exception("Failed to define argument parser for: player-achievements import")

		export_parser = parsers.add_parser('export', help="player-achievements export help")
		if not add_args_player_achievements_export(export_parser, config=config):
			raise Exception("Failed to define argument parser for: player-achievements export")
		debug('Finished')	
		return True
	except Exception as err:
		error(f'{err}')
	return False


def add_args_player_achievements_fetch(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
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


def add_args_player_achievements_prune(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	debug('starting')
	return True


def add_args_player_achievements_import(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	debug('starting')
	return True


def add_args_player_achievements_export(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	debug('starting')
	return True


###########################################
# 
# cmd_ functions  
#
###########################################

async def cmd_player_achievements(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		if args.player_achievements_cmd == 'fetch':
			return await cmd_player_achievements_fetch(db, args)

		# elif args.player_achievements_cmd == 'export':
		# 	return await cmd_player_achievements_export(db, args)

		# elif args.player_achievements_cmd == 'import':
		# 	# return await cmd_player_achievements_import(db, args)
		# 	pass
		else:
			raise ValueError(f'Unsupported command: player-achievements { args.player_achievements_cmd}')
			
	except Exception as err:
		error(f'{err}')
	return False

async def cmd_player_achievements_fetch(db: Backend, args : Namespace) -> bool:
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

		for task in tasks:
			task.cancel()
		
		for ec in await gather(*tasks, return_exceptions=True):
			if isinstance(ec, EventCounter):
				stats.merge_child(ec)
			elif isinstance(ec, Exception):
				error(f'{type(ec).__name__}(): {ec}')
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
