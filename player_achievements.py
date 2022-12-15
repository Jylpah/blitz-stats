from argparse import ArgumentParser, Namespace
from configparser import ConfigParser
from typing import Optional, Iterable, cast
import logging
from asyncio import create_task, gather, Queue, CancelledError, Task, sleep, Condition
from aiofiles import open
from os.path import isfile
from asyncstdlib import enumerate
from alive_progress import alive_bar		# type: ignore

from backend import Backend, OptAccountsInactive, ACCOUNTS_Q_MAX, MIN_UPDATE_INTERVAL
from models import BSAccount, BSBlitzRelease, StatsTypes
from accounts import split_accountQ_by_region, create_accountQ

from pyutils import BucketMapper, CounterQueue, EventCounter
from pyutils.utils import get_url, get_url_JSON_model, epoch_now
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
												metavar='update | prune | import | export')
		parsers.required = True
		
		update_parser = parsers.add_parser('update', aliases=['get'], help="player-achievements update help")
		if not add_args_player_achievements_update(update_parser, config=config):
			raise Exception("Failed to define argument parser for: player-achievements update")
		
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


def add_args_player_achievements_update(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
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
							help='Update stats for SAMPLE of accounts. If 0 < SAMPLE < 1, SAMPLE defines a %% of users')
		parser.add_argument('--cache_valid', type=int, default=None, metavar='DAYS',
							help='Update only accounts with stats older than DAYS')		
		parser.add_argument('--distributed', '--dist',type=str, dest='distributed', metavar='I:N', 
							default=None, help='Distributed update for accounts: id %% N == I')
		parser.add_argument('--accounts', type=int, default=None, nargs='*', metavar='ACCOUNT_ID [ACCOUNT_ID1 ...]',
							help='Update player achievements for the listed ACCOUNT_ID(s)')
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
		if args.player_achievements_cmd == 'update':
			return await cmd_player_achievements_update(db, args)

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

async def cmd_player_achievements_update(db: Backend, args : Namespace) -> bool:
	"""Update player achievements"""
	assert 'wg_app_id' in args and type(args.wg_app_id) is str, "'wg_app_id' must be set and string"
	assert 'rate_limit' in args and (type(args.rate_limit) is float or \
			type(args.rate_limit) is int), "'rate_limit' must set and a number"	
	assert 'region' in args and type(args.region) is list, "'region' must be set and a list"
	
	debug('starting')
	
	wg 	: WGApi = WGApi(WG_app_id=args.wg_app_id, rate_limit=args.rate_limit)

	try:
		stats 	 	: EventCounter								= EventCounter('player-achievements update')
		regions	 	: set[Region]								= { Region(r) for r in args.region }
		regionQs 	: dict[str, Queue[BSAccount]]				= dict()
		accountQ	: Queue[BSAccount] 							= Queue(maxsize=ACCOUNTS_Q_MAX)
		retryQ  	: Queue[BSAccount] 							= Queue() 
		statsQ	 	: Queue[list[WGplayerAchievementsMaxSeries]] = Queue(maxsize=PLAYER_ACHIEVEMENTS_Q_MAX)

		tasks : list[Task] = list()
		tasks.append(create_task(update_player_achievements_worker(db, statsQ)))
		
		accounts_left : Condition = Condition()
		await accounts_left.acquire()

		for r in regions:
			regionQs[r.name] = Queue(maxsize=100)			
			for _ in range(int(args.threads/len(regions))):
				tasks.append(create_task(update_player_achievements_api_region_worker(wg_api=wg, region=r, 
																	accountQ=regionQs[r.name], 
																	statsQ=statsQ,
																	accounts_left=accounts_left, 
																	retryQ=retryQ)))
		
		tasks.append(create_task(split_accountQ_by_region(accountQ, regionQs)))
		await create_accountQ(db, args, accountQ, StatsTypes.player_achievements,
								bar_title='Fetching player achievements')
		accounts_left.release()		# All accounts have been added to region Queues

		# waiting for region queues to finish
		for rname, Q in regionQs.items():
			debug(f'waiting for region queue to finish: {rname}')
			await Q.join()

		if retryQ is not None and not retryQ.empty():
			retries_left : Condition = Condition()
			await retries_left.acquire()
			for r in regions:
				regionQs[r.name] = Queue(maxsize=100) # do not fill the old queues or it will never complete
				for _ in range(int(args.threads/len(regions))):
					tasks.append(create_task(update_player_achievements_api_region_worker(wg_api=wg, region=r, 
																					accountQ=regionQs[r.name],
																					accounts_left=retries_left, 
																					statsQ=statsQ)))
			ec_retries : EventCounter = await split_accountQ_by_region(retryQ, regionQs, progress=True, bar_title='Re-trying failed accounts')
			stats.merge_child(ec_retries)
			await retryQ.join()		

		await statsQ.join()

		for task in tasks:
			task.cancel()
		
		for ec in await gather(*tasks, return_exceptions=True):
			if issubclass(EventCounter, ec):
				stats.merge_child(ec)
		message(stats.print(do_print=False))
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


async def update_player_achievements_api_region_worker(wg_api: WGApi, region: Region,  
														accountQ: Queue[BSAccount], 
														statsQ:   Queue[list[WGplayerAchievementsMaxSeries]],
														accounts_left : Condition,
														retryQ:   Queue[BSAccount] | None = None) -> EventCounter:
	"""Fetch stats from a single region"""
	debug('starting')
	stats = EventCounter(f'WG API {region.name}')
	try:
		while True:
			accounts : list[BSAccount] = list()
			account_ids : list[int] = list()
			try:				
				while not accountQ.empty() and len(accounts) < 100:
					accounts.append(await accountQ.get())

				account_ids = [ a.id for a in accounts ]
				if (res:= await wg_api.get_player_achievements(account_ids, region)) is None:
					if retryQ is not None :
						stats.log('re-tries', len(account_ids))
						for a in accounts:
							await retryQ.put(a)
					else:
						stats.log('no stats', len(account_ids))
					continue
				else:
					await statsQ.put(res)
			finally:
				for _ in range(len(accounts)):
					accountQ.task_done()
			if accountQ.empty():
				await sleep(1)
				if not accounts_left.locked() and accountQ.empty():
					break

	except Exception as err:
		error(f'{err}')
	return stats


async def update_player_achievements_worker(db: Backend, statsQ: Queue[list[WGplayerAchievementsMaxSeries]]) -> EventCounter:
	"""Async worker to add player achievements to backend. Assumes batch is for the same account"""
	debug('starting')
	stats 		: EventCounter = EventCounter(f'Backend ({db.driver})')
	added 		: int
	not_added 	: int
	account 	: BSAccount | None
	account_id	: int
	
	try:
		releases : BucketMapper[BSBlitzRelease] = BucketMapper[BSBlitzRelease](attr='cut_off')
		async for r in db.releases_get():
			releases.insert(r)
		while True:
			player_achievements : list[WGplayerAchievementsMaxSeries] = await statsQ.get()			
			added 			= 0
			not_added 		= 0
			account			= None
			try:
				if len(player_achievements) > 0:
					debug(f'Read {len(player_achievements)} from queue')
					rel : BSBlitzRelease | None = releases.get(epoch_now())
					if rel is not None:
						for pa in player_achievements:
							pa.release = rel.release
					added, not_added = await db.player_achievements_insert(player_achievements)
					account_id = player_achievements[0].account_id
					if (account := await db.account_get(account_id=account_id)) is None:
						account = BSAccount(id=account_id)
					account.last_battle_time = last_battle_time
					account.stats_updated(StatsTypes.player_achievements)
					if added > 0:
						stats.log('accounts /w new stats')
						if account.inactive:
							stats.log('accounts marked active')
						account.inactive = False
					else:
						stats.log('accounts w/o new stats')
						if account.is_inactive(StatsTypes.player_achievements): 
							if not account.inactive:
								stats.log('accounts marked inactive')
							account.inactive = True
						
					await db.account_replace(account=account, upsert=True)
			except Exception as err:
				error(f'{err}')
			finally:
				stats.log('player achievements added', added)
				stats.log('old player achievements found', not_added)
				debug(f'{added} player achievements added, {not_added} old player achievements found')				
				statsQ.task_done()
	except CancelledError as err:
		debug(f'Cancelled')	
	except Exception as err:
		error(f'{err}')
	return stats
