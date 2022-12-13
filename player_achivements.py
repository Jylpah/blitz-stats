from argparse import ArgumentParser, Namespace
from configparser import ConfigParser
from typing import Optional, Iterable, cast
import logging
from asyncio import create_task, gather, Queue, CancelledError, Task, sleep
from aiofiles import open
from os.path import isfile
from asyncstdlib import enumerate
from alive_progress import alive_bar		# type: ignore

from backend import Backend, OptAccountsInactive, ACCOUNTS_Q_MAX, MIN_UPDATE_INTERVAL
from models import BSAccount, BSBlitzRelease, StatsTypes
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
		stats 	 : EventCounter				= EventCounter('player-achievements update')
		regions	 : list[Region]				=  [ Region(r) for r in args.region ]
		accountQs : dict[str, Queue[BSAccount]]	= dict()
		# Queue(maxsize=ACCOUNTS_Q_MAX)
		retryQs  : dict[str, Queue[BSAccount] | None]	= dict() 
		statsQ	 : Queue[list[WGplayerAchievementsMaxSeries]]	= Queue(maxsize=PLAYER_ACHIEVEMENTS_Q_MAX)
		accounts_N 		: int = 0
		accounts_added 	: int = 0

		tasks : list[Task] = list()
		tasks.append(create_task(update_player_achievements_worker(db, statsQ)))
		for r in regions:
			accountQs[r.name] = Queue(maxsize=ACCOUNTS_Q_MAX)
			retryQs[r.name] = Queue(maxsize=ACCOUNTS_Q_MAX)
			
			for _ in range(int(args.threads/len(regions))):
				tasks.append(create_task(update_player_achievements_api_worker(db, wg_api=wg, region=r, 
																	accountQ=accountQs[r.name], statsQ=statsQ, 
																	retryQ=retryQs[r.name])))

		# count number of accounts
		if args.accounts is not None:
			accounts_N = len(args.accounts)			
		elif args.file is not None:
			message(f'Reading accounts from {args.file}')
			async with open(args.file, mode='r') as f:
				async for accounts_N, _ in enumerate(f):
					pass
			accounts_N += 1
			if args.file.endswith('.csv'):
				accounts_N -= 1
		else:
			if args.sample > 1:
				accounts_N = int(args.sample)
			else:				
				message('Counting accounts to fetch stats...')
				accounts_N = await db.accounts_count(stats_type=StatsTypes.player_achievements, 
													regions=set(regions), sample=args.sample, 
													cache_valid=args.cache_valid)

		if accounts_N == 0:
			raise ValueError('No accounts to update player achievements for')		
		aq : int = 0
		with alive_bar(accounts_N, title= "Fetching player achievements", manual=True, enrich_print=False) as bar:
			
			if args.accounts is not None:	
				async for accounts_added, account_id in enumerate(args.accounts):
					try:
						a : BSAccount = BSAccount(id=account_id)
						ar : Region | None = a.region
						if ar is None:
							continue
						await accountQs[ar.name].put(a)
					except Exception as err:
						error(f'Could not add account ({account_id}) to queue')
					finally:
						aq : int = 0
						for r in regions:
							aq += accountQs[r.name].qsize()
						bar((accounts_added + 1 - aq)/accounts_N)

			elif args.file is not None:

				if args.file.endswith('.txt'):
					async for accounts_added, account in enumerate(BSAccount.import_txt(args.file)):
						try:
							await accountQ.put(account)
						except Exception as err:
							error(f'Could not add account to the queue: {err}')
						finally:
							bar((accounts_added + 1 - accountQ.qsize())/accounts_N)

				elif args.file.endswith('.csv'):					
					async for accounts_added, account in enumerate(BSAccount.import_csv(args.file)):
						try:
							await accountQ.put(account)
						except Exception as err:
							error(f'Could not add account to the queue: {err}')
						finally:
							bar((accounts_added + 1 - accountQ.qsize())/accounts_N)

				elif args.file.endswith('.json'):				
					async for accounts_added, account in enumerate(BSAccount.import_json(args.file)):
						try:
							await accountQ.put(account)
						except Exception as err:
							error(f'Could not add account to the queue: {err}')
						finally:
							bar((accounts_added + 1 - accountQ.qsize())/accounts_N)
			
			else:
				async for accounts_added, account in enumerate(db.accounts_get(stats_type=StatsTypes.player_achievements, 
													regions=regions, sample=args.sample, cache_valid=args.cache_valid)):
					try:
						await accountQ.put(account)
					except Exception as err:
						error(f'Could not add account ({account.id}) to queue')
					finally:	
						bar((accounts_added + 1 - accountQ.qsize())/accounts_N)

			incomplete : bool = False				
			while accountQ.qsize() > 0:
				incomplete = True
				await sleep(1)
				bar((accounts_added + 1 - accountQ.qsize())/accounts_N)
			if incomplete:						# FIX the edge case of queue getting processed before while loop
				bar(1)

		await accountQ.join()

		if retryQ is not None and not retryQ.empty():
			tasks.append(create_task(update_player_achievements_api_worker(db, wg_api=wg, regions=regions, accountQ=retryQ, statsQ=statsQ)))
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


async def update_player_achievements_api_worker(db: Backend, wg_api : WGApi, region: Region, 
										accountQ: Queue[list[BSAccount]], 
										statsQ: Queue[list[WGplayerAchievementsMaxSeries]], 
										retryQ: Queue[BSAccount] | None = None, 
										check_invalid : bool = False) -> EventCounter:
	"""Async worker to fetch player achievements from WG API"""
	debug('starting')
	stats = EventCounter('WG API')
	try:
		while True:
			accounts : list[BSAccount] = await accountQ.get()
			try:
				debug(f'account_id: {account.id}')
				stats.log('accounts total')
				
				if account.region != region:
					raise ValueError(f"account_id's ({account.id}) region ({account.region}) is different from API region ({region})")
				player_achievements : list[WGplayerAchievementsMaxSeries] | None = await wg_api.get_player_achievements(account.id, account.region)

				if player_achievements is None:
					
					debug(f'Could not fetch account: {account.id}')
					if retryQ is not None:
						stats.log('accounts to re-try')
						await retryQ.put(account)
					else:
						stats.log('accounts w/o stats')
						account.disabled = True
						await db.account_update(account=account, fields=['disabled'])
						stats.log('accounts disabled')
				else:
					
					await statsQ.put(player_achievements)
					stats.log('player achievements fetched', len(player_achievements))
					stats.log('accounts /w stats')
					if check_invalid:
						account.disabled = False
						await db.account_update(account=account, fields=['disabled'])
						stats.log('accounts enabled')

			except Exception as err:
				stats.log('errors')
				error(f'{err}')
			finally:
				accountQ.task_done()
		
	except CancelledError as err:
		debug(f'Cancelled')	
	except Exception as err:
		error(f'{err}')
	stats.log('accounts total', - stats.get_value('accounts to re-try'))  # remove re-tried accounts from total
	return stats


async def update_player_achievements_worker(db: Backend, statsQ: Queue[list[WGplayerAchievementsMaxSeries]]) -> EventCounter:
	"""Async worker to add player achievements to backend. Assumes batch is for the same account"""
	debug('starting')
	stats 		: EventCounter = EventCounter(f'Backend ({db.driver})')
	added 		: int
	not_added 	: int
	account 	: BSAccount | None
	account_id	: int
	last_battle_time : int

	try:
		releases : BucketMapper[BSBlitzRelease] = BucketMapper[BSBlitzRelease](attr='cut_off')
		async for r in db.releases_get():
			releases.insert(r)
		while True:
			player_achievements : list[WGplayerAchievementsMaxSeries] = await statsQ.get()			
			added 			= 0
			not_added 		= 0
			last_battle_time = -1
			account			= None
			try:
				if len(player_achievements) > 0:
					debug(f'Read {len(player_achievements)} from queue')
					last_battle_time = max( [ ts.last_battle_time for ts in player_achievements] )
					for ts in player_achievements:
						rel : BSBlitzRelease | None = releases.get(ts.last_battle_time)
						if rel is not None:
							ts.release = rel.release
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
