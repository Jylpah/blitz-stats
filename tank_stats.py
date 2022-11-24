from argparse import ArgumentParser, Namespace
from configparser import ConfigParser
from typing import Optional, Iterable, cast
import logging
from asyncio import create_task, gather, Queue, CancelledError, Task, sleep
from aiofiles import open
from asyncstdlib import enumerate
from alive_progress import alive_bar		# type: ignore

from backend import Backend, OptAccountsInactive, ACCOUNTS_Q_MAX, CACHE_VALID
from models import BSAccount, StatsTypes
from pyutils.eventcounter import EventCounter
from pyutils.utils import get_url, get_url_JSON_model, epoch_now
from blitzutils.models import WoTBlitzReplayJSON, Region, WGApiWoTBlitzTankStats, WGtankStat
from blitzutils.wg import WGApi 

logger = logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug

# Constants

WORKERS_WGAPI 		: int = 40
TANK_STATS_Q_MAX 	: int = 1000

########################################################
# 
# add_args_ functions  
#
########################################################

def add_args_tank_stats(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		tank_stats_parsers = parser.add_subparsers(dest='tank_stats_cmd', 	
												title='tank-stats commands',
												description='valid commands',
												metavar='update | prune | import | export')
		tank_stats_parsers.required = True
		
		update_parser = tank_stats_parsers.add_parser('update', aliases=['get'], help="tank-stats update help")
		if not add_args_tank_stats_update(update_parser, config=config):
			raise Exception("Failed to define argument parser for: tank-stats update")
		
		prune_parser = tank_stats_parsers.add_parser('prune', help="tank-stats prune help")
		if not add_args_tank_stats_prune(prune_parser, config=config):
			raise Exception("Failed to define argument parser for: tank-stats prune")

		import_parser = tank_stats_parsers.add_parser('import', help="tank-stats import help")
		if not add_args_tank_stats_import(import_parser, config=config):
			raise Exception("Failed to define argument parser for: tank-stats import")

		export_parser = tank_stats_parsers.add_parser('export', help="tank-stats export help")
		if not add_args_tank_stats_export(export_parser, config=config):
			raise Exception("Failed to define argument parser for: tank-stats export")
		debug('Finished')	
		return True
	except Exception as err:
		error(str(err))
	return False


def add_args_tank_stats_update(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
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
							default=list(Region.API_regions()), help='Filter by region (default: eu + com + asia + ru)')
		parser.add_argument('--sample', type=float, default=0, metavar='SAMPLE',
							help='Update tank stats for SAMPLE of accounts. If 0 < SAMPLE < 1, SAMPLE defines a %% of users')
		parser.add_argument('--older-than', type=int, default=CACHE_VALID, metavar='DAYS',
							help='Update only accounts with stats older than DAYS')		
		parser.add_argument('--distributed', '--dist',type=str, dest='distributed', metavar='I:N', 
							default=None, help='Distributed update for accounts: id %% N == I')
		parser.add_argument('--check-invalid', action='store_true', default=False, 
							help='Re-check invalid accounts')
		parser.add_argument('--inactive', type=str, choices=[ o.value for o in OptAccountsInactive ], 
								default=OptAccountsInactive.default().value, help='Include inactive accounts')
		parser.add_argument('--accounts', type=int, default=None, nargs='*', metavar='ACCOUNT_ID [ACCOUNT_ID1 ...]',
							help='Update tank stats for the listed ACCOUNT_ID(s)')
		parser.add_argument('--file',type=str, metavar='FILENAME', default=None, 
							help='Read account_ids from FILENAME one account_id per line')

		return True
	except Exception as err:
		error(str(err))
	return False

def add_args_tank_stats_prune(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	debug('starting')
	return True


def add_args_tank_stats_import(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	debug('starting')
	return True


def add_args_tank_stats_export(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	debug('starting')
	return True


###########################################
# 
# cmd_ functions  
#
###########################################

async def cmd_tank_stats(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		if args.tank_stats_cmd == 'update':
			return await cmd_tank_stats_update(db, args)

		elif args.tank_stats_cmd == 'export':
			return await cmd_tank_stats_export(db, args)

		elif args.tank_stats_cmd == 'import':
			# return await cmd_accounts_import(db, args)
			pass
			
	except Exception as err:
		error(str(err))
	return False

########################################################
# 
# cmd_tank_stats_update()
#
########################################################

async def cmd_tank_stats_update(db: Backend, args : Namespace) -> bool:
	"""Update tank stats"""
	assert 'wg_app_id' in args and type(args.wg_app_id) is str, "'wg_app_id' must be set and string"
	assert 'rate_limit' in args and (type(args.rate_limit) is float or \
			type(args.rate_limit) is int), "'rate_limit' must set and a number"	
	assert 'region' in args and type(args.region) is list, "'region' must be set and a list"
	
	debug('starting')
	inactive : OptAccountsInactive = OptAccountsInactive.default()
	try: 
		inactive = OptAccountsInactive(args.inactive)
	except ValueError as err:
		assert False, f"Incorrect value for argument 'inactive': {args.inactive}"
	wg 	: WGApi = WGApi(WG_app_id=args.wg_app_id, rate_limit=args.rate_limit)

	try:
		stats 	 : EventCounter				= EventCounter('tank-stats update')
		regions	 : set[Region] 				= { Region(r) for r in args.region }
		accountQ : Queue[BSAccount] 			= Queue(maxsize=ACCOUNTS_Q_MAX)
		statsQ	 : Queue[list[WGtankStat]] 	= Queue(maxsize=TANK_STATS_Q_MAX)

		tasks : list[Task] = list()
		tasks.append(create_task(add_tank_stats_worker(db, statsQ)))
		for i in range(args.threads):
			tasks.append(create_task(update_tank_stats_api_worker(wg, regions, accountQ, statsQ)))

		accounts_N 		: int = 0
		accounts_added 	: int = 0 
		# qsize 			: int = 0

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
				accounts_N = await db.accounts_count(stats_type=StatsTypes.tank_stats, regions=regions, 
													inactive=inactive, disabled=args.check_invalid, 
													sample=args.sample, force=args.force, cache_valid=args.older_than)

		if accounts_N == 0:
			raise ValueError('No accounts to update tank stats for')		

		with alive_bar(accounts_N, title= "Fetching tank stats", manual=True, enrich_print=False) as bar:
			if args.accounts is not None:	
				async for accounts_added, account_id in enumerate(args.accounts):
					try:
						await accountQ.put(BSAccount(id=account_id))
					except Exception as err:
						error(f'Could not add account ({account_id}) to queue')
					finally:
						bar((accounts_added + 1 - accountQ.qsize())/accounts_N)

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
				async for accounts_added, account in enumerate(db.accounts_get(stats_type=StatsTypes.tank_stats, 
													regions=regions, inactive=inactive, disabled=args.check_invalid, 
													sample=args.sample, force=args.force, cache_valid=args.older_than)):
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
		await statsQ.join()

		for task in tasks:
			task.cancel()
		
		for ec in await gather(*tasks, return_exceptions=True):
			stats.merge_child(ec)
		message(stats.print(do_print=False))
		return True
	except Exception as err:
		error(str(err))
	finally:
	
		wg_stats : dict[str, str] | None = wg.print_server_stats()
		if wg_stats is not None and logger.level <= logging.WARNING:
			message('WG API stats:')
			for server in wg_stats:
				message(f'{server.capitalize():7s}: {wg_stats[server]}')
		await wg.close()
	return False

async def update_tank_stats_api_worker(wg : WGApi, regions: set[Region], accountQ: Queue[BSAccount], 
										statsQ: Queue[list[WGtankStat]]) -> EventCounter:
	"""Async worker to fetch tank stats from WG API"""
	debug('starting')
	stats = EventCounter('WG API')
	try:
		while True:
			account : BSAccount = await accountQ.get()
			try:
				debug(f'account_id: {account.id}')
				stats.log('accounts total')
				
				if account.region not in regions:
					raise ValueError(f"account_id's ({account.id}) region ({account.region}) is not in defined regions ({', '.join(regions)})")
				tank_stats : list[WGtankStat] | None = await wg.get_tank_stats(account.id, account.region)

				if tank_stats is None:
					verbose(f'Could not fetch account: {account.id}')
					stats.log('accounts w/o stats')
					continue
				else:
					await statsQ.put(tank_stats)
					stats.log('tank stats fetched', len(tank_stats))
					stats.log('accounts /w stats')

			except Exception as err:
				stats.log('errors')
				error(str(err))
			finally:
				accountQ.task_done()
		
	except CancelledError as err:
		debug(f'Cancelled')	
	except Exception as err:
		error(str(err))
	return stats


async def add_tank_stats_worker(db: Backend, statsQ: Queue[list[WGtankStat]]) -> EventCounter:
	"""Async worker to add tank stats to backend. Assumes batch is for the same account"""
	debug('starting')
	stats 		: EventCounter = EventCounter(f'Backend ({db.name})')
	added 		: int
	not_added 	: int
	account 	: BSAccount | None
	account_id	: int
	try:		
		while True:
			tank_stats : list[WGtankStat] = await statsQ.get()			
			added 		= 0
			not_added 	= 0
			account		= None
			try:
				if len(tank_stats) > 0:
					debug(f'Read {len(tank_stats)} from queue')					
					added, not_added= await db.tank_stats_insert(tank_stats)
					account_id = tank_stats[0].account_id
					if (account := await db.account_get(account_id=account_id)) is None:
						account = BSAccount(id=account_id)
					debug(f'{account}')
					account.stats_updated(StatsTypes.tank_stats)
					if added > 0:
						account.inactive = False
						stats.log('accounts /w new stats')
					else:
						account.inactive = True
						stats.log('accounts w/o new stats')
					await db.account_update(account=account)
			except Exception as err:
				error(str(err))
			finally:
				stats.log('tank stats added', added)
				stats.log('old tank stats found', not_added)
				debug(f'{added} tank stats added, {not_added} old tank stats found')				
				statsQ.task_done()
	except CancelledError as err:
		debug(f'Cancelled')	
	except Exception as err:
		error(str(err))
	return stats


########################################################
# 
# cmd_tank_stats_export()
#
########################################################

async def cmd_tank_stats_export(db: Backend, args : Namespace) -> bool:
	return False