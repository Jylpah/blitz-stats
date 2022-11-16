from argparse import ArgumentParser, Namespace
from configparser import ConfigParser
from typing import Optional, Iterable, cast
import logging
from asyncio import create_task, gather, Queue, CancelledError, Task, sleep
from alive_progress import alive_bar		# type: ignore

from backend import Backend, OptAccountsInactive, ACCOUNTS_Q_MAX
from models import Account
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

###########################################
# 
# add_args_ functions  
#
###########################################

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
		parser.add_argument('--region', type=str, choices=['any'] + [ r.name for r in Region ], 
							default=Region.API.name, help='Filter by region (default is API = eu + com + asia)')
		parser.add_argument('--sample', type=float, default=0, metavar='SAMPLE',
							help='Update tank stats for SAMPLE of accounts. If 0 < SAMPLE < 1, SAMPLE defines a %% of users')
		parser.add_argument('--distributed', '--dist',type=str, dest='distributed', metavar='I:N', 
							default=None, help='Distributed update for accounts: id %% N == I')
		parser.add_argument('--check-invalid', dest='chk_invalid', action='store_true', default=False, 
							help='Re-check invalid accounts')
		parser.add_argument('--check-inactive', dest='chk_inactive', action='store_true', default=False, 
							help='Re-check inactive accounts')
		parser.add_argument('--accounts', type=int, default=None, nargs='*', metavar='ACCOUNT_ID [ACCOUNT_ID1 ...]',
							help='Update tank stats for the listed ACCOUNT_ID(s)')
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
			#return await cmd_tank_stats_export(db, args)
			pass

		elif args.tank_stats_cmd == 'import':
			# return await cmd_accounts_import(db, args)
			pass
			
	except Exception as err:
		error(str(err))
	return False


async def cmd_tank_stats_update(db: Backend, args : Namespace) -> bool:
	assert 'wg_app_id' in args and type(args.wg_app_id) is str, "'wg_app_id' must be set and string"
	assert 'rate_limit' in args and (type(args.rate_limit) is float or \
			type(args.rate_limit) is int), "'rate_limit' must set and a number"	
	assert 'region' in args and type(args.region) is str, "'region' must be set and string"
	
	try:
		debug('starting')
		
		stats 	: EventCounter	= EventCounter('tank-stats update')
		wg 		: WGApi 		= WGApi(WG_app_id=args.wg_app_id, rate_limit=args.rate_limit)
		region 	: Region 		= Region(args.region)
		accountQ : Queue[Account] 			= Queue(maxsize=ACCOUNTS_Q_MAX)
		statsQ	 : Queue[list[WGtankStat]] 	= Queue(maxsize=TANK_STATS_Q_MAX)

		tasks : list[Task] = list()
		# tasks.append(create_task(add_tank_stats_worker(db, statsQ)))
		for i in range(args.threads):
			tasks.append(create_task(update_tank_stats_api_worker(wg, region, accountQ, statsQ)))

		if args.accounts is not None:
			if len(args.accounts) == 0:
				raise ValueError('--accounts requires account_id(s) as parameters')
			for account_id in args.accounts:
				try:
					await accountQ.put(Account(_id=account_id))
				except Exception as err:
					error(f'Could not add account ({account_id}) to queue')
		debug('print out API results')
		await sleep(3)
		while not statsQ.empty():
			tank_stats : list[WGtankStat] = await statsQ.get()
			for tank_stat in tank_stats:
				print(tank_stat.json_src())
			await sleep(1)

		for task in tasks:
			task.cancel()	
		
		return True
	except Exception as err:
		error(str(err))
	return False

async def update_tank_stats_api_worker(wg : WGApi, region: Region, accountQ: Queue[Account], 
										statsQ: Queue[list[WGtankStat]]) -> EventCounter:
	"""Async worker to fetch tank stats from WG API"""
	debug('starting')
	stats = EventCounter('WG API')
	try:
		while True:
			account : Account = await accountQ.get()
			try:
				debug(f'account_id: {account.id}')
				stats.log('accounts total')
				tank_stats : list[WGtankStat] | None = await wg.get_tank_stats(account.id, region)
				
				if tank_stats is None:
					verbose(f'Could not fetch account: {account.id}')
					stats.log('accounts w/o stats')
					continue
				else:
					await statsQ.put(tank_stats)
					stats.log('tank stats added', len(tank_stats))
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
	"""Async worker to add tank stats to backend"""
	debug('starting')
	stats 		: EventCounter = EventCounter(f'Backend ({db.name})')
	added 		: int
	not_added 	: int
	try:
		while True:
			tank_stats : list[WGtankStat] = await statsQ.get()
			added 		= 0
			not_added 	= 0
			try:
				debug(f'Read {len(tank_stats)} from queue')
				stats.log('tank stats total', len(tank_stats))
				added, not_added= await db.tank_stats_insert(tank_stats)

			except Exception as err:
				error(str(err))
			finally:
				stats.log('tank stats added', added)
				stats.log('old tank stats found', not_added)				
				statsQ.task_done()
	except CancelledError as err:
		debug(f'Cancelled')	
	except Exception as err:
		error(str(err))
	return stats