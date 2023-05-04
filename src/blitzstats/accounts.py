from argparse import ArgumentParser, Namespace
from configparser import ConfigParser
from typing import Optional, cast, Type, Any, TypeVar, Sequence
from datetime import datetime, timedelta
from time import time
import logging
from asyncio import create_task, gather, wait, Queue, CancelledError, Task, sleep
from aiofiles import open
from asyncstdlib import enumerate
from alive_progress import alive_bar		# type: ignore

from pyutils 			import EventCounter, TXTExportable, CSVExportable, \
	 							 JSONExportable, IterableQueue, QueueDone
from pyutils.exportable import  export
from pyutils.utils 		import alive_bar_monitor, get_url_JSON_model, chunker

from blitzutils			import WoTBlitzReplayJSON, WoTBlitzReplayData, \
								Region, Account, WGAccountInfo, \
								WGApi, WoTinspector

from .backend import Backend, OptAccountsInactive, \
					 OptAccountsDistributed, \
					 BSTableType, ACCOUNTS_Q_MAX, get_sub_type
from .models import BSAccount, StatsTypes, BSBlitzRelease
from .models_import import WG_Account


logger 	= logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug

# wotinspector.com
WI_MAX_PAGES 	: int 				= 100
WI_MAX_OLD_REPLAYS: int 			= 30
WI_RATE_LIMIT	: Optional[float] 	= None
WI_AUTH_TOKEN	: Optional[str] 	= None

# yastati.st
YS_CLIENT_ID 		: str = 'none'
YS_CLIENT_SECRET 	: str = 'missing'
YS_DAYS_SINCE 		: int = 30

EXPORT_SUPPORTED_FORMATS : list[str] = ['json', 'txt', 'csv']

ACCOUNT_INFO_CACHE_VALID : int = 7 # days

###########################################
# 
# add_args_accouts functions  
#
###########################################

def add_args(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		accounts_parsers = parser.add_subparsers(dest='accounts_cmd', 	
												title='accounts commands',
												description='valid commands',
												metavar='fetch | update | export | remove')
		accounts_parsers.required = True
		
		fetch_parser = accounts_parsers.add_parser('fetch', aliases=['get'], help="accounts fetch help")
		if not add_args_fetch(fetch_parser, config=config):
			raise Exception("Failed to define argument parser for: accounts fetch")
		
		update_parser = accounts_parsers.add_parser('update', help="accounts update help")
		if not add_args_update(update_parser, config=config):
			raise Exception("Failed to define argument parser for: accounts update")

		export_parser = accounts_parsers.add_parser('export', help="accounts export help")
		if not add_args_export(export_parser, config=config):
			raise Exception("Failed to define argument parser for: accounts export")
		
		remove_parser = accounts_parsers.add_parser('remove', aliases=['rm'], help="accounts remove help")
		if not add_args_remove(remove_parser, config=config):
			raise Exception("Failed to define argument parser for: accounts remove")

		import_parser = accounts_parsers.add_parser('import', help="accounts import help")
		if not add_args_import(import_parser, config=config):
			raise Exception("Failed to define argument parser for: accounts import")	
		
		return True
	except Exception as err:
		error(f'{err}')
	return False


def add_args_update(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		update_parsers = parser.add_subparsers(dest='accounts_update_source', 	
													title='accounts update source',
													description='valid sources', 
													metavar='wg | files')
		update_parsers.required = True

		update_wg_parser = update_parsers.add_parser('wg', help='accounts update wg help')
		if not add_args_update_wg(update_wg_parser, config=config):
			raise Exception("Failed to define argument parser for: accounts update wg")

		update_files_parser = update_parsers.add_parser('files', help='accounts update files help')
		if not add_args_update_files(update_files_parser, config=config):
			raise Exception("Failed to define argument parser for: accounts update files")		
		
		parser.add_argument('--force', action='store_true', default=False, 
							help='add accounts not found in the backend')
		return True	
	except Exception as err:
		error(f'{err}')
	return False


def add_args_update_files(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	"""Update accounts from file(s)"""
	debug(f'add_args_update_files(): not implemented')
	return True


def add_args_update_wg(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	"""Update existing accounts from WG API"""
	try:
		debug('starting')		

		WG_RATE_LIMIT 	: float = 10
		WG_WORKERS 		: int 	= 10
		WG_APP_ID		: str 	= WGApi.DEFAULT_WG_APP_ID
		# Lesta / RU
		LESTA_RATE_LIMIT: float = 10
		LESTA_WORKERS 	: int 	= 10
		LESTA_APP_ID 	: str 	= WGApi.DEFAULT_LESTA_APP_ID
		# NULL_RESPONSES 	: int 	= 20
		
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

		parser.add_argument('--wg-workers', dest='wg_workers', 
							type=int, default=WG_WORKERS, metavar='WORKERS',
							help='number of async workers')
		parser.add_argument('--wg-app-id', type=str, default=WG_APP_ID, metavar='APP_ID',
							help='Set WG APP ID')
		parser.add_argument('--wg-rate-limit', type=float, default=WG_RATE_LIMIT, 
		      				metavar='RATE_LIMIT', help='rate limit for WG API per server')
		parser.add_argument('--ru-app-id', type=str, default=LESTA_APP_ID, metavar='APP_ID',
							help='Set Lesta (RU) APP ID')
		parser.add_argument('--ru-rate-limit', type=float, default=LESTA_RATE_LIMIT, 
		      				metavar='RATE_LIMIT', help='Rate limit for Lesta (RU) API')
		parser.add_argument('--regions', '--region', type=str, nargs='*', 
							choices=[ r.value for r in Region.API_regions() ], 
							default=[ r.value for r in Region.API_regions() ], 
							help='filter by region (default: ' + ' + '.join(Region.API_regions()) + ')')
		parser.add_argument('--disabled', action='store_true', default=False, 
							help='Check existing disabled accounts')
		parser.add_argument('--active-since', type=str, default=None, metavar='RELEASE/DAYS',
							help='update account info for accounts that have been active since RELEASE/DAYS')
		parser.add_argument('--inactive-since', type=str,  default=None, metavar='RELEASE/DAYS',
							help='update account info for accounts that have been inactive since RELEASE/DAYS')		
		parser.add_argument('--inactive', type=str, choices=[ o.value for o in OptAccountsInactive ], 
								default=OptAccountsInactive.both.value, help='Include inactive accounts')
		parser.add_argument('--accounts', type=str, default=[], nargs='*', metavar='ACCOUNT_ID [ACCOUNT_ID1 ...]',
								help="update accounts for the listed ACCOUNT_ID(s). \
									ACCOUNT_ID format 'account_id:region' or 'account_id'")		
		# parser.add_argument('--start', dest='wg_start_id', 
		# 					metavar='ACCOUNT_ID', type=int, default=0, 
		# 					help='start fetching account_ids from ACCOUNT_ID (default = 0 \
		# 						start from highest ACCOUNT_ID in backend)')
		parser.add_argument('--distributed', '--dist',type=str, dest='distributed', metavar='I:N', 
							default=None, help='Distributed stats fetching for accounts: id %% N == I')
		parser.add_argument('--cache-valid', type=float, default=ACCOUNT_INFO_CACHE_VALID, metavar='DAYS',
							help='Fetch stats only for accounts with stats older than DAYS')
		parser.add_argument('--sample', type=float, default=0, metavar='SAMPLE',
							help='update SAMPLE of accounts. If 0 < SAMPLE < 1, SAMPLE defines a %% of users')
		parser.add_argument('--file', metavar='FILE', type=str, default=None, 
							help='file to read accounts to update from')
		parser.add_argument('--format', type=str, choices=['json', 'txt', 'csv', 'auto'], 
							default='json', help='accounts list format')
							
		return True	
	except Exception as err:
		error(f'{err}')
	return False
	

def add_args_fetch(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		fetch_parsers = parser.add_subparsers(dest='accounts_fetch_source', 	
														title='accounts fetch source',
														description='valid sources', 
														metavar='wg | wi | files')
		fetch_parsers.required = True
		fetch_wg_parser = fetch_parsers.add_parser('wg', help='accounts fetch wg help')
		if not add_args_fetch_wg(fetch_wg_parser, config=config):
			raise Exception("Failed to define argument parser for: accounts fetch wg")		

		fetch_wi_parser = fetch_parsers.add_parser('wi', help='accounts fetch wi help')
		if not add_args_fetch_wi(fetch_wi_parser, config=config):
			raise Exception("Failed to define argument parser for: accounts fetch wi")
		
		# fetch_ys_parser = fetch_parsers.add_parser('ys', help='accounts fetch ys help')
		# if not add_args_fetch_ys(fetch_ys_parser, config=config):
		# 	raise Exception("Failed to define argument parser for: accounts fetch ys")

		fetch_files_parser = fetch_parsers.add_parser('files', help='accounts fetch files help')
		if not add_args_fetch_files(fetch_files_parser, config=config):
			raise Exception("Failed to define argument parser for: accounts fetch files")		
		
		parser.add_argument('--force', action='store_true', default=False, 
							help='Ignore existing accounts exporting')
		
		return True	
	except Exception as err:
		error(f'{err}')
	return False



def add_args_fetch_wg(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
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

		parser.add_argument('--wg-workers', dest='wg_workers', 
							type=int, default=WG_WORKERS, metavar='WORKERS',
							help='number of async workers')
		parser.add_argument('--wg-app-id', type=str, default=WG_APP_ID, metavar='APP_ID',
							help='Set WG APP ID')
		parser.add_argument('--wg-rate-limit', type=float, default=WG_RATE_LIMIT, 
		      				metavar='RATE_LIMIT', help='rate limit for WG API per server')
		parser.add_argument('--ru-app-id', type=str, default=LESTA_APP_ID, metavar='APP_ID',
							help='Set Lesta (RU) APP ID')
		parser.add_argument('--ru-rate-limit', type=float, default=LESTA_RATE_LIMIT, 
		      				metavar='RATE_LIMIT', help='Rate limit for Lesta (RU) API')
		parser.add_argument('--regions', '--region', type=str, nargs='*', 
							choices=[ r.value for r in Region.API_regions() ], 
							default=[ r.value for r in Region.API_regions() ], 
							help='filter by region (default: ' + ' + '.join(Region.API_regions()) + ')')
		parser.add_argument('--start', dest='wg_start_id', 
							metavar='ACCOUNT_ID', type=int, default=0, 
							help='start fetching account_ids from ACCOUNT_ID (default = 0 \
								start from highest ACCOUNT_ID in backend)')
		parser.add_argument('--force', action='store_true', default=False, 
							help='fetch accounts starting from --start ACCOUNT_ID')
		parser.add_argument('--max', dest='max_accounts', 
							type=int, default=0, metavar='ACCOUNT_IDS',
							help='maximum number of accounts to try')
		parser.add_argument('--end', dest='null_responses', 
							type=int, default=NULL_RESPONSES, metavar='N',
							help='end fetching accounts after N consequtive empty responses')
		parser.add_argument('--file',type=str, metavar='FILENAME', default=None, 
							help='Read account_ids from FILENAME one account_id per line')
							
		return True	
	except Exception as err:
		error(f'{err}')
	return False


def add_args_fetch_wi(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		global WI_MAX_OLD_REPLAYS

		WI_RATE_LIMIT 	: float  		= 20/3600
		WI_MAX_PAGES 	: int 			= 100
		WI_AUTH_TOKEN 	: Optional[str] = None
		WI_WORKERS		: int 			= 2		

		if config is not None and 'WOTINSPECTOR' in config.sections():
			configWI 		= config['WOTINSPECTOR']
			WI_RATE_LIMIT	= configWI.getfloat('rate_limit', WI_RATE_LIMIT)
			WI_MAX_PAGES	= configWI.getint('max_pages', WI_MAX_PAGES)
			WI_WORKERS		= configWI.getint('workers', WI_WORKERS)
			WI_AUTH_TOKEN	= configWI.get('auth_token', WI_AUTH_TOKEN)
		parser.add_argument('--max', '--max-pages', dest='wi_max_pages', 
							type=int, default=WI_MAX_PAGES, metavar='MAX_PAGES',
							help='Maximum number of pages to spider')
		parser.add_argument('--start','--start_page',   dest='wi_start_page', 
							metavar='START_PAGE', type=int, default=0, 
							help='Start page to start spidering of WoTinspector.com')
		parser.add_argument('--workers', dest='wi_workers', 
							type=int, default=WI_WORKERS, metavar='WORKERS',
							help='Number of async workers to spider wotinspector.com')
		parser.add_argument('--old-replay-limit', dest='wi_max_old_replays', 
							type=int, default=WI_MAX_OLD_REPLAYS, metavar='OLD-REPLAYS',
							help='Cancel spidering after number of old replays found')
		parser.add_argument('--wi-auth-token', dest='wi_auth_token', 
							type=str, default=WI_AUTH_TOKEN, metavar='AUTH_TOKEN',
							help='Start page to start spidering of WoTinspector.com')
		parser.add_argument('--wi-rate-limit', type=float, default=WI_RATE_LIMIT, metavar='RATE_LIMIT',
							help='Rate limit for WoTinspector.com')
		
		return True	
	except Exception as err:
		error(f'{err}')
	return False


def add_args_fetch_ys(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		global YS_CLIENT_ID, YS_CLIENT_SECRET, YS_DAYS_SINCE

		if config is not None and 'YASTATIST' in config.sections():
			configYS 		= config['YASTATIST']
			# YS_RATE_LIMIT	= configYS.getfloat('rate_limit', YS_RATE_LIMIT)			
			YS_CLIENT_ID	= configYS.get('client_id', YS_CLIENT_ID)
			YS_CLIENT_SECRET= configYS.get('client_secret', YS_CLIENT_SECRET)
			YS_DAYS_SINCE 	= configYS.getint('days_since', YS_DAYS_SINCE)

		parser.add_argument('--since','--days-since',   dest='ys_days_since', 
							metavar='DAYS', type=int, default=YS_DAYS_SINCE, 
							help='fetch accounts that have been active since DAYS')
		parser.add_argument('--client-id', dest='ys_client_id', 
							type=str, default=YS_CLIENT_ID, metavar='CLIENT_ID',
							help='client ID for Yastati.st')
		parser.add_argument('--client-secret', dest='ys_client_secret', 
							type=str, default=YS_CLIENT_SECRET, metavar='CLIENT_SECRET',
							help='client secret for Yastati.st')
		
		return True	
	except Exception as err:
		error(f'{err}')
	return False


def add_args_fetch_files(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		IMPORT_FORMAT 	= 'txt'

		if config is not None and 'ACCOUNTS' in config.sections():
			configAccs 	= config['ACCOUNTS']
			IMPORT_FORMAT	= configAccs.get('import_format', IMPORT_FORMAT)
		parser.add_argument('--format', type=str, choices=['json', 'txt', 'csv', 'auto'], 
							default=IMPORT_FORMAT, help='Accounts list file format')
		parser.add_argument('files', metavar='FILE1 [FILE2 ...]', type=str, nargs='*', 
							default='-', help='Files to read. Use \'-\' for STDIN')		
		return True	
	except Exception as err:
		error(f'{err}')
	return False


def add_args_export(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		EXPORT_FORMAT 	= 'txt'
		EXPORT_FILE 	= 'accounts'

		if config is not None and 'ACCOUNTS' in config.sections():
			configAccs 	= config['ACCOUNTS']
			EXPORT_FORMAT	= configAccs.get('export_format', EXPORT_FORMAT)
			EXPORT_FILE		= configAccs.get('export_file', EXPORT_FILE )

		parser.add_argument('format', type=str, nargs='?', choices=EXPORT_SUPPORTED_FORMATS, 
		 					 default=EXPORT_FORMAT, help='Accounts list file format')
		parser.add_argument('filename', metavar='FILE', type=str, nargs='?', default=EXPORT_FILE, 
							help='File to export accounts to. Use \'-\' for STDIN')
		parser.add_argument('--append', action='store_true', default=False, help='Append to file(s)')
		parser.add_argument('--force', action='store_true', default=False, help='Overwrite existing file(s) when exporting')
		parser.add_argument('--disabled', action='store_true', default=False, help='Disabled accounts')
		parser.add_argument('--inactive', type=str, choices=[ o.value for o in OptAccountsInactive ], 
								default=OptAccountsInactive.no.value, help='Include inactive accounts')
		parser.add_argument('--active-since', type=str, default=None, metavar='RELEASE/DAYS',
							help='Fetch stats for accounts that have been active since RELEASE/DAYS')
		parser.add_argument('--inactive-since', type=str,  default=None, metavar='RELEASE/DAYS',
							help='Fetch stats for accounts that have been inactive since RELEASE/DAYS')		
		parser.add_argument('--regions', '--region', type=str, nargs='*', choices=[ r.value for r in Region.API_regions() ], 
								default=[ r.value for r in Region.API_regions() ], help='Filter by region (default is API = eu + com + asia)')
		parser.add_argument('--by-region', action='store_true', default=False, help='Export accounts by region')
		parser.add_argument('--distributed', '--dist',type=str, dest='distributed', metavar='I:N', 
							default=None, help='Distributed stats fetching for accounts: id %% N == I')
		parser.add_argument('--sample', type=float, default=0, help='Sample accounts')

		return True	
	except Exception as err:
		error(f'{err}')
	return False


def add_args_import(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	"""Add argument parser for accounts import"""
	try:
		debug('starting')
		
		import_parsers = parser.add_subparsers(dest='import_backend', 
												title='accounts import backend',
												description='valid backends', 
												metavar=' | '.join(Backend.list_available()))
		import_parsers.required = True

		for backend in Backend.get_registered():
			import_parser =  import_parsers.add_parser(backend.driver, help=f'accounts import {backend.driver} help')
			if not backend.add_args_import(import_parser, config=config):
				raise Exception(f'Failed to define argument parser for: accounts import {backend.driver}')
				
		parser.add_argument('--import-model', metavar='IMPORT-TYPE', type=str, required=True,
							choices=['BSAccount', 'WG_Account'], 
							help='Data format to import. Default is blitz-stats native format.')
		parser.add_argument('--regions', '--region', type=str, nargs='*', 
								choices=[ r.value for r in Region.has_stats() ], 
								default=[ r.value for r in Region.has_stats() ], 
								help='Filter by region (default is API = eu + com + asia)')
		parser.add_argument('--sample', type=float, default=0, 
								help='Sample size. 0 < SAMPLE < 1 : %% of stats, 1<=SAMPLE : Absolute number')
		parser.add_argument('--force', action='store_true', default=False, 
								help='Overwrite existing file(s) when exporting')

		return True
	except Exception as err:
		error(f'{err}')
	return False


def add_args_remove(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		IMPORT_FORMAT 	= 'txt'

		if config is not None:
			if 'ACCOUNTS' in config.sections():
				configAccs 	= config['ACCOUNTS']
				IMPORT_FORMAT	= configAccs.get('import_format', IMPORT_FORMAT)
					
		parser.add_argument('--format', type=str, choices=['json', 'txt', 'csv'], 
							default=IMPORT_FORMAT, help='Accounts list file format')
		account_src_parser = parser.add_mutually_exclusive_group()
		account_src_parser.add_argument('--file', metavar='FILE', type=str, default=None, 
										help='File to export accounts to. Use \'-\' for STDIN')	
		account_src_parser.add_argument('--accounts', metavar='ACCOUNT_ID [ACCOUNT_ID ...]', type=int, nargs='+', 
										help='accounts to remove')	
		
		return True	
	except Exception as err:
		error(f'{err}')
	return False

###########################################
# 
# cmd_accouts functions  
#
###########################################

async def cmd(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')

		if args.accounts_cmd == 'fetch':
			return await cmd_fetch(db, args)

		elif args.accounts_cmd == 'update':
			return await cmd_update(db, args)

		elif args.accounts_cmd == 'export':
			return await cmd_export(db, args)
		
		elif args.accounts_cmd == 'import':
			return await cmd_import(db, args)

		elif args.accounts_cmd == 'remove':
			return await cmd_remove(db, args)

	except Exception as err:
		error(f'{err}')
	return False



async def cmd_update(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		
		stats = EventCounter('accounts update', totals='total')
		accountQ : IterableQueue[BSAccount] = IterableQueue(maxsize=10000)
		db_worker = create_task(db.accounts_insert_worker(accountQ, force=args.force))
		
		try:
			if args.accounts_update_source == 'wg':
				debug('wg')
				stats.merge_child(await cmd_update_wg(db, args, accountQ))
			else:
				raise ValueError(f'unknown accounts update source: {args.accounts_update_source}')
			
		except Exception as err:
			error(f'{err}')

		await accountQ.join()
		await stats.gather_stats([db_worker])	
		stats.print()

	except Exception as err:
		error(f'{err}')
	return False


###########################################
# 
# cmd_update_wg()
#
###########################################


async def cmd_update_wg(db		: Backend, 
						args 	: Namespace, 
						accountQ 	: IterableQueue[BSAccount]) -> EventCounter:
	"""Update accounts from WG API"""
	debug('starting')
	stats		: EventCounter = EventCounter('WG API', totals='Total')
	try:		
		regions			: set[Region] 	= { Region(r) for r in args.regions }		
		wg 				: WGApi = WGApi(app_id = args.wg_app_id, 
										ru_app_id= args.ru_app_id,
										rate_limit = args.wg_rate_limit, 
										ru_rate_limit = args.ru_rate_limit,)
		WORKERS 		: int 	= max( [ args.wg_workers, 1 ])
		workQ_creators	: list[Task]	= list()
		api_workers 	: list[Task]	= list()
		# inQ				: IterableQueue[BSAccount] = IterableQueue(maxsize=ACCOUNTS_Q_MAX)
		workQs 			: dict[Region, IterableQueue[list[BSAccount]]] = dict()		

		for region in regions:
			workQs[region] = IterableQueue(maxsize=100)			
			workQ_creators.append(create_task(create_accountQ_batch(db, args, region, 
																	accountQ=workQs[region], 
														  			stats_type=StatsTypes.account_info)))
		#workQ_creators.append(create_task(split_accountQ_batch(inQ, workQs)))

		for region in regions:
			for _ in range(WORKERS):
				api_workers.append(create_task(update_account_info_worker(wg, region, 
																			workQs[region], accountQ)))
			
		with alive_bar(None, title='Updating accounts from WG API ') as bar:
			try:
				prev 	: int = 0
				done	: int
				while not accountQ.is_filled:
					done = accountQ.count
					if done - prev > 0:
						bar(done - prev)
					prev = done
					await sleep(1)						

			except KeyboardInterrupt:
				message('cancelled')
				for workQ in workQs.values():
					await workQ.shutdown()		
		await stats.gather_stats(workQ_creators, merge_child=False)
		for region in workQs.keys():
			debug(f'waiting for idQ for {region} to complete')
			await workQs[region].join()
		await stats.gather_stats(api_workers)
		wg.print()
		await wg.close()
	except Exception as err:
		error(f'{err}')	
	return stats


async def update_account_info_worker(wg		: WGApi, 
									region	: Region, 
									workQ	: IterableQueue[list[BSAccount]], 
									accountQ: IterableQueue[BSAccount], 
									) -> EventCounter:
	"""Update accounts with data from WG API accounts/info"""
	debug('starting')
	stats	: EventCounter = EventCounter(f'{region}')
	infos 	: list[WGAccountInfo] | None
	accounts: dict[int, BSAccount]
	ids		: list[int] = list()

	try:
		await accountQ.add_producer()

		while True:
			accounts = dict()
			for a in await workQ.get():
				accounts[a.id] = a
			N 	: int = len(accounts)			
			try:				
				stats.log('account_ids', N)
				if N == 0 or N > 100:
					raise ValueError(f'Incorrect number of account_ids give {N}')

				ids = [ a.id for a in accounts.values() ]
				ids_stats : list[int] = list()
				if (infos:= await wg.get_account_info(ids, region)) is not None:					
					stats.log('stats found', len(infos))
					
					# accounts with stats
					for info in infos:
						try:
							a = accounts[info.account_id]
							ids_stats.append(info.account_id)
							#error(f'updating account_id={a.id}: {info}')
							if a.update(info):
								#error(f'updated: {a}')
								await accountQ.put(a)
								stats.log('updated')
							else:
								debug(f'Could not update: account_id={a.id} region={a.region}')
								stats.log('not updated')
						except KeyError as err:
							error(f'{err}')
					
					# accounts w/o stats		
					no_stats : set[int] = set(ids) - set(ids_stats)	
					for account_id in no_stats:
						try:
							a = accounts[account_id]
							a.disabled = True
							await accountQ.put(a)
							# error(f'disabled account: {a}')
							stats.log('disabled')
						except KeyError as err:
							error(f'account w/o stats: {account_id}: {err}')
				else:
					stats.log('query errors')
				
			except ValueError:				
				stats.log('errors', N)				
			except Exception as err:
				stats.log('errors')
				error(f'{err}')	
			finally:
				# debug(f'accounts={len(accounts)}, left={left}')
				workQ.task_done()
		
	except QueueDone:
		debug('account_id queue is done')
	except CancelledError:
		debug('cancelled')
	except Exception as err:
		error(f'{err}')
	finally:
		debug(f'closing accountQ: {region}')
		await accountQ.finish()
	return stats


async def cmd_fetch(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		
		stats = EventCounter('accounts fetch')
		accountQ : IterableQueue[BSAccount] = IterableQueue(maxsize=ACCOUNTS_Q_MAX)
		db_worker = create_task(db.accounts_insert_worker(accountQ))

		try:
			if args.accounts_fetch_source == 'wg':
				debug('wg')
				stats.merge_child(await cmd_fetch_wg(db, args, accountQ))

			if args.accounts_fetch_source == 'wi':
				debug('wi')
				stats.merge_child(await cmd_fetch_wi(db, args, accountQ))
			
			# elif args.accounts_fetch_source == 'ys':
			# 	debug('ys')
			# 	stats.merge_child(await cmd_fetch_ys(db, args, accountQ))

			elif args.accounts_fetch_source == 'files':
				debug('files')
				stats.merge_child(await cmd_fetch_files(db, args, accountQ))
		except Exception as err:
			error(f'{err}')

		await accountQ.join()		
		await stats.gather_stats([db_worker])
		stats.print()

	except Exception as err:
		error(f'{err}')
	return False


# async def add_worker(db: Backend, accountQ: Queue[list[BSAccount]]) -> EventCounter:
# 	"""worker to read accounts from queue and add those to backend"""
# 	## REFACTOR: use Queue[list[BSAccount]] instead
# 	debug('starting')
# 	stats 		: EventCounter = EventCounter(f'{db.driver}')
# 	added 		: int
# 	not_added 	: int
# 	try:
# 		while True:
# 			accounts : list[BSAccount] = await accountQ.get()
# 			try:
# 				debug(f'Read {len(accounts)} from queue')
# 				stats.log('accounts total', len(accounts))
# 				try:					
# 					added, not_added= await db.accounts_insert(accounts)
# 					stats.log('accounts added', added)
# 					stats.log('old accounts found', not_added)
# 				except Exception as err:
# 					stats.log('errors')
# 					error(f'Cound not add accounts do {db.backend}: {err}')
# 			except Exception as err:
# 				error(f'{err}')
# 			finally:
# 				accountQ.task_done()
# 	except QueueDone:
# 		debug('account queue is done')
# 	except CancelledError as err:
# 		debug(f'Cancelled')
# 	except Exception as err:
# 		error(f'{err}')
# 	return stats


async def cmd_fetch_files(db: Backend, args : Namespace, 
							accountQ : Queue[BSAccount]) -> EventCounter:
	
	debug('starting')
	raise NotImplementedError


###########################################
# 
# cmd_fetch_wg()
#
###########################################


async def cmd_fetch_wg(db		: Backend, 
						args 	: Namespace, 
						accountQ: IterableQueue[BSAccount]) -> EventCounter:
	"""Fetch account_ids from WG API"""
	debug('starting')
	stats		: EventCounter = EventCounter('WG API')
	wg 			: WGApi 		= WGApi(app_id = args.wg_app_id, 
										ru_app_id= args.ru_app_id,
										rate_limit = args.wg_rate_limit,
										ru_rate_limit = args.ru_rate_limit,)
	id_creators	: list[Task]	= list()
	api_workers : list[Task]	= list()
	latest		: dict[Region, BSAccount] = dict()
	try:		
		regions		: set[Region] 	= { Region(r) for r in args.regions }
		start 		: int 			= args.wg_start_id
		if start > 0 and len(regions) > 1:
			raise ValueError('if --start > 0, only one region can be chosen')
		if args.file is not None and len(regions) > 1:
			raise ValueError('if --file set, only one region can be chosen')
		force 		: bool 			= args.force		
		null_responses: int 		= args.null_responses
		max_accounts: int 			= args.max_accounts

		WORKERS 	: int 			= max( [int(args.wg_workers), 1])

		idQs 		: dict[Region, IterableQueue[Sequence[int]]] = dict()
		
		if start == 0 and not force and args.file is None:				
			message('finding latest accounts by region')
			latest = await db.accounts_latest(regions)
		for region in regions:
			try:
				idQs[region] = IterableQueue(maxsize=100)
				for _ in range(WORKERS):
					api_workers.append(create_task(fetch_account_info_worker(wg, region,
                                                             idQs[region], accountQ,
                                                             null_responses=null_responses)))
				if args.file is None:
					id_range: range = region.id_range
					if start == 0 and not force:
						id_range = range(latest[region].id + 1, id_range.stop)
					else:
						id_range = range(start, id_range.stop)
					if max_accounts > 0:
						id_range = range(id_range.start, min(
							[id_range.start + max_accounts,  id_range.stop]))

					message(
						f'fetching accounts for {region}: start={id_range.start}, stop={id_range.stop}')
					id_creators.append(create_task(account_idQ_maker(
						idQs[region], id_range.start, id_range.stop)))
				else:
					ids: list[int] = list()
					await idQs[region].add_producer()
					async for account in BSAccount.import_file(args.file):
						ids.append(account.id)
						if len(ids) == 100:
							await idQs[region].put(ids)
							ids = list()
					if len(ids) > 0:
						await idQs[region].put(ids)
					await idQs[region].finish()

			except Exception as err:
				error(f"could not create account queue for '{region}': {err}")
				raise Exception()
		
		with alive_bar(None, title='Getting accounts from WG API ') as bar:
			try:
				prev 	: int = 0
				done	: int
				while not accountQ.is_filled:
					done = accountQ.count
					if done - prev > 0:
						bar(done - prev)
					prev = done
					await sleep(1)						

			except KeyboardInterrupt:
				message('cancelled')
				for idQ in idQs.values():
					await idQ.shutdown()		
		await stats.gather_stats(id_creators)
		for region in idQs.keys():
			debug(f'waiting for idQ for {region} to complete')
			await idQs[region].join()
		
	except Exception as err:
		error(f'{err}')
	finally:
		await stats.gather_stats(api_workers)
		wg.print()
		await wg.close()
	return stats


async def account_idQ_maker(idQ : IterableQueue[Sequence[int]], 
				 start: int, 
				 end: int, 
				 batch: int = 100) -> EventCounter:
	"""Create account_id queue"""
	debug('starting')
	stats	: EventCounter = EventCounter(f'account_ids')
	last 	: int = start
	await idQ.add_producer()
	try:
		for i in range(start, end, batch):
			await idQ.put(range(i, i + batch))
			last = i + batch		
	except QueueDone:
		debug('queue done')
	except (KeyboardInterrupt, CancelledError):
		debug('Cancelled')
		pass
	except Exception as err:
		error(f'{err}')
	stats.log('queued', last - start)
	debug(f'closing idQ')
	await idQ.finish()
	return stats


async def fetch_account_info_worker(wg		: WGApi, 
									region	: Region, 
									idQ		: IterableQueue[Sequence[int]], 
									accountQ: IterableQueue[BSAccount], 
									force 	: bool = False, 
									null_responses : int = 100) -> EventCounter:
	"""Fetch account info from WG API accounts/info"""
	debug('starting')
	stats	: EventCounter = EventCounter(f'{region}')
	left 	: int 	= null_responses
	infos 	: list[WGAccountInfo] | None
	ids		: Sequence[int]

	try:
		await accountQ.add_producer()
		while True:
			valid_stats : bool = False
			ids = await idQ.get()
			N_ids 		: int = len(ids)			
			try:				
				stats.log('account_ids', N_ids)
				if N_ids == 0 or N_ids > 100:
					raise ValueError(f'Incorrect number of account_ids give {N_ids}')
				
				if (infos:= await wg.get_account_info(ids, region)) is not None:					
					stats.log('stats found', len(infos))
					
					for info in infos:
						if (acc := BSAccount.transform(info)) is not None:
							await accountQ.put(acc)
							#stats.log('stats valid')
							valid_stats = True
						else:
							stats.log('format errors')
				else:
					stats.log('query errors')
				
			except ValueError:				
				stats.log('errors', N_ids)				
			except Exception as err:
				stats.log('errors')
				error(f'{err}')	
			finally:
				# debug(f'accounts={len(accounts)}, left={left}')
				idQ.task_done()
				if not force:
					left = null_responses if valid_stats else left - 1					
					if left <= 0:	# too many NULL responses, stop					
						break			
		
	except QueueDone:
		debug('account_id queue is done')
	except CancelledError:
		debug('cancelled')
	except Exception as err:
		error(f'{err}')
	finally:
		debug(f'closing accountQ: {region}')
		await accountQ.finish()
		debug(f'closing idQ: {region}')
		await idQ.shutdown()		
	return stats


# async def accountQ_filter(limit : int,
# 						  inQ: Queue[list[BSAccount] | None], 
# 						  accountQ : Queue[list[BSAccount]]) -> EventCounter:
# 	"""Read inputQ and stop after 'limit' consequtive None  has been counted"""
# 	debug('starting')
# 	stats : EventCounter = EventCounter('account Q filter')
# 	try:
# 		accounts : list[BSAccount] | None
# 		left : int = limit
# 		while True:
# 			accounts = await inQ.get()
# 			if accounts is None:
# 				left -= 1
# 				stats.log('no stats')
# 			else:
# 				left = limit
# 				await accountQ.put(accounts)
# 				stats.log('stats', len(accounts))
# 			if left == 0:
# 				debug(f'limit reached: {limit}')
# 				break
# 	except Exception as err:
# 		error(f'{err}')	
# 	return stats


async def cmd_fetch_wi(db: Backend, 
						args : Namespace, 
						accountQ : IterableQueue[BSAccount] | None
						) -> EventCounter:
	"""Fetch account_ids from replays.wotinspector.com replays"""
	debug('starting')
	stats		: EventCounter = EventCounter('WoTinspector')	
	workersN	: int 	= args.wi_workers
	workers		: list[Task] = list()
	max_pages	: int	= args.wi_max_pages
	start_page 	: int 	= args.wi_start_page
	rate_limit 	: float	= args.wi_rate_limit
	# force 		: bool  = args.force
	token		: str 	= args.wi_auth_token	# temp fix...
	replay_idQ  : Queue[str] = Queue()
	# pageQ		: Queue[int] = Queue()
	wi 			: WoTinspector 	= WoTinspector(rate_limit=rate_limit, auth_token=token)	
	if accountQ is not None:
		await accountQ.add_producer()
	try:					
		step : int = 1
		if max_pages < 0:
			step = -1
		elif max_pages == 0:
			step = -1
			max_pages = - start_page
		
		pages : range = range(start_page,(start_page + max_pages), step)

		stats.merge_child(await fetch_wi_get_replay_ids(db, wi, args, replay_idQ, pages))

		replays 	: int = replay_idQ.qsize()
		replays_left: int = replays
		with alive_bar(replays, title="Fetching replays ", manual=True, enrich_print=False) as bar:
			for _ in range(workersN):
				workers.append(create_task(fetch_wi_fetch_replays(db, wi, replay_idQ, accountQ)))
			while True:
				await sleep(1)
				replays_left = replay_idQ.qsize()
				bar(1-replays_left/replays)
				if replays_left == 0:
					break
		
		await replay_idQ.join()

		for worker in await gather(*workers, return_exceptions=True):
			stats.merge_child(worker)

	except Exception as err:
		error(f'{err}')
	finally:
		if accountQ is not None:
			await accountQ.finish()
		await wi.close()
	return stats


# async def update_wi_spider_replays(db: Backend, wi: WoTinspector, args: Namespace,
# 											replay_idQ: Queue[str], pages: range) -> EventCounter:
# 	"""Spider replays.WoTinspector.com and feed found replay IDs into replayQ. Return stats"""
# 	debug('starting')
# 	stats			: EventCounter = EventCounter('Crawler')
# 	max_old_replays	: int 	= args.wi_max_old_replays
# 	force			: bool 	= args.force
# 	old_replays		: int 	= 0

# 	try:
# 		debug(f'Starting ({len(pages)} pages)')
# 		with alive_bar(len(pages), title= "Spidering replays", enrich_print=False) as bar:
# 			for page in pages:			
# 				try:
# 					if old_replays > max_old_replays:						
# 						raise CancelledError
# 						#  break
# 					debug(f'spidering page {page}')
# 					url: str = wi.get_url_replay_listing(page)
# 					resp: str | None = await get_url(wi.session, url)
# 					if resp is None:
# 						error('could not spider replays.WoTinspector.com page {page}')
# 						stats.log('errors')
# 						continue
# 					debug(f'HTTP request OK')
# 					replay_ids: set[str] = wi.parse_replay_ids(resp)
# 					debug(f'Page {page}: {len(replay_ids)} found')
# 					if len(replay_ids) == 0:
# 						break
# 					for replay_id in replay_ids:
# 						res: WoTBlitzReplayData | None = await db.replay_get(replay_id=replay_id)
# 						if res is not None:
# 							debug(f'Replay already in the {db.backend}: {replay_id}')
# 							stats.log('old replays found')
# 							if not force:
# 								old_replays += 1
# 							continue
# 						else:
# 							await replay_idQ.put(replay_id)
# 							stats.log('new replays')
# 				except Exception as err:
# 					error(f'{err}')
# 				finally:
# 					bar()
# 	except CancelledError as err:
# 		# debug(f'Cancelled')
# 		message(f'{max_old_replays} found. Stopping spidering for more')
# 	except Exception as err:
# 		error(f'{err}')
# 	return stats


async def fetch_wi_get_replay_ids(db: Backend, wi: WoTinspector, args: Namespace,
									replay_idQ: Queue[str], pages: range) -> EventCounter:
	"""Spider replays.WoTinspector.com and feed found replay IDs into replayQ. Return stats"""
	debug('starting')
	stats			: EventCounter = EventCounter('Crawler')
	max_old_replays	: int 	= args.wi_max_old_replays
	force			: bool 	= args.force
	old_replays		: int 	= 0

	try:
		debug(f'Starting ({len(pages)} pages)')
		with alive_bar(len(pages), title= "Spidering replays", enrich_print=False) as bar:
			for page in pages:			
				try:
					if old_replays > max_old_replays:						
						raise CancelledError
						#  break
					debug(f'spidering page {page}')
					
					for replay_id in await wi.get_replay_ids(page):
						res: WoTBlitzReplayData | None = await db.replay_get(replay_id=replay_id)
						if res is not None:
							debug(f'Replay already in the {db.backend}: {replay_id}')
							stats.log('old replays found')
							if not force:
								old_replays += 1
							continue
						else:
							await replay_idQ.put(replay_id)
							stats.log('new replays')
				except KeyboardInterrupt:
					debug('CTRL+C pressed, stopping...')
					raise CancelledError
				except Exception as err:
					error(f'{err}')
				finally:
					bar()
	except CancelledError as err:
		# debug(f'Cancelled')
		message(f'{max_old_replays} found. Stopping spidering for more')
	except Exception as err:
		error(f'{err}')
	return stats


async def fetch_wi_fetch_replays(db			: Backend, 
								  wi			: WoTinspector, 
								  replay_idQ 	: Queue[str], 
								  accountQ 		: Queue[BSAccount] | None, 

								 ) -> EventCounter:
	debug('starting')
	stats : EventCounter = EventCounter('Fetch replays')
	try:
		while not replay_idQ.empty():
			replay_id = await replay_idQ.get()
			try:
				url : str = wi.get_url_replay_JSON(replay_id)
				replay : WoTBlitzReplayJSON | None = await get_url_JSON_model(wi.session, url, WoTBlitzReplayJSON )
				if replay is None:
					verbose(f'Could not fetch replay id: {replay_id}')
					continue
				if accountQ is not None:
					account_ids : list[int] = replay.get_players()
					stats.log('players found', len(account_ids))
					for account_id in account_ids:
						await accountQ.put(BSAccount(id=account_id))						
				if await db.replay_insert(replay):
					stats.log('replays added')
				else:
					stats.log('replays not added')
			finally:
				replay_idQ.task_done()
	except Exception as err:
		error(f'{err}')	
	return stats


# async def cmd_fetch_ys(db: Backend, 
# 						args : Namespace, 
# 						accountQ : IterableQueue[BSAccount]) -> EventCounter:
# 	"""Fetch account_ids fromy yastati.st"""
# 	debug('starting')
# 	stats		: EventCounter = EventCounter('Yastati.st')
# 	try:
# 		since 			: int = args.ys_days_since
# 		client_id 		: str = args.ys_client_id
# 		client_secret 	: str = args.ys_client_secret
		
# 		await accountQ.add_producer()
# 		with alive_bar(None, title='Getting accounts from yastati.st ') as bar:
# 			for region in [Region.eu, Region.ru]:
# 				async for account in get_accounts_since(region, days= since,
# 														client_id = client_id, 
# 														secret = client_secret):
# 					await accountQ.put(account)
# 					stats.log('accounts read')
# 					bar()

# 	except Exception as err:
# 		error(f'{err}')
# 	await accountQ.finish()
# 	return stats


async def cmd_import(db: Backend, args : Namespace) -> bool:
	"""Import accounts from other backend"""	
	try:
		stats 			: EventCounter 		= EventCounter('accounts import')
		accountQ 		: Queue[BSAccount]	= Queue(ACCOUNTS_Q_MAX)
		regions 		: set[Region]		= { Region(r) for r in args.regions }
		import_db   	: Backend | None 	= None		
		import_backend 	: str 				= args.import_backend
		force 			: bool 				= args.force
		if args.force:
			force = True
		
		write_worker : Task = create_task(db.accounts_insert_worker(accountQ=accountQ, 
																	force=force))

		if (import_db := Backend.create_import_backend(driver=import_backend, 
														args=args, 
														import_type=BSTableType.Accounts, 
														copy_from=db,
														config_file=args.import_config)) is None:
			raise ValueError(f'Could not init {import_backend} to import accounts from')

		message('Counting accounts to import ...')
		N : int = await db.accounts_count(regions=regions,
										inactive=OptAccountsInactive.both,
										sample=args.sample)

		with alive_bar(N, title="Importing accounts ", enrich_print=False) as bar:
			async for account in import_db.accounts_export(sample=args.sample):
				await accountQ.put(account)
				bar()
				stats.log('read')

		await accountQ.join()
		write_worker.cancel()
		worker_res : tuple[EventCounter|BaseException] = await gather(write_worker,
																	return_exceptions=True)
		if type(worker_res[0]) is EventCounter:
			stats.merge_child(worker_res[0])
		elif type(worker_res[0]) is BaseException:
			error(f'account insert worker threw an exception: {worker_res[0]}')
		stats.print()
		return True
	except Exception as err:
		error(f'{err}')	
	return False


async def cmd_export(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')		
		
		## not implemented...
		# query_args : dict[str, str | int | float | bool ] = dict()
		stats 		: EventCounter 			= EventCounter('accounts export')
		disabled 	: bool 					= args.disabled
		inactive 	: OptAccountsInactive 	= OptAccountsInactive.default()
		regions		: set[Region] 			= { Region(r) for r in args.regions }
		distributed : OptAccountsDistributed 
		filename	: str					= args.filename
		force		: bool 					= args.force
		export_stdout : bool 				= filename == '-'
		sample 		: float = args.sample
		accountQs 	: dict[str, IterableQueue[BSAccount]]	 = dict()
		# regionQs 	: dict[Region, IterableQueue[BSAccount]] = dict()
		account_workers : list[Task] = list()
		export_workers 	: list[Task] = list()

		accounts_args : dict[str, Any] | None 
		if (accounts_args := await accounts_parse_args(db, args)) is None:
			raise ValueError(f'could not parse args: {args}')

		try: 
			inactive = OptAccountsInactive(args.inactive)
			if inactive == OptAccountsInactive.auto:		# auto mode requires specication of stats type
				inactive = OptAccountsInactive.no
		except ValueError as err:
			assert False, f"Incorrect value for argument 'inactive': {args.inactive}"
		
		total : int = await db.accounts_count(**accounts_args)

		if 'dist' in accounts_args:
			distributed = accounts_args['dist']
			i : int = distributed.mod			
			Qid : str = str(i)
			accountQs[Qid] = IterableQueue(maxsize=ACCOUNTS_Q_MAX)
			await accountQs[Qid].add_producer()
			account_workers.append(create_task(db.accounts_get_worker(accountQs[Qid], 																		
																		**accounts_args)))
			export_workers.append(create_task(export(Q=cast(Queue[CSVExportable] | Queue[TXTExportable] | Queue[JSONExportable], 
														accountQs[Qid]), 
													format=args.format, 
													filename=f'{filename}.{i}', 
													force=force, 
													append=args.append)))
		elif args.by_region:
			accountQs['all'] = IterableQueue(maxsize=ACCOUNTS_Q_MAX, count_items=False)
			
			# fetch accounts for all the regios
			await accountQs['all'].add_producer()
			account_workers.append(create_task(db.accounts_get_worker(accountQs['all'], 
																	 **accounts_args)))
			# by region
			for region in regions:
				accountQs[region.name] = IterableQueue(maxsize=ACCOUNTS_Q_MAX)
				
				await accountQs[region.name].add_producer()										
				export_workers.append(create_task(export(Q=cast(Queue[CSVExportable] | Queue[TXTExportable] | Queue[JSONExportable], 
														accountQs[region.name]), 
														format=args.format, 
														filename=f'{filename}.{region.name}', 
														force=force, append=args.append)))			
			# split by region
			export_workers.append(create_task(split_accountQ(inQ=accountQs['all'], 
																regionQs=accountQs)))
		else:
			accountQs['all'] = IterableQueue(maxsize=ACCOUNTS_Q_MAX)
			await accountQs['all'].add_producer()
			account_workers.append(create_task(db.accounts_get_worker(accountQs['all'], 
																		**accounts_args)))

			if filename != '-':
				filename += '.all'
			export_workers.append(create_task(export(Q=cast(Queue[CSVExportable] | Queue[TXTExportable] | Queue[JSONExportable], accountQs['all']), 
											format=args.format, filename=filename, 
											force=force, append=args.append)))
		
		bar : Task | None = None
		if not export_stdout:
			bar = create_task(alive_bar_monitor(list(accountQs.values()), 'Exporting accounts', total=total, enrich_print=False))
			
		await wait(account_workers)

		for queue in accountQs.values():
			await queue.finish()
			await queue.join() 
		if bar is not None:
			bar.cancel()
		
		await stats.gather_stats(account_workers)
		await stats.gather_stats(export_workers)
		
		if not export_stdout:
			stats.print()

	except Exception as err:
		error(f'{err}')
	return False


async def cmd_remove(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		raise NotImplementedError

	except Exception as err:
		error(f'{err}')
	return False


async def count_accounts(db: Backend, args : Namespace, stats_type: StatsTypes | None) -> int:
	"""Helper to count accounts based on CLI args"""
	debug('starting')
	accounts_N : int = 0
	try:
		regions	 	: set[Region]	= { Region(r) for r in args.regions }
		accounts 	: list[BSAccount] | None = read_args_accounts(args.accounts)

		if accounts is not None:
			accounts_N = len(accounts)
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
				inactive : OptAccountsInactive = OptAccountsInactive.default()
				try: 
					inactive = OptAccountsInactive(args.inactive)
				except ValueError as err:
					assert False, f"Incorrect value for argument 'inactive': {args.inactive}"

				accounts_N = await db.accounts_count(stats_type=stats_type, regions=regions, 
													inactive=inactive, sample=args.sample, 
													cache_valid=args.cache_valid)
	except Exception as err:
		error(f'{err}')
	return accounts_N


###########################################
# 
# create_accountQ()  
#
###########################################


async def create_accountQ(db		: Backend, 
						  args 		: Namespace, 
						  accountQ	: IterableQueue[BSAccount],						  
						  stats_type: StatsTypes | None = None, 
						  ) -> EventCounter:
	"""Helper to make accountQ from arguments"""	
	stats : EventCounter = EventCounter(f'{db.driver}: accounts')
	debug('starting')
	try:
		accounts 	: list[BSAccount] | None = None
		try:
			accounts = read_args_accounts(args.accounts)
		except:
			debug('could not read --accounts')

		await accountQ.add_producer()

		if accounts is not None:
			
			for account in accounts:
				try:
					await accountQ.put(account)
					stats.log('read')
				except Exception as err:
					error(f'Could not add account ({account.id}) to queue')
					stats.log('errors')

		elif args.file is not None:
			async for account in BSAccount.import_file(args.file):
				await accountQ.put(account)
				debug(f'account put to queue: id={account.id}')
				stats.log('read')		
		
		else:			
			accounts_args : dict[str, Any] | None
			if (accounts_args := await accounts_parse_args(db, args)) is not None:
				async for account in db.accounts_get(stats_type=stats_type, 
													**accounts_args):
					try:
						await accountQ.put(account)
						stats.log('read')
					except Exception as err:
						error(f'Could not add account ({account.id}) to queue')
						stats.log('errors')
			else:
				error(f'could not parse args: {args}')		
	except CancelledError as err:
		debug(f'Cancelled')
	except Exception as err:
		error(f'{err}')		
	finally:
		await accountQ.finish()	
	debug('finished')
	return stats


async def create_accountQ_batch(db			: Backend, 
								args 		: Namespace, 
								region 		: Region,
								accountQ	: IterableQueue[list[BSAccount]], 								
								stats_type	: StatsTypes | None = None, 
								batch 		: int = 100) -> EventCounter:
	"""Helper to make accountQ from arguments"""	
	stats : EventCounter = EventCounter(f'{db.driver}: accounts')
	# assert len(args.regions) == 1, "account batch queue supports only a single region"
	debug(f'starting: {region}')
	##  region : Region = [ Region(r) for r in args.regions ][0]
	try:
		accounts 	: list[BSAccount] | None = None
		try:
			accounts = read_args_accounts(args.accounts)
		except:
			debug('could not read --accounts')

		await accountQ.add_producer()

		if accounts is not None:

			accounts = [ account for account in accounts if account.region == region ]
			for account_batch in chunker(accounts, batch):
				try:
					await accountQ.put(account_batch)
					stats.log('read', len(account_batch))
				except Exception as err:
					error(f'{err}')
					stats.log('errors', len(accounts))

		elif args.file is not None:
			accounts = list()			
			async for account in BSAccount.import_file(args.file):
				try:
					if account.region == region:
						accounts.append(account)
						if len(accounts) == batch:
							await accountQ.put(accounts)
							stats.log('read', batch)
							accounts = list()
				except Exception as err:
					error(f'Could not add account to the queue: {err}')
					stats.log('errors')				
		
			if len(accounts) > 0:
				await accountQ.put(accounts)
				stats.log('read', len(accounts))
		else:
			# message('counting accounts...')
			# start = time()
			# total : int = await db.accounts_count(stats_type=stats_type, 
			# 										regions=regions, 
			# 										inactive=inactive,
			# 										disabled=disabled,
			# 										active_since=active_since,
			# 										inactive_since=inactive_since,
			# 										sample=sample, 
			# 										cache_valid=cache_valid)
			# end = time()
			# message(f'{total} accounts, counting took {end - start}')

			accounts_args : dict[str, Any] | None
			if (accounts_args := await accounts_parse_args(db, args)) is not None:
				accounts_args['regions'] = {region}
				async for accounts in db.accounts_get_batch(stats_type=stats_type, 
															batch=batch, 
															**accounts_args):
					try:
						await accountQ.put(accounts)
						stats.log('read', len(accounts))
					except Exception as err:
						error(f'Could not add accounts to queue: {err}')
						stats.log('errors')
			else:
				error(f'could not parse args: {args}')		
	except CancelledError as err:
		debug(f'Cancelled')
	except Exception as err:
		error(f'{err}')		
	finally:
		await accountQ.finish()	
		
	return stats


async def create_accountQ_active(db: Backend, 
								accountQ: Queue[BSAccount], 
								release: BSBlitzRelease, 
								regions: set[Region], 
								randomize: bool = True) -> EventCounter:
	"""Add accounts active during a release to accountQ"""
	debug('starting')
	stats : EventCounter = EventCounter(f'accounts')
	try:
		if randomize:
			workers : list[Task] = list()
			for r in regions:
				workers.append(create_task(create_accountQ_active(db, accountQ, 
						      										release, 
																	regions={r}, 
																	randomize=False)))
			await stats.gather_stats(workers, merge_child=False, cancel=False)
		else:
			async for account_id in db.tank_stats_unique('account_id', int, 
														release=release, 
														regions=regions):
				try:
					await accountQ.put(BSAccount(id=account_id))
					stats.log('added')
				except Exception as err:
					error(f'{err}')
					stats.log('errors')
	except Exception as err:
		error(f'{err}')
	return stats


async def split_accountQ(inQ : IterableQueue[BSAccount], 
						 regionQs : dict[str, IterableQueue[BSAccount]]) -> EventCounter:
	"""split accountQ by region"""
	debug('starting')
	stats : EventCounter = EventCounter('accounts')
	try:		
		for Q in regionQs.values():
			await Q.add_producer()

		while True:
			account = await inQ.get()
			try:
				if account.region is None:
					raise ValueError(f'account ({account.id}) does not have region defined')
				if account.region.name in regionQs.keys(): 
					await regionQs[account.region.name].put(account)
					stats.log(account.region.name)
				else:
					stats.log(f'excluded region: {account.region}')
			except CancelledError:
				raise CancelledError from None
			except Exception as err:
				stats.log('errors')
				error(f'{err}')
			finally:
				stats.log('total')
				inQ.task_done()
	except QueueDone:
		debug('Marking regionQs finished')		
	except CancelledError as err:
		debug(f'Cancelled')
	except Exception as err:
		error(f'{err}')
	for Q in regionQs.values():
		await Q.finish()
	return stats


async def split_accountQ_batch(inQ			: IterableQueue[BSAccount],
								regionQs  	: dict[str, IterableQueue[list[BSAccount]]],						
								batch 		: int = 100) -> EventCounter:
	"""Make accountQ batches by region"""
	stats 	: EventCounter = EventCounter('batch maker')
	batches : dict[str, list[BSAccount]] = dict()
	region 	: str
	try:		
		for region, Q in regionQs.items():
			batches[region] = list()
			await Q.add_producer()

		while True:
			account = await inQ.get()
			try:
				region = account.region				
				if region in regionQs.keys(): 
					batches[region].append(account)					
					if len(batches[region]) == batch:
						await regionQs[region].put(batches[region])
						stats.log(f'{region} accounts', len(batches[region]))
						batches[region] = list()
				else:
					stats.log(f'excluded region: {region}')
			except CancelledError:
				raise CancelledError from None
			except Exception as err:
				stats.log('errors')
				error(f'{err}')
			finally:
				stats.log('total')
				inQ.task_done()
	except QueueDone:
		debug('inQ done')
		for region in batches.keys():
			if len(batches[region]) > 0:
				await regionQs[region].put(batches[region])
				stats.log(f'{region} accounts', len(batches[region]))	
	except CancelledError as err:
		debug(f'Cancelled')
	except Exception as err:
		error(f'{err}')
	for Q in regionQs.values():
		await Q.finish()
	return stats


def read_args_accounts(accounts: list[str]) -> list[BSAccount] | None:
	res : list[BSAccount] = list()
	for a in accounts:
		try:
			if (acc := BSAccount.from_str(a) ) is not None:
				res.append(acc)
		except Exception as err:
			error(f'{err}')
	if len(res) == 0:
		return None
	return res


async def accounts_parse_args(db: Backend, 
			      				args : Namespace,
							 ) -> dict[str, Any] | None:
	"""parse accounts args"""
	debug('starting')
	res : dict[str, Any] = dict()
	
	try:
		try:
			res['regions']	= { Region(r) for r in args.regions }		
		except:
			debug('could not read --regions')
	
		# try:
		# 	res['accounts'] = read_args_accounts(args.accounts)
		# except:
		# 	debug('could not read --accounts')
	
		try:
			res['inactive'] = OptAccountsInactive(args.inactive)
		except:
			debug('could not read --inactive')

		try:
			res['disabled'] = args.disabled
		except:
			debug('could not read --disabled')
		
		try:
			res['sample'] = args.sample
		except:
			debug('could not read --sample')

		try:
			res['cache_valid'] = args.cache_valid
		except:
			debug('could not read --cache-valid')

		try:
			if (dist := OptAccountsDistributed.parse(args.distributed)) is not None:
				res['dist'] = dist
		except:
			debug('could not read --distributed')
		
		days : int
		today : datetime = datetime.today()
		start : datetime
		try:
			if ( rel := await db.release_get(args.inactive_since)) is not None:
				if (prev := await db.release_get_previous(rel)) is not None:
					res['inactive_since'] = prev.cut_off				
			else:
				days = int(args.inactive_since)
				start = today - timedelta(days=days)
				res['inactive_since'] = int(start.timestamp())
		except Exception as err:
			debug(f'could not read --inactive-since: {err}')
		
		try:
			if ( rel := await db.release_get(args.active_since)) is not None:
				if (prev := await db.release_get_previous(rel)) is not None:
					res['active_since'] = prev.cut_off
			else:
				days = int(args.active_since)
				start = today - timedelta(days=days)
				res['active_since'] = int(start.timestamp())
		except Exception as err:
			debug(f'could not read --active-since: {err}')
		
		return res
	except Exception as err:
		error(f'{err}')
	return None