from argparse import ArgumentParser, Namespace, SUPPRESS
from configparser import ConfigParser
from datetime import datetime
from typing import Optional, Literal, Any, cast
import logging
from asyncio import run, create_task, gather, sleep, wait, Queue, CancelledError, Task

import copy
from os import getpid, makedirs
from aiofiles import open
from os.path import isfile, dirname
import os.path
from math import ceil
# from asyncstdlib import enumerate
from alive_progress import alive_bar		# type: ignore
#from yappi import profile 					# type: ignore

# multiprocessing
from multiprocessing import Manager, cpu_count
from multiprocessing.pool import Pool, AsyncResult 
import queue

# export data
import pandas as pd  						# type: ignore
import pyarrow as pa						# type: ignore
import pyarrow.dataset as ds				# type: ignore
import pyarrow.parquet as pq				# type: ignore
from pandas.io.json import json_normalize	# type: ignore

from backend import Backend, OptAccountsInactive, BSTableType, \
					ACCOUNTS_Q_MAX, MIN_UPDATE_INTERVAL, get_sub_type
from models import BSAccount, BSBlitzRelease, StatsTypes
from accounts import create_accountQ, read_args_accounts, create_accountQ_active, \
					accounts_parse_args
from releases import get_releases, release_mapper

from pyutils import alive_bar_monitor, \
					is_alphanum, JSONExportable, TXTExportable, CSVExportable, export, \
					BucketMapper, IterableQueue, QueueDone, EventCounter, AsyncQueue, CounterQueue
from blitzutils.models import Region, WGTankStat, WGTankStatAll, Tank, \
								EnumVehicleTier, EnumNation, \
								EnumVehicleTypeInt
from blitzutils.wg import WGApi 

logger 	= logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug

# Constants

WORKERS_WGAPI 		: int = 75
WORKERS_IMPORTERS	: int = 5
WORKERS_PRUNE 		: int = 10
WORKERS_EDIT 		: int = 10
TANK_STATS_Q_MAX 	: int = 1000
TANK_STATS_BATCH 	: int = 50000

EXPORT_WRITE_BATCH 			: int = int(50e6)
EXPORT_DATA_FORMATS 		: list[str] = [ 'parquet', 'arrow' ]
DEFAULT_EXPORT_DATA_FORMAT 	: str 		= EXPORT_DATA_FORMATS[0]

# Globals

# export_total_rows : int = 0
db 			: Backend
mp_wg 		: WGApi
readQ 		: AsyncQueue[list[Any] | None]
progressQ   : AsyncQueue[int | None]
workQ_t   	: AsyncQueue[int | None]
workQ_a     : AsyncQueue[BSAccount | None]
tank_statQ  : AsyncQueue[list[WGTankStat]]
writeQ 		: AsyncQueue[pd.DataFrame]
in_model	: type[JSONExportable]
mp_options	: dict[str, Any] = dict()
mp_args 	: Namespace

########################################################
# 
# add_args_ functions  
#
########################################################

def add_args(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')		
		tank_stats_parsers = parser.add_subparsers(dest='tank_stats_cmd', 	
												title='tank-stats commands',
												description='valid commands',
												metavar='fetch | prune | edit | import | export')
		tank_stats_parsers.required = True
		
		fetch_parser = tank_stats_parsers.add_parser('fetch', aliases=['get'], help="tank-stats fetch help")
		if not add_args_fetch(fetch_parser, config=config):
			raise Exception("Failed to define argument parser for: tank-stats fetch")

		edit_parser = tank_stats_parsers.add_parser('edit', help="tank-stats edit help")
		if not add_args_edit(edit_parser, config=config):
			raise Exception("Failed to define argument parser for: tank-stats edit")
		
		prune_parser = tank_stats_parsers.add_parser('prune', help="tank-stats prune help")
		if not add_args_prune(prune_parser, config=config):
			raise Exception("Failed to define argument parser for: tank-stats prune")

		import_parser = tank_stats_parsers.add_parser('import', help="tank-stats import help")
		if not add_args_import(import_parser, config=config):
			raise Exception("Failed to define argument parser for: tank-stats import")

		export_parser = tank_stats_parsers.add_parser('export', help="tank-stats export help")
		if not add_args_export(export_parser, config=config):
			raise Exception("Failed to define argument parser for: tank-stats export")
		
		export_data_parser = tank_stats_parsers.add_parser('export-data', help="tank-stats export-data help")
		if not add_args_export_data(export_data_parser, config=config):
			raise Exception("Failed to define argument parser for: tank-stats export-data")

		debug('Finished')	
		return True
	except Exception as err:
		error(f'{err}')
	return False


def add_args_fetch(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		WG_RATE_LIMIT 	: float = 10
		WG_WORKERS 	: int 	= 10
		WG_APP_ID		: str 	= WGApi.DEFAULT_WG_APP_ID
		
		# Lesta / RU
		LESTA_RATE_LIMIT: float = 10
		LESTA_WORKERS 	: int 	= 10
		LESTA_APP_ID 	: str 	= WGApi.DEFAULT_LESTA_APP_ID
		
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

		parser.add_argument('--workers', type=int, dest='wg_workers', default=WG_WORKERS, 
							help='Set number of asynchronous workers')
		parser.add_argument('--wg-app-id', type=str, default=WG_APP_ID, metavar='APP_ID',
							help='Set WG APP ID')
		parser.add_argument('--wg-rate-limit', type=float, default=WG_RATE_LIMIT, 
		      				metavar='RATE_LIMIT', help='rate limit for WG API per server')
		parser.add_argument('--ru-app-id', type=str, default=LESTA_APP_ID, metavar='APP_ID',
							help='Set Lesta (RU) APP ID')
		parser.add_argument('--ru-rate-limit', type=float, default=LESTA_RATE_LIMIT, 
		      				metavar='RATE_LIMIT', help='Rate limit for Lesta (RU) API')
		parser.add_argument('--regions', '--region', type=str, nargs='*', choices=[ r.value for r in Region.API_regions() ], 
							default=[ r.value for r in Region.API_regions() ], 
							help='Filter by region (default: eu + com + asia + ru)')		
		parser.add_argument('--inactive', type=str, choices=[ o.value for o in OptAccountsInactive ], 
								default=OptAccountsInactive.both.value, help='Include inactive accounts')
		parser.add_argument('--active-since', type=str, default=None, metavar='RELEASE/DAYS',
							help='Fetch stats for accounts that have been active since RELEASE/DAYS')
		parser.add_argument('--inactive-since', type=str,  default=None, metavar='RELEASE/DAYS',
							help='Fetch stats for accounts that have been inactive since RELEASE/DAYS')		
		parser.add_argument('--cache-valid', type=float, default=1, metavar='DAYS',
							help='Fetch stats only for accounts with stats older than DAYS')		
		parser.add_argument('--distributed', '--dist',type=str, dest='distributed', metavar='I:N', 
							default=None, help='Distributed fetching for accounts: id %% N == I')
		parser.add_argument('--check-disabled',  dest='disabled', action='store_true', default=False, 
							help='Check disabled accounts')
		parser.add_argument('--accounts', type=str, default=[], nargs='*', 
							metavar='ACCOUNT_ID [ACCOUNT_ID1 ...]',
							help="Fetch stats for the listed ACCOUNT_ID(s). \
									ACCOUNT_ID format 'account_id:region' or 'account_id'")
		# parser.add_argument('--mp', action='store_true', default=False, 
		# 					help='Multiprocess fetch')		
		parser.add_argument('--force', action='store_true', default=False, 
							help='Fetch stats for all accounts')
		parser.add_argument('--sample', type=float, default=0, metavar='SAMPLE',
							help='Fetch tank stats for SAMPLE of accounts. If 0 < SAMPLE < 1, SAMPLE defines a %% of users')
		parser.add_argument('--file',type=str, metavar='FILENAME', default=None, 
							help='Read account_ids from FILENAME one account_id per line')
		parser.add_argument('--last', action='store_true', default=False, help=SUPPRESS)

		return True
	except Exception as err:
		error(f'{err}')
	return False


def add_args_edit(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	debug('starting')
	try:
		edit_parsers = parser.add_subparsers(dest='tank_stats_edit_cmd', 	
												title='tank-stats edit commands',
												description='valid commands',
												metavar='remap-release')
		edit_parsers.required = True
		
		remap_parser = edit_parsers.add_parser('remap-release', help="tank-stats edit remap-release help")
		if not add_args_edit_remap_release(remap_parser, config=config):
			raise Exception("Failed to define argument parser for: tank-stats edit remap-release")
		
		missing_parser = edit_parsers.add_parser('fix-missing', help="tank-stats edit fix-missing help")
		if not add_args_edit_fix_missing(missing_parser, config=config):
			raise Exception("Failed to define argument parser for: tank-stats edit fix-missing")	

		return True
	except Exception as err:
		error(f'{err}')
	return False


def add_args_edit_common(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	"""Adds common arguments to edit subparser. Required for meaningful helps and usability"""
	debug('starting')
	parser.add_argument('--commit', action='store_true', default=False, 
							help='Do changes instead of just showing what would be changed')
	parser.add_argument('--sample', type=float, default=0, metavar='SAMPLE',
						help='Sample size. 0 < SAMPLE < 1 : %% of stats, 1<=SAMPLE : Absolute number')
	# filters
	parser.add_argument('--regions', '--region', type=str, nargs='*', 
							choices=[ r.value for r in Region.has_stats() ], 
							default=[ r.value for r in Region.has_stats() ], 
							help=f"Filter by region (default is API = {' + '.join([r.value for r in Region.API_regions()])})")
	parser.add_argument('--release', type=str, metavar='RELEASE', default=None, 
							help='Apply edits to a RELEASE')
	parser.add_argument('--since', type=str, metavar='DATE', default=None, nargs='?',
						help='Apply edits to releases launched after DATE (default is all releases)')
	parser.add_argument('--accounts', type=str, default=[], nargs='*', metavar='ACCOUNT_ID [ACCOUNT_ID1 ...]',
							help="Edit tank stats for the listed ACCOUNT_ID(s) ('account_id:region' or 'account_id')")
	parser.add_argument('--tank_ids', type=int, default=None, nargs='*', metavar='TANK_ID [TANK_ID1 ...]',
							help="Edit tank stats for the listed TANK_ID(S).")
	parser.add_argument('--workers', type=int, dest='workers', default=WORKERS_EDIT, 
							help='Set number of asynchronous workers')
	return True


def add_args_edit_remap_release(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	debug('starting')
	return add_args_edit_common(parser, config)


def add_args_edit_fix_missing(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	debug('starting')
	if not add_args_edit_common(parser, config):
		return False
	parser.add_argument('field', type=str, metavar='FIELD',  
						help='Fix stats with missing FIELD')
	return True


def add_args_prune(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	debug('starting')
	parser.add_argument('release', type=str, metavar='RELEASE',  
						help='prune tank stats for a RELEASE')
	parser.add_argument('--regions', '--region', type=str, nargs='*', choices=[ r.value for r in Region.API_regions() ], 
							default=[ r.value for r in Region.API_regions() ], 
							help='filter by region (default: eu + com + asia + ru)')
	parser.add_argument('--commit', action='store_true', default=False, 
							help='execute pruning stats instead of listing duplicates')
	parser.add_argument('--workers', type=int, default=WORKERS_PRUNE, 
							help=f'set number of worker processes (default={WORKERS_PRUNE})')
	parser.add_argument('--sample', type=int, default=0, metavar='SAMPLE',
						help='sample size')
	return True


def add_args_import(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	"""Add argument parser for tank-stats import"""
	try:
		debug('starting')
		
		import_parsers = parser.add_subparsers(dest='import_backend', 	
												title='tank-stats import backend',
												description='valid backends', 
												metavar=', '.join(Backend.list_available()))
		import_parsers.required = True

		for backend in Backend.get_registered():
			import_parser =  import_parsers.add_parser(backend.driver, help=f'tank-stats import {backend.driver} help')
			if not backend.add_args_import(import_parser, config=config):
				raise Exception(f'Failed to define argument parser for: tank-stats import {backend.driver}')
		parser.add_argument('--workers', type=int, default=0, 
							help='set number of worker processes (default=0 i.e. auto)')
		parser.add_argument('--import-model', metavar='IMPORT-TYPE', type=str, required=True,
							choices=['WGTankStat'], 
							help='data format to import. Default is blitz-stats native format.')
		parser.add_argument('--sample', type=float, default=0, 
							help='Sample size. 0 < SAMPLE < 1 : %% of stats, 1<=SAMPLE : Absolute number')
		parser.add_argument('--force', action='store_true', default=False, 
							help='overwrite existing stats when importing')
		# parser.add_argument('--mp', action='store_true', default=False, 
		# 					help='Use multi-processing')
		parser.add_argument('--no-release-map', action='store_true', default=False, 
							help='do not map releases when importing')
		# parser.add_argument('--last', action='store_true', default=False, help=SUPPRESS)

		return True
	except Exception as err:
		error(f'{err}')
	return False


def add_args_export(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		EXPORT_FORMAT 	= 'json'
		EXPORT_FILE 	= 'tank_stats'
		EXPORT_SUPPORTED_FORMATS : list[str] = ['json']    # , 'csv'

		if config is not None and 'TANK_STATS' in config.sections():
			configTS 	= config['TANK_STATS']
			EXPORT_FORMAT	= configTS.get('export_format', EXPORT_FORMAT)
			EXPORT_FILE		= configTS.get('export_file', EXPORT_FILE )

		parser.add_argument('format', type=str, nargs='?', choices=EXPORT_SUPPORTED_FORMATS, 
		 					 default=EXPORT_FORMAT, help='export file format')
		parser.add_argument('filename', metavar='FILE', type=str, nargs='?', default=EXPORT_FILE, 
							help='file to export tank-stats to. Use \'-\' for STDIN')
		parser.add_argument('--basedir', metavar='FILE', type=str, nargs='?', default='.', 
							help='base dir to export data')
		parser.add_argument('--append', action='store_true', default=False, help='Append to file(s)')
		parser.add_argument('--force', action='store_true', default=False, 
								help='overwrite existing file(s) when exporting')
		# parser.add_argument('--disabled', action='store_true', default=False, help='Disabled accounts')
		# parser.add_argument('--inactive', type=str, choices=[ o.value for o in OptAccountsInactive ], 
								# default=OptAccountsInactive.no.value, help='Include inactive accounts')
		parser.add_argument('--regions', '--region', type=str, nargs='*', choices=[ r.value for r in Region.API_regions() ], 
								default=[ r.value for r in Region.API_regions() ], 
								help='filter by region (default is API = eu + com + asia)')
		parser.add_argument('--by-region', action='store_true', default=False, help='Export tank-stats by region')
		parser.add_argument('--accounts', type=str, default=[], nargs='*', metavar='ACCOUNT_ID [ACCOUNT_ID1 ...]',
								help="exports tank stats for the listed ACCOUNT_ID(s). \
									ACCOUNT_ID format 'account_id:region' or 'account_id'")
		parser.add_argument('--tanks', type=int, default=[], nargs='*', metavar='TANK_ID [TANK_ID1 ...]',
								help="export tank stats for the listed TANK_ID(s)")
		parser.add_argument('--release', type=str, metavar='RELEASE', default=None, 
							help='export stats for a RELEASE')
		parser.add_argument('--sample', type=float, default=0, 
								help='sample size. 0 < SAMPLE < 1 : %% of stats, 1<=SAMPLE : Absolute number')
		return True	
	except Exception as err:
		error(f'{err}')
	return False

def add_args_export_data(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		EXPORT_FORMAT 	: str = DEFAULT_EXPORT_DATA_FORMAT
		EXPORT_FILE 	: str = 'update_totals'
		EXPORT_DIR 		: str = 'export' 
		
		if config is not None and 'TANK_STATS' in config.sections():
			configTS 		= config['TANK_STATS']
			EXPORT_FORMAT	= configTS.get('export_data_format', DEFAULT_EXPORT_DATA_FORMAT)
			EXPORT_FILE		= configTS.get('export_data_file', EXPORT_FILE )
			EXPORT_DIR		= configTS.get('export_data_file', EXPORT_DIR )
		parser.add_argument('EXPORT_TYPE', type=str, choices=['update', 'career'], 
							help="export latest stats or stats for the update")
		parser.add_argument('RELEASE', type=str, 
							help='export stats for a RELEASE')
		parser.add_argument('--after', action='store_true', default=False, 
								help='exports stats at the end of release, default is False')
		parser.add_argument('--format', type=str, nargs='?', choices=EXPORT_DATA_FORMATS, 
		 					 default=EXPORT_FORMAT, help='export file format')
		# parser.add_argument('--filename', metavar='FILE', type=str, nargs='?', default=None, 
		# 					help='file to export tank-stats to')
		parser.add_argument('--basedir', metavar='FILE', type=str, nargs='?', default=EXPORT_DIR, 
							help='base dir to export data')
		parser.add_argument('--regions', '--region', type=str, nargs='*', 
							choices=[ r.value for r in Region.API_regions() ], 
							default=[ r.value for r in Region.API_regions() ], 
							help='filter by region (default: ' + ' + '.join(Region.API_regions()) + ')')
		parser.add_argument('--workers', type=int, default=0, 
							help='set number of worker processes (default=0 i.e. auto)')
		parser.add_argument('--force', action='store_true', default=False, 
								help='overwrite existing file(s) when exporting')

		return True	
	except Exception as err:
		error(f'{err}')
	return False

###########################################
# 
# cmd_ functions  
#
###########################################

async def cmd(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		if args.tank_stats_cmd == 'fetch':
			if len(args.accounts) > 0:
				return await cmd_fetch(db, args)
			else:	
				return await cmd_fetchMP(db, args)

		elif args.tank_stats_cmd == 'edit':
			return await cmd_edit(db, args)

		elif args.tank_stats_cmd == 'export':
			return await cmd_export_text(db, args)
		
		elif args.tank_stats_cmd == 'export-data':
			if args.EXPORT_TYPE == 'update':
				return await cmd_export_update(db, args)
			elif args.EXPORT_TYPE == 'career':
				return await cmd_export_career(db, args)
			else:
				raise NotImplementedError(f'export type not implemented: {args.EXPORT_TYPE}')
		elif args.tank_stats_cmd == 'import':
			return await cmd_importMP(db, args)
		
		elif args.tank_stats_cmd == 'prune':
			return await cmd_prune(db, args)
			
	except Exception as err:
		error(f'{err}')
	return False

########################################################
# 
# cmd_fetchMP()
#
########################################################

async def cmd_fetchMP(db: Backend, args : Namespace) -> bool:
	"""fetch tank stats"""
	debug('starting')
	
	try:
		stats 	: EventCounter				= EventCounter('tank-stats fetch')	
		regions	: set[Region] 				= { Region(r) for r in args.regions }
		worker 	: Task

		with Manager() as manager:
			writeQ 	: queue.Queue[list[WGTankStat]]	= manager.Queue(TANK_STATS_Q_MAX)
			statsQ	: AsyncQueue[list[WGTankStat]]	= AsyncQueue.from_queue(writeQ)
			WORKERS : int = len(regions)

			with Pool(processes=WORKERS, initializer=fetch_mp_init, 
					  initargs=[ db.config, args, writeQ ]) as pool:
				
				accounts_args : dict[str, Any] | None
				if (accounts_args := await accounts_parse_args(db, args)) is None:
					raise ValueError(f'could not parse account args: {args}')
				
				worker = create_task(fetch_backend_worker(db, statsQ))
				accounts : int = await db.accounts_count(StatsTypes.tank_stats, 
												 		**accounts_args)
				debug(f'starting {WORKERS} workers')
				results : AsyncResult = pool.map_async(fetch_mp_worker_start, 
														regions)
				pool.close()

				done : int
				prev : int = 0
				with alive_bar(accounts, title='Fetching tank stats ') as bar:
					while not results.ready():
						done = statsQ.done
						if done -prev > 0:
							bar(done - prev)
						prev = done
						await sleep(1) 
				
				await statsQ.join()
				await stats.gather_stats([worker])
				for res in results.get():
					stats.merge_child(res)
				pool.join()
		
		message(stats.print(do_print=False, clean=True))
		return True
	except Exception as err:
		error(f'{err}')

	return False


def fetch_mp_init(backend_config: dict[str, Any],
				  args 			: Namespace, 
				  writeQ 		: queue.Queue[list[WGTankStat]], 
				  ):
	"""Initialize static/global backend into a forked process"""
	global db, tank_statQ, mp_args
	debug(f'starting (PID={getpid()})')

	if (tmp_db := Backend.create(**backend_config)) is None:
		raise ValueError('could not create backend')	
	db 			= tmp_db
	mp_args 	= args	
	tank_statQ 	= AsyncQueue.from_queue(writeQ)	
	debug('finished')


def fetch_mp_worker_start(region: Region) -> EventCounter:
	"""Forkable tank stats fetch worker for tank stats"""
	debug(f'starting fetch worker {region}')
	return run(fetch_mp_worker(region), debug=False)


async def  fetch_mp_worker(region: Region) -> EventCounter:
	"""Forkable tank stats import worker for latest (career) stats"""
	global db, tank_statQ, mp_args

	debug(f'fetch worker starting: {region}')
	stats 		: EventCounter 	= EventCounter(f'fetch {region}')
	accountQ	: IterableQueue[BSAccount] = IterableQueue(maxsize=100)
	retryQ 		: IterableQueue[BSAccount] | None = None
	THREADS 	: int 			= 20		
	args 		: Namespace 	= mp_args
	
	try:
		args.regions = { region }
		wg 	: WGApi = WGApi(app_id=args.wg_app_id, 
							ru_app_id= args.ru_app_id, 
							rate_limit=args.wg_rate_limit, 
							ru_rate_limit = args.ru_rate_limit,)

		if not args.disabled:
			retryQ = IterableQueue()	# must not use maxsize
		
		workers : list[Task] = list()
			
		for _ in range(THREADS):
			workers.append(create_task(fetch_api_worker(db, wg_api=wg, 
														accountQ=accountQ, 
														statsQ=tank_statQ, 
														retryQ=retryQ, 
														disabled=args.disabled)))	
		stats.merge(await create_accountQ(db, args, accountQ, 
											stats_type=StatsTypes.tank_stats))

		debug(f'waiting for account queue to finish: {region}')
		await accountQ.join()
		
		# Process retryQ
		if retryQ is not None and not retryQ.empty():
			retry_accounts : int = retryQ.qsize()
			debug(f'retryQ: size={retry_accounts} is_filled={retryQ.is_filled}')			
			for _ in range(THREADS):
				workers.append(create_task(fetch_api_worker(db, wg_api=wg,  
															accountQ=retryQ, 
															statsQ=tank_statQ)))
			await retryQ.join()

		await stats.gather_stats(workers)
		
	except Exception as err:
		error(f'{err}')
	finally:
		wg.print()
		await wg.close()
		
	return stats


########################################################
# 
# cmd_fetch()
#
########################################################


async def cmd_fetch(db: Backend, args : Namespace) -> bool:
	"""fetch tank stats"""
	debug('starting')
	wg 	: WGApi = WGApi(app_id=args.wg_app_id, 
						ru_app_id= args.ru_app_id, 
						rate_limit=args.wg_rate_limit, 
						ru_rate_limit = args.ru_rate_limit,)

	try:
		stats 	 : EventCounter				= EventCounter('tank-stats fetch')	
		regions	 : set[Region] 				= { Region(r) for r in args.regions }
		accountQs: dict[Region, IterableQueue[BSAccount]]	= dict()
		retryQ 	 : IterableQueue[BSAccount] | None = None
		statsQ	 : Queue[list[WGTankStat]]	= Queue(maxsize=TANK_STATS_Q_MAX)

		if not args.disabled:
			retryQ = IterableQueue()		# must not use maxsize
		
		workers : list[Task] = list()
		workers.append(create_task(fetch_backend_worker(db, statsQ)))

		# Process accountQ
		accounts : int 
		if len(args.accounts) > 0:
			accounts = len(args.accounts)
		else:
			accounts_args : dict[str, Any] | None
			if (accounts_args := await accounts_parse_args(db, args)) is None:
				raise ValueError(f'could not parse account args: {args}')

			accounts = await db.accounts_count(StatsTypes.tank_stats, 
												**accounts_args)
		
		WORKERS	: int 	= max( int(args.wg_workers / len(Region.API_regions())), 1)
		WORKERS = min( [ WORKERS, ceil(accounts/4) ])
		for r in regions:
			accountQs[r] = IterableQueue(maxsize=ACCOUNTS_Q_MAX)
			r_args = copy.copy(args)
			r_args.regions = { r }
			workers.append(create_task(create_accountQ(db, r_args, accountQs[r],
					      								stats_type=StatsTypes.tank_stats)))
			for _ in range(WORKERS):
				workers.append(create_task(fetch_api_worker(db, wg_api=wg, accountQ=accountQs[r], 
															statsQ=statsQ, retryQ=retryQ, 
															disabled=r_args.disabled)))

		task_bar : Task = create_task(alive_bar_monitor([Q for Q in accountQs.values() ], 
														total=accounts, 
														title="Fetching tank stats"))
		

		# stats.merge_child(await create_accountQ(db, args, accountQ, StatsTypes.tank_stats))
		# debug(f'AccountQ created. count={accountQ.count}, size={accountQ.qsize()}')
		for r, Q in accountQs.items():
			debug(f'waiting for account queue to finish: {r}')
			await accountQs[r].join()
		task_bar.cancel()

		# Process retryQ
		if retryQ is not None and not retryQ.empty():
			retry_accounts : int = retryQ.qsize()
			debug(f'retryQ: size={retry_accounts} is_filled={retryQ.is_filled}')
			task_bar = create_task(alive_bar_monitor([retryQ], total=retry_accounts, 
													  title="Retrying failed accounts"))
			for _ in range(min([args.wg_workers, ceil(retry_accounts/4)])):
				workers.append(create_task(fetch_api_worker(db, wg_api=wg,  
														accountQ=retryQ, 
														statsQ=statsQ)))
			await retryQ.join()
			task_bar.cancel()
		
		await statsQ.join()

		for task in workers:
			task.cancel()
		
		for ec in await gather(*workers, return_exceptions=True):
			if isinstance(ec, EventCounter):
				stats.merge_child(ec)
		message(stats.print(do_print=False, clean=True))
		return True
	except Exception as err:
		error(f'{err}')
	finally:
		wg.print()
		await wg.close()
	return False


async def fetch_api_worker(db: Backend, 
							wg_api : WGApi,										
							accountQ: IterableQueue[BSAccount], 
							statsQ	: Queue[list[WGTankStat]], 
							retryQ 	: IterableQueue[BSAccount] | None = None, 
							disabled : bool = False) -> EventCounter:
	"""Async worker to fetch tank stats from WG API"""
	debug('starting')
	stats 		: EventCounter
	tank_stats 	: list[WGTankStat] | None
	if retryQ is None:
		stats = EventCounter('re-try')
	else:
		stats = EventCounter('fetch')
		await retryQ.add_producer()
		
	try:
		while True:
			account : BSAccount = await accountQ.get()
			# if retryQ is None:
			# 	print(f'retryQ: account_id={account.id}')
			try:
				debug(f'account_id: {account.id}')
				stats.log('accounts total')

				if account.region is None:
					raise ValueError(f'account_id={account.id} does not have region set')
				
				if (tank_stats := await wg_api.get_tank_stats(account.id, account.region)) is None:
					
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
					await statsQ.put(tank_stats)
					stats.log('tank stats fetched', len(tank_stats))
					stats.log('accounts /w stats')
					if disabled:
						account.disabled = False
						await db.account_update(account=account, fields=['disabled'])
						stats.log('accounts enabled')

			except Exception as err:
				stats.log('errors')
				error(f'{err}')
			finally:
				accountQ.task_done()
	except QueueDone:
		debug('accountQ has been processed')
	except CancelledError as err:
		debug(f'Cancelled')	
	except Exception as err:
		error(f'{err}')
	finally:
		if retryQ is not None:
			await retryQ.finish()

	stats.log('accounts total', - stats.get_value('accounts to re-try'))  # remove re-tried accounts from total
	return stats


async def fetch_backend_worker(db: Backend, statsQ: Queue[list[WGTankStat]]) -> EventCounter:
	"""Async worker to add tank stats to backend. Assumes batch is for the same account"""
	debug('starting')
	stats 		: EventCounter = EventCounter(f'db: {db.driver}')
	added 		: int
	not_added 	: int
	account 	: BSAccount | None
	account_id	: int
	last_battle_time : int
	rel : BSBlitzRelease | None

	try:
		releases : BucketMapper[BSBlitzRelease] = await release_mapper(db)
		while True:
			added 			= 0
			not_added 		= 0
			last_battle_time = -1
			account			= None
			tank_stats : list[WGTankStat] = await statsQ.get()
			
			try:
				if len(tank_stats) > 0:
					debug(f'Read {len(tank_stats)} from queue')
					
					last_battle_time = max( [ ts.last_battle_time for ts in tank_stats] )
					for ts in tank_stats:						 
						if (rel := releases.get(ts.last_battle_time)) is not None:
							ts.release = rel.release

					added, not_added = await db.tank_stats_insert(tank_stats)	
					account_id = tank_stats[0].account_id
					if (account := await db.account_get(account_id=account_id)) is None:
						account = BSAccount(id=account_id)
					account.last_battle_time = last_battle_time
					account.stats_updated(StatsTypes.tank_stats)
					if added > 0:
						stats.log('accounts /w new stats')
						if account.inactive:
							stats.log('accounts marked active')
						account.inactive = False
					else:
						stats.log('accounts w/o new stats')
						# if account.is_inactive(): 
						# 	if not account.inactive:
						# 		stats.log('accounts marked inactive')
						# 	account.inactive = True
						
					await db.account_replace(account=account, upsert=True)
			except Exception as err:
				error(f'{err}')
			finally:
				stats.log('tank stats added', added)
				stats.log('old tank stats found', not_added)
				debug(f'{added} tank stats added, {not_added} old tank stats found')				
				statsQ.task_done()
	except CancelledError as err:
		debug(f'Cancelled')	
	except Exception as err:
		error(f'{err}')
	return stats


########################################################
# 
# cmd_edit()
#
########################################################

async def cmd_edit(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		release 	: BSBlitzRelease | None = None
		if args.release is not None:
			release = BSBlitzRelease(release=args.release)
		stats 		: EventCounter 			= EventCounter('tank-stats edit')
		regions		: set[Region] 			= { Region(r) for r in args.regions }
		since		: int = 0
		if args.since is not None:
			since = int(datetime.fromisoformat(args.since).timestamp())
		accounts 	: list[BSAccount] | None= read_args_accounts(args.accounts)
		sample 		: float 				= args.sample
		commit 		: bool  				= args.commit
		tank_statQ 	: IterableQueue[WGTankStat] = IterableQueue(maxsize=TANK_STATS_Q_MAX)
		edit_tasks 	: list[Task] 			= list()
		message('Counting tank-stats to scan for edits')
		
		if args.tank_stats_edit_cmd == 'remap-release':
			N : int = await db.tank_stats_count(release=release, regions=regions,
											accounts=accounts, since=since, sample=sample)
			edit_tasks.append(create_task(cmd_edit_rel_remap(db, tank_statQ, commit, N)))
			
			stats.merge_child(await db.tank_stats_get_worker(tank_statQ, 
						    								release=release, 
															regions=regions, 
															accounts=accounts,
															since=since, 
															sample=sample))
			
			await tank_statQ.join()
		if args.tank_stats_edit_cmd == 'fix-missing':
			BATCH: int = 100
			writeQ : CounterQueue[list[WGTankStat]]  = CounterQueue(maxsize=TANK_STATS_Q_MAX, 
							   										batch=BATCH)
			for _ in range(args.workers): 
				edit_tasks.append(create_task(cmd_edit_fix_missing(db, tank_statQ, 
						       								writeQ=writeQ,
															missing=args.field, 
															commit=commit, 
															batch = BATCH)))
			edit_tasks.append(create_task(db.tank_stats_insert_worker(writeQ, force=commit)))
			edit_tasks.append(create_task(alive_bar_monitor([writeQ], 
						   									title=f'Fixing {args.field}', 
															total=None)))
			for r in regions:
				edit_tasks.append(create_task(db.tank_stats_get_worker(tank_statQ,
						    								release=release, 
															regions={r}, 
															accounts=accounts,
															missing=args.field,
															since=since, 
															sample=sample)))

			await tank_statQ.join()
			await writeQ.join()
		else:
			raise NotImplementedError			
		
		await stats.gather_stats(edit_tasks)
		
		message(stats.print(do_print=False, clean=True))

	except Exception as err:
		error(f'{err}')
	return False


async def cmd_edit_fix_missing(db: Backend, 
								tank_statQ : IterableQueue[WGTankStat], 
								writeQ: Queue[list[WGTankStat]],
								missing: str,
								commit: bool = False, 
								batch : int = 1000
								) -> EventCounter:
	"""Fix missing fields by estimating those from later stats"""
	debug('starting')
	stats : EventCounter = EventCounter('fix missing')
	tank_stats : list[WGTankStat] = list()
	try:		
		missing = missing.split('.')[1]
		while True:
			ts : WGTankStat = await tank_statQ.get()
			try:
				fixed : bool = False
				tsa: WGTankStatAll = ts.all
				if (region := ts.region) is None: 
					stats.log('errors')
					continue

				async for later in db.tank_stats_get(accounts=[BSAccount(id=ts.account_id, region=ts.region)], 
													regions={region}, 
													tanks=[Tank(tank_id=ts.tank_id)], 
													since=ts.last_battle_time + 1):
					try:
						ltsa = later.all
						lvalue = getattr(ltsa, missing)
						value = int(lvalue * tsa.battles / ltsa.battles)
						setattr(tsa, missing, value)
						ts.all = tsa
						debug(f'updating {ts}: all.{missing}={getattr(ts.all, missing)}')
						if commit:
							tank_stats.append(ts)
							if len(tank_stats) == batch:
								await writeQ.put(tank_stats)
								stats.log('fixed', batch)
								tank_stats = list()
						else:
							message(f"Would fix all.{missing}: account={ts.account_id} tank={ts.tank_id} lbt={ts.last_battle_time}: {value} (battles={tsa.battles}) [{lvalue} (battles={ltsa.battles})]")
							stats.log('would fix')
						fixed = True
						break					
					
					except Exception as err:
						verbose(f'Failed to update {ts}: {err}')
				if not fixed:
					stats.log('could not fix')
					verbose(f'could not fix: {ts}')
			except Exception as err:
				error(f'failed to fix {ts}')
			finally:
				tank_statQ.task_done()
	
	except QueueDone:
		debug('Queue done')
		if len(tank_stats) > 0:
			await writeQ.put(tank_stats)
			stats.log('fixed', len(tank_stats))
	except CancelledError:
		debug('cancelled')		
	except Exception as err:
		error(f'{err}')	
	return stats



async def cmd_edit_rel_remap(db: Backend, 
								tank_statQ : Queue[WGTankStat], 
								commit: bool = False, 
								total: int | None = None) -> EventCounter:
	"""Remap tank stat's releases"""
	debug('starting')
	stats : EventCounter = EventCounter('remap releases')
	try:
		release 	: BSBlitzRelease | None
		release_map : BucketMapper[BSBlitzRelease] = await release_mapper(db)
		with alive_bar(total, title="Remapping tank stats' releases ", refresh_secs=1,
						enrich_print=False) as bar:
			while True:
				ts : WGTankStat = await tank_statQ.get()			
				try:
					release = release_map.get(ts.last_battle_time)
					if release is None:
						error(f'Could not map: {ts}')
						stats.log('errors')
						continue
					if ts.release != release.release:
						if commit:
							ts.release = release.release
							debug(f'Remapping {ts.release} to {release.release}: {ts}')
							if await db.tank_stat_update(ts, fields=['release']):
								debug(f'remapped release for {ts}')
								stats.log('updated')
							else:
								debug(f'failed to remap release for {ts}')
								stats.log('failed to update')
						else:
							message(f'Would update release {ts.release} to {release.release} for {ts}')
					else:
						debug(f'No need to remap: {ts}')
						stats.log('no need')			
				except Exception as err:
					error(f'could not remap {ts}: {err}')
				finally:
					tank_statQ.task_done()
					bar()
	except CancelledError as err:
		debug(f'Cancelled')
	except Exception as err:
		error(f'{err}')
	return stats


########################################################
# 
# cmd_prune()
#
########################################################


async def cmd_prune(db: Backend, args : Namespace) -> bool:
	"""prune tank stats"""
	# performance can be increased 5-10x with multiprocessing
	debug('starting')
	try:
		regions		: set[Region] 		= { Region(r) for r in args.regions }
		sample 		: int 				= args.sample
		release 	: BSBlitzRelease  	= BSBlitzRelease(release=args.release)
		stats 		: EventCounter 		= EventCounter(f'tank-stats prune {release}')
		commit 		: bool 				= args.commit
		tankQ 		: Queue[Tank]		= Queue()
		workers 	: list[Task]		= list()
		WORKERS 	: int 				= args.workers
		if sample > 0:
			WORKERS = 1
		progress_str : str 				= 'Finding duplicates ' 
		if commit:
			message(f'Pruning tank stats of release {release} from {db.table_uri(BSTableType.TankStats)}')
			message('Press CTRL+C to cancel in 3 secs...')
			await sleep(3)
			progress_str = 'Pruning duplicates '
		
		async for tank_id in db.tank_stats_unique('tank_id', int, 
													release=release, 
													regions=regions):
			await tankQ.put(Tank(tank_id=tank_id))

		# N : int = await db.tankopedia_count()
		N : int = tankQ.qsize()

		for _ in range(WORKERS):
			workers.append(create_task(prune_worker(db, tankQ=tankQ, release=release, 
													regions=regions, commit=commit, sample=sample)))
		prev : int = 0
		done : int
		left : int
		with alive_bar(N, title=progress_str, refresh_secs=1) as bar:
			while (left := tankQ.qsize()) > 0:
				done = N - left
				if (done - prev) > 0:
					bar(done - prev)
				prev = done
				await sleep(1)
			
		await tankQ.join()
		await stats.gather_stats(workers)
		stats.print()
		return True
	except Exception as err:
		error(f'{err}')
	return False


async def prune_worker(db : Backend, 
						tankQ: Queue[Tank],
						# tank: Tank, 
						release: BSBlitzRelease, 
						regions : set[Region], 
						commit: bool = False, 
						sample : int = 0) -> EventCounter:
	"""Worker to delete duplicates"""
	debug(f'starting')
	stats 	: EventCounter 	= EventCounter('duplicates')
	try:
		while True:
			tank = await tankQ.get()
			async for dup in db.tank_stats_duplicates(tank, release, regions):
				stats.log('found')
				if commit:
					if await db.tank_stat_delete(account_id=dup.account_id, 
												last_battle_time=dup.last_battle_time, 
												tank_id=tank.tank_id):
						verbose(f'deleted duplicate: {dup}')
						stats.log('deleted')
					else:
						error(f'failed to delete duplicate: {dup}')
						stats.log('deletion errors')
				else:
					verbose(f'duplicate:  {dup}')
					async for newer in db.tank_stats_get(release=release, 
														regions=regions, 
														accounts=[BSAccount(id=dup.account_id)], 
														tanks=[tank], 
														since=dup.last_battle_time + 1):
						verbose(f'newer stat: {newer}')
				if sample == 1:
					tankQ.task_done()
					raise CancelledError
				elif sample > 1:
					sample -= 1
			tankQ.task_done()
	except CancelledError:
		debug('cancelled')		
	except Exception as err:
		error(f'{err}')
	return stats


########################################################
# 
# cmd_export_text()
#
########################################################

async def cmd_export_text(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')		
		assert type(args.sample) in [int, float], 'param "sample" has to be a number'
		
		stats 		: EventCounter 			= EventCounter('tank-stats export')
		regions		: set[Region] 			= { Region(r) for r in args.regions }
		filename	: str					= args.filename
		force		: bool 					= args.force
		export_stdout : bool 				= filename == '-'
		sample 		: float = args.sample
		accounts 	: list[BSAccount] | None = read_args_accounts(args.accounts)
		tanks 		: list[Tank] | None 	= read_args_tanks(args.tanks)
		release 	: BSBlitzRelease | None = None
		if args.release is not None:
			release = (await get_releases(db, [args.release]))[0]

		tank_statQs 		: dict[str, IterableQueue[WGTankStat]] = dict()
		backend_worker 		: Task 
		export_workers 		: list[Task] = list()
		
		total : int = await db.tank_stats_count(regions=regions, sample=sample, 
												accounts=accounts, tanks=tanks, 
												release=release)

		tank_statQs['all'] = IterableQueue(maxsize=ACCOUNTS_Q_MAX, count_items=not args.by_region)
		# fetch tank_stats for all the regios
		backend_worker = create_task(db.tank_stats_get_worker(tank_statQs['all'], 
															regions=regions, sample=sample, 
															accounts=accounts, tanks=tanks, 
															release=release))
		if args.by_region:
			for region in regions:
				tank_statQs[region.name] = IterableQueue(maxsize=ACCOUNTS_Q_MAX)											
				export_workers.append(create_task(export(Q=cast(Queue[CSVExportable] | Queue[TXTExportable] | Queue[JSONExportable], 
																tank_statQs[region.name]), 
														format=args.format, 
														filename=f'{filename}.{region.name}', 
														force=force, 
														append=args.append)))
			# split by region
			export_workers.append(create_task(split_tank_statQ_by_region(Q_all=tank_statQs['all'], 
									regionQs=cast(dict[str, Queue[WGTankStat]], tank_statQs))))
		else:
			if filename != '-':
				filename += '.all'
			export_workers.append(create_task(export(Q=cast(Queue[CSVExportable] | Queue[TXTExportable] | Queue[JSONExportable], 
															tank_statQs['all']), 
											format=args.format, 
											filename=filename, 
											force=force, 
											append=args.append)))
		bar : Task | None = None
		if not export_stdout:
			bar = create_task(alive_bar_monitor(list(tank_statQs.values()), 'Exporting tank_stats', 
												total=total, enrich_print=False, refresh_secs=1))		
			
		await wait( [backend_worker] )
		for queue in tank_statQs.values():
			await queue.join() 
		if bar is not None:
			bar.cancel()
		for res in await gather(backend_worker):
			if isinstance(res, EventCounter):
				stats.merge_child(res)
			elif type(res) is BaseException:
				error(f'{db.driver}: tank_stats_get_worker() returned error: {res}')
		for worker in export_workers:
			worker.cancel()
		for res in await gather(*export_workers):
			if isinstance(res, EventCounter):
				stats.merge_child(res)
			elif type(res) is BaseException:
				error(f'export(format={args.format}) returned error: {res}')
		if not export_stdout:
			message(stats.print(do_print=False, clean=True))

	except Exception as err:
		error(f'{err}')
	return False


########################################################
# 
# cmd_export_data() OLD VERSION
#
########################################################

# async def cmd_export_data(db: Backend, args : Namespace) -> bool:
# 	debug('starting')	
# 	assert args.format in EXPORT_DATA_FORMATS, "--format has to be 'arrow' or 'parquet'"
# 	MAX_WORKERS : int 			= 10
# 	stats 		: EventCounter 	= EventCounter('tank-stats export')	
# 	try:
		
		
# 		regions		: set[Region] 			= { Region(r) for r in args.regions }
# 		release 	: BSBlitzRelease		= BSBlitzRelease(release=args.RELEASE)

# 		# sample 		: float 				= args.sample
# 		# accounts 	: list[BSAccount] | None= read_args_accounts(args.accounts)
# 		# tanks 		: list[Tank] | None 	= read_args_tanks(args.tanks)
# 		# tanks_tiers : dict[EnumVehicleTier, list[Tank]] = dict()

# 		## Logic TODO
# 		# Form a tank Queue in main process
# 		# MP processes to fetch, validate, transform, flatten and join the data with tank_id/tier
# 		# writing to dataset in the main process

# 		with Manager() as manager:			
# 			doneQ : queue.Queue[int | None]	= manager.Queue()
# 			options : dict[str, Any] 		= dict()
# 			options['force'] 				= args.force
# 			options['format']				= args.format
# 			options['basedir']				= args.basedir
# 			options["filename"]				= args.filename
# 			options['regions']				= regions
# 			options['release']				= release.release
			
# 			WORKERS : int = min( [cpu_count() - 1, MAX_WORKERS ])

# 			with Pool(processes=WORKERS, initializer=export_data_mp_init, 
# 					  initargs=[ db.config, doneQ, db.model_tank_stats, options ]) as pool:

# 				debug(f'starting {WORKERS} workers')
# 				results : AsyncResult = pool.map_async(export_data_mp_worker_start, range(1,11))
# 				pool.close()

# 				N : int = await db.tankopedia_count()

# 				with alive_bar(N, title="Exporting tank stats ", 
# 								enrich_print=False, refresh_secs=1) as bar:	
# 					active : int = 10
# 					tank_id : int | None
# 					while active > 0:
# 						if (tank_id:= doneQ.get()) is None:
# 							active -= 1
# 						else:
# 							debug(f'tank_id {tank_id} processed')
# 							stats.log("tanks exported")
# 							bar()
				
# 				for res in results.get():
# 					stats.merge_child(res)
# 				pool.join()

# 	except Exception as err:
# 		error(f'{err}')
# 	stats.print()
# 	return False


# def export_data_mp_init(backend_config	: dict[str, Any],					
# 						doneQ 			: queue.Queue[int | None],
# 						import_model	: type[JSONExportable],
# 						options 		: dict[str, Any]):
# 	"""Initialize static/global backend into a forked process"""
# 	global db, progressQ, in_model, mp_options
# 	debug(f'starting (PID={getpid()})')

# 	if (tmp_db := Backend.create(**backend_config)) is None:
# 		raise ValueError('could not create backend')	
# 	db 					= tmp_db
# 	progressQ 			= AsyncQueue.from_queue(doneQ)
# 	in_model 			= import_model	
# 	mp_options			= options
# 	debug('finished')
	

# def export_data_mp_worker_start(tier: int = 0) -> EventCounter:
# 	"""Forkable tank stats import worker"""
# 	debug(f'starting import worker #{tier}')
# 	return run(export_data_mp_worker(tier), debug=False)


# async def  export_data_mp_worker(tier: int = 0) -> EventCounter:
# 	"""Forkable tank stats import worker"""
# 	debug(f'#{id}: starting')
# 	stats 		: EventCounter 	= EventCounter('importer')
# 	workers 	: list[Task]	= list()
# 	try: 
# 		global db, progressQ, in_model, mp_options
# 		THREADS 	: int = 4		
# 		import_model: type[JSONExportable] 		= in_model
		
# 		tankQ			: Queue[Tank | None] 	= Queue()
# 		dataQ 			: Queue[pd.DataFrame]	= Queue(100)
# 		force 			: bool 					= mp_options['force']
# 		export_format	: str					= mp_options['format']
# 		basedir 		: str 					= mp_options["basedir"]
# 		filename 		: str 					= mp_options["filename"]		
# 		regions 		: set[Region]			= mp_options['regions']	
# 		release 		: BSBlitzRelease 		= BSBlitzRelease(release=mp_options['release'])

# 		export_file : str = os.path.join(basedir, release.release, f'{filename}.{tier}')

# 		for _ in range(THREADS):
# 			workers.append(create_task(export_data_fetcher(db, progressQ, release, tankQ, dataQ, regions) ))		
# 		workers.append(create_task(export_data_writer(export_file, dataQ, export_format, force)))

# 		async for tank in db.tankopedia_get_many(tier=EnumVehicleTier(tier)):
# 			await tankQ.put(tank)
# 		await tankQ.put(None)
		
# 		await tankQ.join()
# 		await dataQ.join()

# 		await stats.gather_stats(workers)	
# 	except CancelledError:
# 		pass
# 	except Exception as err:
# 		error(f'{err}')	
# 	return stats


# async def export_data_fetcher(db: Backend, 
# 							  progressQ : AsyncQueue[int | None], 
# 							  release 	: BSBlitzRelease,
# 							  tankQ 	: Queue[Tank | None], 
# 							  dataQ 	: Queue[pd.DataFrame], 
# 							  regions 	: set[Region],
# 							  ) -> EventCounter:
# 	"""Fetch tanks stats data from backend and convert to Pandas data frames"""
# 	debug('starting')
# 	stats : EventCounter 	= EventCounter(f'fetch {db.driver}')
# 	tank  : Tank | None
# 	tank_stats : list[dict[str, Any]] = list()
# 	BATCH : int = TANK_STATS_BATCH
# 	try:		
# 		while (tank := await tankQ.get()) is not None:
# 			async for tank_stat in db.tank_stats_get(release=release, regions=regions, tanks=[tank]):
# 				tank_stats.append(tank_stat.obj_src())
# 				if len(tank_stats) == BATCH:
# 					await dataQ.put(pd.json_normalize(tank_stats).drop('id', axis=1))
# 					stats.log('tank stats read', len(tank_stats))
# 					tank_stats = list()
			
# 			stats.log('tanks processed')
# 			await progressQ.put(tank.tank_id)
# 			tankQ.task_done()
		
# 		if len(tank_stats) > 0:
# 			await dataQ.put(pd.json_normalize(tank_stats).drop('id', axis=1))
# 			stats.log('tank stats read', len(tank_stats))
		
# 		tankQ.task_done()
# 		await progressQ.put(None)  # add sentinel
		
# 	except CancelledError:
# 		debug('cancelled')
# 	except Exception as err:
# 		error(f'{err}')	
# 	return stats


# async def export_data_writer(filename: str, 
# 							dataQ : Queue[pd.DataFrame], 
# 							export_format : str, 
# 							force: bool = False) -> EventCounter:
# 	"""Worker to write on data stats to a file in format"""
# 	debug('starting')
# 	assert export_format in EXPORT_DATA_FORMATS, f"export format has to be one of: {', '.join(EXPORT_DATA_FORMATS)}"
# 	stats : EventCounter 	= EventCounter(f'writer')
# 	try:
# 		export_file : str = f'{filename}.{export_format}'
# 		if not force and isfile(export_file):
# 			raise FileExistsError(export_file)
# 		makedirs(dirname(filename), exist_ok=True)
# 		batch 	: pa.RecordBatch = pa.RecordBatch.from_pandas(await dataQ.get())
# 		schema 	: pa.Schema 	 = batch.schema
# 		with pa.OSFile(export_file, 'wb') as sink:
# 			with pa.ipc.new_file(sink, schema, 
# 								options=pa.ipc.IpcWriteOptions(compression='lz4')) as writer:
# 				while True:
# 					try:
# 						writer.write_batch(batch)
# 					except Exception as err:
# 						error(f'{err}')	
# 					stats.log('rows written', batch.num_rows)
# 					dataQ.task_done()
# 					batch = pa.RecordBatch.from_pandas(await dataQ.get(), schema)					

# 	except CancelledError:
# 		debug('cancelled')
# 	except Exception as err:
# 		error(f'{err}')	
# 	return stats


########################################################
# 
# cmd_export_career()
#
########################################################


async def cmd_export_career(db: Backend, args : Namespace) -> bool:
	"""Export career stats for accounts who played during release"""
	debug('starting')
	# global export_total_rows	
	assert args.format in EXPORT_DATA_FORMATS, "--format has to be 'arrow' or 'parquet'"
	WORKERS : int 			= args.workers  
	stats 	: EventCounter 	= EventCounter('tank-stats export')	
	try:		
		regions		: set[Region] 	= { Region(r) for r in args.regions }
		release 	: BSBlitzRelease= BSBlitzRelease(release=args.RELEASE)
		force 		: bool			= args.force
		format		: str			= args.format
		basedir 	: str			= os.path.join(args.basedir, release.release, \
													'career_' + ('after' if args.after else 'before') ) 
		options 		: dict[str, Any]= dict()
		options['release']				= release
		options['after']				= args.after

		if WORKERS > 0:
			WORKERS = min( [cpu_count() - 1, WORKERS ])
		else:
			WORKERS = cpu_count() - 1

		with Manager() as manager:			
			dataQ : queue.Queue[pd.DataFrame]	 = manager.Queue(TANK_STATS_Q_MAX)
			workQ : queue.Queue[BSAccount| None] = manager.Queue(100)
			adataQ: AsyncQueue[pd.DataFrame]	 = AsyncQueue.from_queue(dataQ)
			aworkQ: AsyncQueue[BSAccount | None] = AsyncQueue.from_queue(workQ)
			writer : Task = create_task(export_dataset_writer(basedir, adataQ, format, force))

			with Pool(processes=WORKERS, initializer=export_career_mp_init, 
					  initargs=[ db.config, workQ, dataQ, options ]) as pool:
				message('Counting tank stats...')
				N : int = await db.tank_stats_unique_count('account_id',
															release=release, 
															regions=regions)
				Qcreator : Task = create_task(create_accountQ_active(db, aworkQ, release, 
																	regions, 
																	randomize=True))
				debug(f'starting {WORKERS} workers')
				results : AsyncResult = pool.map_async(export_career_stats_mp_worker_start, 
														range(WORKERS))
				pool.close()		

				prev: int = 0
				done: int = 0
				delta: int = 0
				with alive_bar(N, title="Exporting tank stats ", 
								enrich_print=False, refresh_secs=1) as bar:	
					
					while not Qcreator.done():						
						done = 	aworkQ.items
						delta = done - prev
						if delta > 0:
							bar(delta)
						prev = done
						await sleep(1)

				for _ in range(WORKERS):
					await aworkQ.put(None)
				for res in results.get():
					stats.merge_child(res)
				pool.join()

			await adataQ.join()
			await stats.gather_stats([writer, Qcreator])
			
	except Exception as err:
		error(f'{err}')
	stats.print()
	return False


def export_career_mp_init(backend_config	: dict[str, Any],	
						 accountQ 		: queue.Queue[BSAccount | None],
						 dataQ 			: queue.Queue[pd.DataFrame],
						 options 		: dict[str, Any]):
	"""Initialize static/global backend into a forked process"""
	global db, workQ_a, writeQ, mp_options
	debug(f'starting (PID={getpid()})')

	if (tmp_db := Backend.create(**backend_config)) is None:
		raise ValueError('could not create backend')	
	db 			= tmp_db
	workQ_a 	= AsyncQueue.from_queue(accountQ)
	writeQ 		= AsyncQueue.from_queue(dataQ)
	mp_options	= options
	debug('finished')


def export_career_stats_mp_worker_start(worker: int = 0) -> EventCounter:
	"""Forkable tank stats import worker cor career stats"""
	debug(f'starting import worker #{worker}')
	return run(export_career_stats_mp_worker(worker), debug=False)


async def  export_career_stats_mp_worker(worker: int = 0) -> EventCounter:
	"""Forkable tank stats import worker for latest (career) stats"""
	global db, workQ_a, writeQ, mp_options

	debug(f'#{worker}: starting')
	stats 		: EventCounter 	= EventCounter('importer')
	THREADS 	: int = 4		

	try:		
		release : BSBlitzRelease 	= mp_options['release']
		after	: bool 				= mp_options['after']
		workers : list[Task]		= list()
		if not after:			
			if (rel := await db.release_get_previous(release)) is None:
				raise ValueError(f'could not find previous release: {release}')
			else:
				release = rel
				
		for _ in range(THREADS):
			workers.append(create_task(export_career_fetcher(db, workQ_a, writeQ, release)))		

		for w in workers:
			stats.merge_child(await w)
		debug(f'#{worker}: async workers done')
	except CancelledError:
		pass
	except Exception as err:
		error(f'{err}')	
	return stats


async def export_career_fetcher(db: Backend, 							  
							  accountQ 	: AsyncQueue[BSAccount | None], 
							  dataQ 	: AsyncQueue[pd.DataFrame],
							  release 	: BSBlitzRelease
							  ) -> EventCounter:
	"""Fetch tanks stats data from backend and convert to Pandas data frames"""
	debug('starting')
	stats 		: EventCounter 			= EventCounter(f'fetch {db.driver}')
	datas 		: list[dict[str, Any]] 	= list()
	tank_stats 	: list[WGTankStat]
	try:		
		while (account := await accountQ.get()) is not None:
		
			async for tank_stats in db.tank_stats_export_career(account=account, release=release):
				datas.extend([ ts.obj_src() for ts in tank_stats ])
				if len(datas) >= TANK_STATS_BATCH:
					# error(f'putting DF to dataQ: {len(tank_stats)} rows')
					await dataQ.put(pd.json_normalize(datas   ))
					stats.log('tank stats read', len(datas))
					datas = list()
	
			stats.log('accounts')
			accountQ.task_done()
	
		if len(datas) > 0:
			await dataQ.put(pd.json_normalize(datas))
			stats.log('tank stats read', len(datas))
		
	except CancelledError:
		debug('cancelled')
	except Exception as err:
		error(f'{err}')	
	
	accountQ.task_done()
	await accountQ.put(None)
	
	return stats


########################################################
# 
# cmd_export_update()  
#
########################################################


async def cmd_export_update(db: Backend, args : Namespace) -> bool:
	debug('starting')	
	assert args.format in EXPORT_DATA_FORMATS, "--format has to be 'arrow' or 'parquet'"
	MAX_WORKERS : int 			= 16
	stats 		: EventCounter 	= EventCounter('tank-stats export')	
	try:		
		regions			: set[Region] 	= { Region(r) for r in args.regions }
		release 		: BSBlitzRelease= BSBlitzRelease(release=args.RELEASE)
		force 			: bool			= args.force
		export_format 	: str			= args.format
		basedir 		: str			= os.path.join(args.basedir, release.release, 'update_total') 
		# filename 		: str			= args.filename
		options 		: dict[str, Any]= dict()
		options['regions']				= regions
		options['release']				= release.release
		tank_id 		: int
		WORKERS 		: int 			= min( [cpu_count() - 1, MAX_WORKERS ])

		with Manager() as manager:			
			dataQ : queue.Queue[pd.DataFrame]= manager.Queue(TANK_STATS_Q_MAX)
			tankQ : queue.Queue[int | None] = manager.Queue()
			adataQ: AsyncQueue[pd.DataFrame]=AsyncQueue.from_queue(dataQ)
			atankQ: AsyncQueue[int | None]  = AsyncQueue.from_queue(tankQ)
			worker : Task = create_task(export_dataset_writer(basedir, adataQ, export_format, force))

			with Pool(processes=WORKERS, initializer=export_update_mp_init, 
					  initargs=[ db.config, tankQ, dataQ, options ]) as pool:
				
				async for tank_id in db.tank_stats_unique('tank_id', int, regions=regions, 
															release=release):
					await atankQ.put(tank_id)					

				debug(f'starting {WORKERS} workers, {atankQ.qsize()} tanks')
				results : AsyncResult = pool.map_async(export_data_update_mp_worker_start, 
														range(WORKERS))
				pool.close()
				
				N 	: int = atankQ.qsize()
				left : int  = N
				prev: int = 0
				done: int

				with alive_bar(N, title="Exporting tank stats ", 
								enrich_print=False, refresh_secs=1) as bar:	
					while True:
						left = atankQ.qsize()
						done = N - left
						bar(done - prev)
						prev = done
						if left == 0:
							break
						await sleep(1)

				await atankQ.join()
				for _ in range(WORKERS):
					await atankQ.put(None)				
				for res in results.get():
					stats.merge_child(res)
				pool.join()

			await adataQ.join()
			await stats.gather_stats([worker])
			
	except Exception as err:
		error(f'{err}')
	stats.print()
	return False


def export_update_mp_init(backend_config	: dict[str, Any],	
							tankQ 			: queue.Queue[int | None],
							dataQ 			: queue.Queue[pd.DataFrame],
							options 		: dict[str, Any]):
	"""Initialize static/global backend into a forked process"""
	global db, workQ_t, writeQ, mp_options
	debug(f'starting (PID={getpid()})')

	if (tmp_db := Backend.create(**backend_config)) is None:
		raise ValueError('could not create backend')	
	db 			= tmp_db
	workQ_t 	= AsyncQueue.from_queue(tankQ)
	writeQ 		= AsyncQueue.from_queue(dataQ)
	mp_options	= options
	debug('finished')


def export_data_update_mp_worker_start(worker: int = 0) -> EventCounter:
	"""Forkable tank stats import worker"""
	debug(f'starting import worker #{worker}')
	return run(export_data_update_mp_worker(worker), debug=False)


async def  export_data_update_mp_worker(worker: int = 0) -> EventCounter:
	"""Forkable tank stats import worker"""
	global db, workQ_t, writeQ, mp_options

	debug(f'#{worker}: starting')
	stats 		: EventCounter 	= EventCounter('importer')
	workers 	: list[Task]	= list()
	THREADS 	: int = 4		

	try:		
		regions 		: set[Region]			= mp_options['regions']	
		release 		: BSBlitzRelease 		= BSBlitzRelease(release=mp_options['release'])

		for _ in range(THREADS):
			workers.append(create_task(export_update_fetcher(db, release, regions, workQ_t, writeQ) ))		

		for w in workers:
			stats.merge_child(await w)
		debug(f'#{worker}: async workers done')
	except CancelledError:
		pass
	except Exception as err:
		error(f'{err}')	
	return stats


async def export_update_fetcher(db: Backend, 							  
							  release 	: BSBlitzRelease,
							  regions 	: set[Region],
							  tankQ 	: AsyncQueue[int | None], 
							  dataQ 	: AsyncQueue[pd.DataFrame],							  
							  ) -> EventCounter:
	"""Fetch tanks stats data from backend and convert to Pandas data frames"""
	debug('starting')
	stats 		: EventCounter 			= EventCounter(f'fetch {db.driver}')
	tank_stats 	: list[dict[str, Any]] 	= list()

	try:		
		while (tank_id := await tankQ.get()) is not None:

			async for tank_stat in db.tank_stats_get(release=release, regions=regions, 
														tanks=[Tank(tank_id=tank_id)]):
				tank_stats.append(tank_stat.obj_src())
				if len(tank_stats) == TANK_STATS_BATCH:
					await dataQ.put(pd.json_normalize(tank_stats))
					stats.log('tank stats read', len(tank_stats))
					tank_stats = list()
			
			stats.log('tanks processed')
			tankQ.task_done()
		
		if len(tank_stats) > 0:
			await dataQ.put(pd.json_normalize(tank_stats))
			stats.log('tank stats read', len(tank_stats))
		
	except CancelledError:
		debug('cancelled')
	except Exception as err:
		error(f'{err}')	
	
	tankQ.task_done()
	await tankQ.put(None)	
	
	return stats


async def export_dataset_writer(basedir: str,
								dataQ : AsyncQueue[pd.DataFrame], 
								export_format : str, 
								force: bool = False) -> EventCounter:
	"""Worker to write on data stats to a file in format"""
	global EXPORT_WRITE_BATCH
	debug('starting')
	assert export_format in EXPORT_DATA_FORMATS, f"export format has to be one of: {', '.join(EXPORT_DATA_FORMATS)}"
	stats 	: EventCounter 	= EventCounter(f'writer')

	try:
		makedirs(dirname(basedir), exist_ok=True)

		# batch 	: pa.RecordBatch = pa.RecordBatch.from_pandas(await dataQ.get())
		# schema 	: pa.Schema 	 = batch.schema
		batch 	: pa.RecordBatch
		schema 	: pa.Schema 	 	= WGTankStat.arrow_schema()
		part 	: ds.Partitioning 	= ds.partitioning(pa.schema([("region", pa.string())]))
		dfs 	: list[pa.RecordBatch] 	= list()
		rows: int = 0		
		i 	: int = 0
		try:
			while True:
				batch = pa.RecordBatch.from_pandas(await dataQ.get(), schema)
				try:				
					rows += batch.num_rows					
					dfs.append(batch)
					if rows > EXPORT_WRITE_BATCH:
						debug(f'writing {rows} rows')
						ds.write_dataset(dfs, base_dir=basedir, 
										basename_template=f'part-{i}' + '-{i}.' + f'{export_format}',
										format=export_format, 
										partitioning=part, 
										schema=schema, 
										existing_data_behavior='overwrite_or_ignore')
						stats.log('stats written', rows)
						rows = 0
						i += 1
						dfs = list()
				except Exception as err:
					error(f'{err}')
				dataQ.task_done()
				# batch = pa.RecordBatch.from_pandas(await dataQ.get(), schema)
		
		except CancelledError:
			debug('cancelled')
		
		if len(dfs) > 0:
			ds.write_dataset(dfs, base_dir=basedir, 
							basename_template=f'part-{i}' + '-{i}.' + f'{export_format}',
							format=export_format, 
							partitioning=part, 
							schema=schema, 
							existing_data_behavior='overwrite_or_ignore')
			stats.log('stats written', rows)

	except Exception as err:
		error(f'{err}')
	return stats


async def export_data_writer(basedir		: str,
							filename		: str, 
							dataQ 			: AsyncQueue[pd.DataFrame], 
							export_format	: str, 
							force			: bool = False) -> EventCounter:
	"""Worker to write on data stats to a file in format"""
	debug('starting')
	# global export_total_rows
	assert export_format in EXPORT_DATA_FORMATS, f"export format has to be one of: {', '.join(EXPORT_DATA_FORMATS)}"
	stats 	: EventCounter 			= EventCounter(f'writer')

	try:
		makedirs(dirname(basedir), exist_ok=True)
		export_file : str = os.path.join(basedir, f'{filename}.{export_format}')
		if not force and isfile( export_file):
			raise FileExistsError(export_file)
		schema 	: pa.Schema 	 = WGTankStat.arrow_schema()

		with pq.ParquetWriter(export_file, schema, compression='lz4') as writer:
			while True:
				batch = pa.RecordBatch.from_pandas(await dataQ.get(), schema)
				try:
					writer.write_batch(batch)
					stats.log('rows written', batch.num_rows)
				except Exception as err:
					error(f'{err}')				
				dataQ.task_done()
				

	except CancelledError:
		debug('cancelled')
	except Exception as err:
		error(f'{err}')	

	return stats


########################################################
# 
# cmd_import()  DEPRECIATED
#
########################################################


# async def cmd_import(db: Backend, args : Namespace) -> bool:
# 	"""Import tank stats from other backend"""	
# 	try:
# 		debug('starting')
# 		assert is_alphanum(args.import_model), f'invalid --import-model: {args.import_model}'
# 		stats 		: EventCounter 				= EventCounter('tank-stats import')
# 		tank_statsQ	: Queue[list[WGTankStat]]	= Queue(100)
# 		rel_mapQ	: Queue[list[WGTankStat]]	= Queue(100)
# 		import_db   	: Backend | None 				= None
# 		import_backend 	: str 							= args.import_backend
# 		import_model 	: type[JSONExportable] | None 	= None
# 		release_map 	: BucketMapper[BSBlitzRelease] | None = None
# 		WORKERS 	 	: int 							= args.workers
# 		workers : list[Task] = list()

# 		if (import_model := get_sub_type(args.import_model, JSONExportable)) is None:
# 			raise ValueError("--import-model has to be subclass of JSONExportable")

# 		if (import_db := Backend.create_import_backend(driver=import_backend, 
# 														args=args, 
# 														import_type=BSTableType.TankStats, 
# 														copy_from=db,
# 														config_file=args.import_config)) is None:
# 			raise ValueError(f'Could not init {import_backend} to import releases from')

# 		if not args.no_release_map: 
# 			release_map = await release_mapper(db)
		
# 		for _ in range(WORKERS):
# 			workers.append(create_task(db.tank_stats_insert_worker(tank_statsQ=tank_statsQ, 
# 																	force=args.force)))
# 		rel_map_worker : Task = create_task(map_releases_worker(release_map, inputQ=rel_mapQ, 
# 		 														outputQ=tank_statsQ))
# 		message('Counting tank stats to import ...')
# 		N : int = await import_db.tank_stats_count(sample=args.sample)
# 		read: 	int	= 0
# 		with alive_bar(N, title="Importing tank stats ", enrich_print=False, refresh_secs=1) as bar:
# 			async for tank_stats in import_db.tank_stats_export(model=import_model, 
# 																sample=args.sample):
# 				read = len(tank_stats)
# 				stats.log('read', read)				
# 				await rel_mapQ.put(tank_stats)
# 				bar(read)
				

# 		await rel_mapQ.join()
# 		rel_map_worker.cancel()
# 		await stats.gather_stats([rel_map_worker])
# 		await tank_statsQ.join()		
# 		await stats.gather_stats(workers)

# 		message(stats.print(do_print=False, clean=True))
# 		return True
# 	except Exception as err:
# 		error(f'{err}')	
# 	return False


# async def cmd_importMP2(db: Backend, args : Namespace) -> bool:
# 	"""Import tank stats from other backend"""	
# 	try:
# 		debug('starting')		
# 		stats 			: EventCounter 					= EventCounter('tank-stats import')
# 		import_db   	: Backend | None 				= None
# 		import_backend 	: str 							= args.import_backend
# 		import_model 	: type[JSONExportable] | None 	= None
# 		WORKERS 	 	: int 							= args.workers
# 		map_releases	: bool 							= not args.no_release_map
# 		release_map		: BucketMapper[BSBlitzRelease] | None  = None		
# 		workers 		: list[Task] 				  	= list()

# 		if (import_model := get_sub_type(args.import_model, JSONExportable)) is None:
# 			raise ValueError("--import-model has to be subclass of JSONExportable")
		
# 		if (import_db := Backend.create_import_backend(driver=import_backend, 
# 														args=args, 
# 														import_type=BSTableType.TankStats, 
# 														copy_from=db,
# 														config_file=args.import_config)) is None:
# 			raise ValueError(f'Could not init {import_backend} to import releases from')

# 		message('Counting tank stats to import ...')
# 		N : int = await import_db.tank_stats_count(sample=args.sample)
# 		if map_releases:
# 			release_map = await release_mapper(db)
		
# 		# debug('#############################################################################')
# 		# db.debug()
# 		await db.test()

# 		with Manager() as manager:
# 			readQ	: queue.Queue[list[Any] | None] 	= manager.Queue(TANK_STATS_Q_MAX)
# 			writeQ	: queue.Queue[list[WGTankStat]] 	= manager.Queue(TANK_STATS_Q_MAX)
# 			tank_statsQ : AsyncQueue[list[WGTankStat]] = AsyncQueue.from_queue(writeQ)

# 			for _ in range(5):
# 				workers.append(create_task(db.tank_stats_insert_worker(tank_statsQ=tank_statsQ, 
# 																	force=args.force)))

# 			with Pool(processes=WORKERS, initializer=import_mp2_init, 
# 					  initargs=[ readQ, writeQ, import_model, release_map ]) as pool:
				
# 				results : AsyncResult = pool.map_async(import_mp2_worker, range(WORKERS))				
# 				pool.close()
								
# 				with alive_bar(N, title="Importing tank stats ", enrich_print=False, refresh_secs=1) as bar:					
# 					async for objs in import_db.objs_export(table_type=BSTableType.TankStats, 
# 															sample=args.sample, 
# 															batch=TANK_STATS_BATCH):
# 						debug(f'read {len(objs)} tank_stat objects')
# 						readQ.put(objs)
# 						stats.log('imported', len(objs))
# 						bar(len(objs))	
				
# 				debug(f'Finished exporting {import_model} from {import_db.table_uri(BSTableType.TankStats)}')
# 				for _ in range(WORKERS):
# 					readQ.put(None) # add sentinel
				
# 				for res in results.get():
# 					stats.merge_child(res)
# 				pool.join()
# 				# left : int = writeQ.qsize()*TANK_STATS_BATCH
# 				# with alive_bar(left, title="Finishing importing", enrich_print=False) as bar:
# 				# 	while not writeQ.empty():
# 				# 		bar(left - writeQ.qsize()*TANK_STATS_BATCH)
# 				# 		await sleep(0.5)
# 			await tank_statsQ.join()
# 			await stats.gather_stats(workers)				
		
# 		message(stats.print(do_print=False, clean=True))
# 		return True
# 	except Exception as err:
# 		error(f'{err}')	
# 	return False


# def import_mp2_init(inputQ 		: queue.Queue[list[Any] | None], 
# 					outputQ    	: queue.Queue[list[WGTankStat]], 
# 					import_model: type[JSONExportable], 
# 					release_map : BucketMapper[BSBlitzRelease] | None) -> None:
# 	"""Initialize static/global backend into a forked process"""
# 	global readQb, writeQ, in_model, rel_mapper, opt_force
# 	debug(f'starting (PID={getpid()})')

# 	readQb 		= inputQ
# 	writeQ		= outputQ
# 	in_model 	= import_model	
# 	rel_mapper 	= release_map
# 	debug('finished')


# def  import_mp2_worker(id: int = 0) -> EventCounter:
# 	"""Forkable tank stats import worker"""
# 	debug(f'#{id}: starting')
# 	stats : EventCounter = EventCounter('import (MP)')

# 	try: 
# 		global readQb, writeQ, in_model, rel_mapper, opt_force
# 		THREADS 	: int = 1		
# 		import_model: type[JSONExportable] 					= in_model
# 		release_map : BucketMapper[BSBlitzRelease] | None 	= rel_mapper		
				
# 		while objs := readQb.get():
# 			debug(f'#{id}: read {len(objs)} objects')
# 			try:
# 				read : int = len(objs)
# 				debug(f'read {read} documents')
# 				stats.log('read', read)	
# 				tank_stats, errors = transform_objs(in_type=import_model, objs=objs)	
# 				stats.log('conversion OK', len(tank_stats))				
# 				stats.log('conversion errors', errors)

# 				if release_map is not None:
# 					tank_stats, mapped, errors = map_releases(tank_stats, release_map)
# 					stats.log('release mapped', mapped)
# 					stats.log('release map errors', errors)
# 				writeQ.put(tank_stats)
# 			except Exception as err:
# 				error(f'{err}')
# 			finally:
# 				readQb.task_done()		
# 		debug(f'#{id}: finished reading objects')
# 		readQb.task_done()			
	
# 	except CancelledError:
# 		pass
# 	except Exception as err:
# 		error(f'{err}')	
# 	return stats


async def cmd_importMP(db: Backend, args : Namespace) -> bool:
	"""Import tank stats from other backend"""	
	try:
		debug('starting')		
		stats 			: EventCounter 					= EventCounter('tank-stats import')
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
														import_type=BSTableType.TankStats, 
														copy_from=db,
														config_file=args.import_config)) is None:
			raise ValueError(f'Could not init {import_backend} to import releases from')

		with Manager() as manager:
			
			readQ	: queue.Queue[list[Any] | None] = manager.Queue(TANK_STATS_Q_MAX)
			options : dict[str, Any] 				= dict()
			options['force'] 						= args.force
			options['map_releases'] 				= not args.no_release_map
			
			with Pool(processes=WORKERS, initializer=import_mp_init, 
					  initargs=[ db.config, readQ, import_model, options ]) as pool:

				debug(f'starting {WORKERS} workers')
				results : AsyncResult = pool.map_async(import_mp_worker_start, range(WORKERS))
				pool.close()

				message('Counting tank stats to import ...')
				N : int = await import_db.tank_stats_count(sample=args.sample)
				
				with alive_bar(N, title="Importing tank stats ", 
								enrich_print=False, refresh_secs=1) as bar:					
					async for objs in import_db.objs_export(table_type=BSTableType.TankStats, 
															sample=args.sample):
						read : int = len(objs)
						# debug(f'read {read} tank_stat objects')
						readQ.put(objs)
						stats.log(f'{db.driver}:stats read', read)
						bar(read)	
				
				debug(f'Finished exporting {import_model} from {import_db.table_uri(BSTableType.TankStats)}')
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
					options : dict[str, Any]):
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
	"""Forkable tank stats import worker"""
	debug(f'starting import worker #{id}')
	return run(import_mp_worker(id), debug=False)


async def  import_mp_worker(id: int = 0) -> EventCounter:
	"""Forkable tank stats import worker"""
	debug(f'#{id}: starting')
	stats : EventCounter = EventCounter('importer')
	workers 	: list[Task] 				= list()
	try: 
		global db, readQ, in_model, mp_options
		THREADS 	: int = 4		
		import_model: type[JSONExportable] 					= in_model
		rel_mapper 	: BucketMapper[BSBlitzRelease] | None 	= None
		tank_statsQ	: Queue[list[WGTankStat]] 				= Queue(100)
		force 		: bool 									= mp_options['force']
		rel_map		: bool									= mp_options['map_releases']
		tank_stats  : list[WGTankStat]
		
		if rel_map:
			rel_mapper = await release_mapper(db)

		for _ in range(THREADS):
			workers.append(create_task(db.tank_stats_insert_worker(tank_statsQ=tank_statsQ, 
																	force=force)))		
		errors : int = 0
		mapped : int = 0
		while (objs := await readQ.get()) is not None:
			debug(f'#{id}: read {len(objs)} objects')
			try:
				read : int = len(objs)
				debug(f'read {read} documents')
				stats.log('stats read', read)	
				tank_stats = WGTankStat.transform_objs(objs=objs, in_type=import_model)
				errors = len(objs) - len(tank_stats)		
				stats.log('tank stats read', len(tank_stats))				
				stats.log('format errors', errors)
				if rel_mapper is not None:
					tank_stats, mapped, errors = map_releases(tank_stats, rel_mapper)
					stats.log('release mapped', mapped)
					stats.log('not release mapped', read - mapped)
					stats.log('release map errors', errors)
				await tank_statsQ.put(tank_stats)
			except Exception as err:
				error(f'{err}')
			finally:
				readQ.task_done()		
		debug(f'#{id}: finished reading objects')
		readQ.task_done()	
		await tank_statsQ.join() 		# add sentinel for other workers
		await stats.gather_stats(workers)	
	except CancelledError:
		pass
	except Exception as err:
		error(f'{err}')	
	return stats


# async def import_read_worker(import_db: Backend, 										
# 										importQ: queue.Queue, 
# 										total: int = 0, 
# 										sample: float = 0):
# 	debug(f'starting import from {import_db.driver}')
# 	stats : EventCounter = EventCounter(f'{import_db.driver} import')
# 	with alive_bar(total, title="Importing tank stats ", enrich_print=False, refresh_secs=1) as bar:
# 		async for objs in import_db.objs_export(table_type=BSTableType.TankStats, 
# 												sample=sample):
# 			debug(f'read {len(objs)} tank_stat objects')
# 			importQ.put(objs)
# 			stats.log('imported', len(objs))
# 			bar(len(objs))
	
# 	importQ.put(None) # add sentinel
# 	return stats


# def import_worker(in_type: type[JSONExportable], 
# 					importQ: queue.Queue, 
# 					tank_statQ: queue.Queue[list[WGTankStat]],
# 					release_map: BucketMapper[BSBlitzRelease] | None) -> EventCounter:
# 	"""Forkable tank stats import worker"""
# 	debug('starting')
# 	stats 		: EventCounter = EventCounter('import')
# 	try:		
# 		tank_stats 	: list[WGTankStat]
# 		objs : list[Any] | None
# 		mapped : int = 0
# 		errors : int = 0
# 		while (objs := importQ.get()) is not None:			 
# 			try:
# 				read : int = len(objs)
# 				debug(f'read {read} documents')
# 				stats.log('read', read)	
# 				tank_stats, mapped, errors = transform_objs(in_type=in_type, 
# 															objs=objs, 
# 															release_map=release_map)		
# 				stats.log('release mapped', mapped)
# 				stats.log('errors', errors)				
# 				tank_statQ.put(tank_stats)
# 			except Exception as err:
# 				error(f'{err}')
		
# 		# close other insert workers
# 		importQ.put(None)			
# 	except Exception as err:
# 		error(f'{err}')	
# 	return stats	


# def transform_objs(in_type: type[JSONExportable], 
# 				objs: list[Any]							
# 				) -> tuple[list[WGTankStat], int]:
# 	"""Transform list of objects to list[WGTankStat]"""

# 	debug('starting')	
# 	errors: int = 0 
# 	tank_stats : list[WGTankStat] = list()
# 	for obj in objs:
# 		try:
# 			obj_in = in_type.parse_obj(obj)					
# 			if (tank_stat := WGTankStat.transform(obj_in)) is None:						
# 				tank_stat = WGTankStat.parse_obj(obj_in.obj_db())			
# 			tank_stats.append(tank_stat)						
# 		except Exception as err:
# 			error(f'{err}')
# 			errors += 1
# 	return tank_stats, errors		
	

def map_releases(tank_stats: list[WGTankStat], 
				release_map: BucketMapper[BSBlitzRelease]) -> tuple[list[WGTankStat], int, int]:
	debug('starting')
	mapped: int = 0
	errors: int = 0
	res : list[WGTankStat] = list()
	for tank_stat in tank_stats:		
		try:			
			if (release := release_map.get(tank_stat.last_battle_time)) is not None:
				tank_stat.release = release.release
				mapped += 1									
		except Exception as err:
			error(f'{err}')
			errors += 1
		res.append(tank_stat)
	return res, mapped, errors


async def map_releases_worker(release_map: BucketMapper[BSBlitzRelease] | None, 
								inputQ: 	Queue[list[WGTankStat]], 
								outputQ:	Queue[list[WGTankStat]]) -> EventCounter:

	"""Map tank stats to releases and pack those to list[WGTankStat] queue.
		map_all is None means no release mapping is done"""
	stats 		: EventCounter = EventCounter('Release mapper')
	release 	: BSBlitzRelease | None
	try:
		debug('starting')
		while True:
			tank_stats = await inputQ.get()
			stats.log('read', len(tank_stats))
			try:
				if release_map is not None:
					for tank_stat in tank_stats:
						if (release := release_map.get(tank_stat.last_battle_time)) is not None:
							tank_stat.release = release.release
							stats.log('mapped')
						else:
							stats.log('could not map')
				else:
					stats.log('not mapped', len(tank_stats))
			
				await outputQ.put(tank_stats)						
			except Exception as err:
				error(f'Processing input queue: {err}')
			finally: 									
				inputQ.task_done()
	except CancelledError:
		debug('Cancelled')		
	except Exception as err:
		error(f'{err}')	
	return stats


async def split_tank_statQ_by_region(Q_all, regionQs : dict[str, Queue[WGTankStat]], 
								progress: bool = False, 
								bar_title: str = 'Splitting tank stats queue') -> EventCounter:
	debug('starting')
	stats : EventCounter = EventCounter('tank stats')
	try:
		with alive_bar(Q_all.qsize(), title=bar_title, manual=True, refresh_secs=1,
						enrich_print=False, disable=not progress) as bar:
			while True:
				tank_stat : WGTankStat = await Q_all.get()
				try:
					if tank_stat.region is None:
						raise ValueError(f'account ({tank_stat.id}) does not have region defined')
					if tank_stat.region.name in regionQs: 
						await regionQs[tank_stat.region.name].put(tank_stat)
						stats.log(tank_stat.region.name)
					else:
						stats.log(f'excluded region: {tank_stat.region.name}')
				except CancelledError:
					raise CancelledError from None
				except Exception as err:
					stats.log('errors')
					error(f'{err}')
				finally:
					stats.log('total')
					Q_all.task_done()
					bar()
					if progress and Q_all.qsize() == 0:
						break

	except CancelledError as err:
		debug(f'Cancelled')
	except Exception as err:
		error(f'{err}')
	return stats


def read_args_tanks(tank_ids : list[int]) -> list[Tank] | None:
	tanks : list[Tank] = list() 
	for id in tank_ids:
		tanks.append(Tank(tank_id=id))
	if len(tanks) == 0:
		return None
	return tanks