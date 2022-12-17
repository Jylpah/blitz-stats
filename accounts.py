from argparse import ArgumentParser, Namespace
from configparser import ConfigParser
from typing import Optional, cast, Type, Any, TypeVar
import logging
from asyncio import create_task, gather, wait, Queue, CancelledError, Task, sleep
from aiofiles import open
from asyncstdlib import enumerate
from os.path import isfile
from sys import stdout

from alive_progress import alive_bar		# type: ignore
from bson import ObjectId

from csv import DictWriter, DictReader, Dialect, Sniffer, excel

from backend import Backend, OptAccountsInactive, OptAccountsDistributed, BSTableType, ACCOUNTS_Q_MAX
from models import BSAccount, StatsTypes
from models_import import WG_Account
from pyutils import CounterQueue, EventCounter,  TXTExportable, CSVExportable, JSONExportable,\
					alive_queue_bar, get_url, get_url_JSON_model, epoch_now, export, \
					alias_mapper, is_alphanum
from blitzutils.models import WoTBlitzReplayJSON, Region, Account
from blitzutils.wotinspector import WoTinspector

logger 	= logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug

WI_MAX_PAGES 	: int 				= 100
WI_MAX_OLD_REPLAYS: int 			= 30
WI_RATE_LIMIT	: Optional[float] 	= None
WI_AUTH_TOKEN	: Optional[str] 	= None

EXPORT_SUPPORTED_FORMATS : list[str] = ['json', 'txt', 'csv']

###########################################
# 
# add_args_accouts functions  
#
###########################################

def add_args_accounts(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		accounts_parsers = parser.add_subparsers(dest='accounts_cmd', 	
												title='accounts commands',
												description='valid commands',
												metavar='update | export | remove')
		accounts_parsers.required = True
		
		update_parser = accounts_parsers.add_parser('update', aliases=['get'], help="accounts update help")
		if not add_args_accounts_update(update_parser, config=config):
			raise Exception("Failed to define argument parser for: accounts update")
		
		export_parser = accounts_parsers.add_parser('export', help="accounts export help")
		if not add_args_accounts_export(export_parser, config=config):
			raise Exception("Failed to define argument parser for: accounts export")
		
		remove_parser = accounts_parsers.add_parser('remove', aliases=['rm'], help="accounts remove help")
		if not add_args_accounts_remove(remove_parser, config=config):
			raise Exception("Failed to define argument parser for: accounts remove")

		import_parser = accounts_parsers.add_parser('import', help="accounts import help")
		if not add_args_accounts_import(import_parser, config=config):
			raise Exception("Failed to define argument parser for: accounts import")	
		
		return True
	except Exception as err:
		error(f'{err}')
	return False


def add_args_accounts_update(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		accounts_update_parsers = parser.add_subparsers(dest='accounts_update_source', 	
														title='accounts update source',
														description='valid sources', 
														metavar='wi | files')
		accounts_update_parsers.required = True
		accounts_update_wi_parser = accounts_update_parsers.add_parser('wi', help='accounts update wi help')
		if not add_args_accounts_update_wi(accounts_update_wi_parser, config=config):
			raise Exception("Failed to define argument parser for: accounts update wi")
		
		accounts_update_files_parser = accounts_update_parsers.add_parser('files', help='accounts update files help')
		if not add_args_accounts_update_files(accounts_update_files_parser, config=config):
			raise Exception("Failed to define argument parser for: accounts update files")		
		
		parser.add_argument('--force', action='store_true', default=False, 
							help='Ignore existing accounts exporting')
		
		return True	
	except Exception as err:
		error(f'{err}')
	return False


def add_args_accounts_update_wi(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
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
		parser.add_argument('--threads', '--workers', dest='wi_workers', 
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


def add_args_accounts_update_files(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
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


def add_args_accounts_export(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
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
		parser.add_argument('--region', type=str, nargs='*', choices=[ r.value for r in Region.API_regions() ], 
								default=[ r.value for r in Region.API_regions() ], help='Filter by region (default is API = eu + com + asia)')
		parser.add_argument('--by-region', action='store_true', default=False, help='Export accounts by region')
		parser.add_argument('--distributed', '--dist',type=int, dest='distributed', metavar='N', 
							default=0, help='Split accounts into N files distributed stats fetching')
		parser.add_argument('--sample', type=float, default=0, help='Sample accounts')

		return True	
	except Exception as err:
		error(f'{err}')
	return False


def add_args_accounts_import(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
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
				
		parser.add_argument('--import-type', metavar='IMPORT-TYPE', type=str, 
							default='BSAccount', choices=['BSAccount', 'WG_Account'], 
							help='Data format to import. Default is blitz-stats native format.')
		parser.add_argument('--region', type=str, nargs='*', 
								choices=[ r.value for r in Region.has_stats() ], 
								default=[ r.value for r in Region.has_stats() ], 
								help='Filter by region (default is API = eu + com + asia)')
		parser.add_argument('--sample', type=float, default=0, 
								help='Sample size. 0 < SAMPLE < 1 : %% of stats, 1<=SAMPLE : Absolute number')

		return True
	except Exception as err:
		error(f'{err}')
	return False


# def add_args_accounts_import_mongodb(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
# 	"""Add argument parser for accounts import"""
# 	try:
# 		debug('starting')
# 		parser.add_argument('--server-url', metavar='SERVER-URL', type=str, default=None, dest='import_host',
# 										help='MongoDB server URL to connect the server. \
# 											Required if the current backend is not the same MongoDB instance')
# 		parser.add_argument('--database', metavar='DATABASE', type=str, default=None, dest='import_database',
# 										help='Database to use. Uses current database as default')
# 		parser.add_argument('--collection', '--table', metavar='COLLECTION', type=str, default=None, dest='import_table',
# 										help='Collection to use. Uses current database as default')
# 		parser.add_argument('--import-type', metavar='IMPORT-TYPE', type=str, default='BSAccount', 
# 										choices=['WG_Account', 'BSAccount'], 
# 										help='Collection to use. Uses current database as default')


# 		return True
# 	except Exception as err:
# 		error(f'{err}')
# 	return False


# def add_args_accounts_import_files(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
# 	"""Add argument parser for accounts import"""
# 	try:
# 		debug('starting')
# 		parser.add_argument('files', type=str, nargs='+', metavar='FILE [FILE1 ...]', 
# 							help='Files to import')
# 		return True
# 	except Exception as err:
# 		error(f'{err}')
# 	return False


def add_args_accounts_remove(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
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

async def cmd_accounts(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')

		if args.accounts_cmd == 'update':
			return await cmd_accounts_update(db, args)

		elif args.accounts_cmd == 'export':
			return await cmd_accounts_export(db, args)
		
		elif args.accounts_cmd == 'import':
			return await cmd_accounts_import(db, args)

		elif args.accounts_cmd == 'remove':
			return await cmd_accounts_remove(db, args)

	except Exception as err:
		error(f'{err}')
	return False


async def cmd_accounts_update(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		
		stats = EventCounter('accounts update')
		accountQ : Queue[list[int]] = Queue(maxsize=ACCOUNTS_Q_MAX)
		db_worker = create_task(accounts_add_worker(db, accountQ))

		try:
			if args.accounts_update_source == 'wi':
				debug('wi')
				stats.merge_child(await cmd_accounts_update_wi(db, args, accountQ))
			elif args.accounts_update_source == 'files':
				debug('files')
				stats.merge_child(await cmd_accounts_update_files(db, args, accountQ))
		except Exception as err:
			error(f'{err}')

		await accountQ.join()
		db_worker.cancel()
		worker_res : tuple[EventCounter | BaseException] = await gather(db_worker,return_exceptions=True)
		for res in worker_res:
			if type(res) is EventCounter:
				stats.merge_child(res)
			elif type(res) is BaseException:
				error(f'{db.driver}: add_accounts_worker() returned error: {str(res)}')

		stats.print()

	except Exception as err:
		error(f'{err}')
	return False


async def accounts_add_worker(db: Backend, accountQ: Queue[list[int]]) -> EventCounter:
	"""worker to read accounts from queue and add those to backend"""
	debug('starting')
	stats : EventCounter = EventCounter(f'{db.driver}')
	added 		: int
	not_added 	: int
	try:
		while True:
			players : list[int] = await accountQ.get()
			try:
				debug(f'Read {len(players)} from queue')
				stats.log('accounts total', len(players))
				try:
					accounts : list[BSAccount] = list()
					for player in players:
						try:
							accounts.append(BSAccount(id=player, added=epoch_now()))  # type: ignore
						except Exception as err:
							error(f'cound not create account object for account_id: {player}')
					added, not_added= await db.accounts_insert(accounts)
					stats.log('accounts added', added)
					stats.log('old accounts found', not_added)
				except Exception as err:
					stats.log('errors')
					error(f'Cound not add accounts do {db.backend}: {err}')
			except Exception as err:
				error(f'{err}')
			finally:
				accountQ.task_done()
	except CancelledError as err:
		debug(f'Cancelled')
	except Exception as err:
		error(f'{err}')
	return stats


async def cmd_accounts_update_files(db: Backend, args : Namespace, 
									accountQ : Queue[list[int]]) -> EventCounter:
	
	debug('starting')
	raise NotImplementedError


async def cmd_accounts_update_wi(db: Backend, args : Namespace, accountQ : Queue[list[int]]) -> EventCounter:
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

	try:					
		step : int = 1
		if max_pages < 0:
			step = -1
		elif max_pages == 0:
			step = -1
			max_pages = - start_page
		
		pages : range = range(start_page,(start_page + max_pages), step)

		stats.merge_child(await accounts_update_wi_spider_replays(db, wi, args, replay_idQ, pages))

		replays 	: int = replay_idQ.qsize()
		replays_left: int = replays
		with alive_bar(replays, title="Fetching replays ", manual=True, enrich_print=False) as bar:
			for _ in range(workersN):
				workers.append(create_task(accounts_update_wi_fetch_replays(db, wi, replay_idQ, accountQ)))
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
		await wi.close()
	return stats


async def accounts_update_wi_spider_replays(db: Backend, wi: WoTinspector, args: Namespace,
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
					url: str = wi.get_url_replay_listing(page)
					resp: str | None = await get_url(wi.session, url)
					if resp is None:
						error('could not spider replays.WoTinspector.com page {page}')
						stats.log('errors')
						continue
					debug(f'HTTP request OK')
					replay_ids: set[str] = wi.parse_replay_ids(resp)
					debug(f'Page {page}: {len(replay_ids)} found')
					if len(replay_ids) == 0:
						break
					for replay_id in replay_ids:
						res: WoTBlitzReplayJSON | None = await db.replay_get(replay_id=replay_id)
						if res is not None:
							debug(f'Replay already in the {db.backend}: {replay_id}')
							stats.log('old replays found')
							if not force:
								old_replays += 1
							continue
						else:
							await replay_idQ.put(replay_id)
							stats.log('new replays')
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


async def accounts_update_wi_fetch_replays(db: Backend, wi: WoTinspector, replay_idQ : Queue[str], 
											accountQ : Queue[list[int]]) -> EventCounter:
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
				players : list[int] = replay.get_players()
				stats.log('players found', len(players))
				await accountQ.put(players)
				if await db.replay_insert(replay):
					stats.log('replays added')
				else:
					stats.log('replays not added')
			finally:
				replay_idQ.task_done()
	except Exception as err:
		error(f'{err}')	
	return stats


async def cmd_accounts_import(db: Backend, args : Namespace) -> bool:
	"""Import accounts from other backend"""	
	try:
		assert is_alphanum(args.import_type), f'invalid --import-type: {args.import_type}'

		stats 		: EventCounter 			= EventCounter('accounts import')
		accountQ 	: Queue[BSAccount]		= Queue(ACCOUNTS_Q_MAX)
		config 		: ConfigParser | None 	= None
		regions 	: set[Region]			= { Region(r) for r in args.region }
		import_type : str 					= args.import_type
		import_backend 	: str 				= args.import_backend
		import_table	: str | None		= args.import_table

		importer : Task = create_task(db.accounts_insert_worker(accountQ=accountQ, force=args.force))

		if args.import_config is not None and isfile(args.import_config):
			debug(f'Reading config from {args.config}')
			config = ConfigParser()
			config.read(args.config)

		kwargs : dict[str, Any] = Backend.read_args(args=args, backend=import_backend)
		if (import_db:= Backend.create(import_backend, config=config, copy_from=db, **kwargs)) is not None:
			if import_table is not None:
				import_db.set_table(BSTableType.Accounts, import_table)
			elif db == import_db and db.table_accounts == import_db.table_accounts:
				raise ValueError('Cannot import from itself')
		else:
			raise ValueError(f'Could not init {import_backend} to import accounts from')

		account_type: type[Account] = globals()[import_type]
		assert issubclass(account_type, Account), "--import-type has to be subclass of blitzutils.models.Account" 
		import_db.set_model(import_db.table_accounts, account_type)

		message('Counting accounts to import ...')
		N : int = await db.accounts_count(regions=regions,
										inactive=OptAccountsInactive.both,
										sample=args.sample)

		with alive_bar(N, title="Importing accounts ", enrich_print=False) as bar:
			async for account in import_db.accounts_export(data_type=account_type, sample=args.sample):
				await accountQ.put(account)
				bar()
				stats.log('read')

		await accountQ.join()
		importer.cancel()
		worker_res : tuple[EventCounter|BaseException] = await gather(importer,return_exceptions=True)
		if type(worker_res[0]) is EventCounter:
			stats.merge_child(worker_res[0])
		elif type(worker_res[0]) is BaseException:
			error(f'account insert worker threw an exception: {worker_res[0]}')
		stats.print()
		return True
	except Exception as err:
		error(f'{err}')	
	return False


# async def cmd_accounts_import_mongodb(db: Backend, args : Namespace, accountsQ: Queue[BSAccount],
# 										config: ConfigParser | None = None) -> EventCounter:
# 	stats : EventCounter = EventCounter('accounts import mongodb')
# 	try:
## 		regions : set[Region] ={ Region(r) for r in args.region }
		
# 		kwargs : dict[str, Any] = dict()
# 		if args.server_url is not None:
# 			kwargs['host'] = args.server_url
# 		if args.database is not None:
# 			kwargs['database'] = args.database

# 		if ( import_db:= Backend.create('mongodb', config=config, **kwargs)) is None:
# 			raise ValueError('Could not init mongodb to import accounts from')

# 		if args.collection is not None:
# 			import_db.set_table('ACCOUNTS', args.collection)
# 		elif db == import_db and db.table_accounts == import_db.table_accounts:
# 			raise ValueError('Cannot import from itself')

# 		message('Counting accounts to import ...')
# 		N : int = await db.accounts_count(regions=regions,
# 										inactive=OptAccountsInactive.both,
# 										sample=args.sample, force=True)

# 		with alive_bar(N, title="Importing accounts ", enrich_print=False) as bar:
			
# 			account_type: type[WG_Account] | type[BSAccount]
# 			if args.import_type == 'BSAccount':	
# 				account_type=BSAccount
# 			elif args.import_type == 'WG_Account':
# 				account_type=WG_Account
# 			else:
# 				raise ValueError(f'Unsupported account --import-type: {args.import_type}')

# 			async for account in import_db.accounts_export(account_type=account_type, regions=regions, 
# 															sample=args.sample):
# 				await accountsQ.put(account)
# 				bar()
# 				stats.log('read')

# 	except Exception as err:
# 		error(f'{err}')	
# 	return stats

# async def cmd_accounts_import_files(db: Backend, args : Namespace, accountsQ: Queue[BSAccount], 
# 									config: ConfigParser | None = None) -> EventCounter:
# 	stats : EventCounter 	= EventCounter('accounts import files')
# 	try:
# 		raise NotImplementedError
# 	except Exception as err:
# 		error(f'{err}')	
# 	return stats


async def cmd_accounts_export(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		assert type(args.distributed) is int, 'param "distributed" has to be integer'
		assert type(args.sample) in [int, float], 'param "sample" has to be a number'

		## not implemented...
		# query_args : dict[str, str | int | float | bool ] = dict()
		stats 		: EventCounter 			= EventCounter('accounts export')
		disabled 	: bool 					= args.disabled
		inactive 	: OptAccountsInactive 	= OptAccountsInactive.default()
		regions		: set[Region] 			= { Region(r) for r in args.region }
		distributed : OptAccountsDistributed 
		filename	: str					= args.filename
		force		: bool 					= args.force
		export_stdout : bool 				= filename == '-'
		sample 		: float = args.sample
		accountQs 	: dict[str, CounterQueue[BSAccount]] = dict()
		account_workers : list[Task] = list()
		export_workers 	: list[Task] = list()

		try: 
			inactive = OptAccountsInactive(args.inactive)
			if inactive == OptAccountsInactive.auto:		# auto mode requires specication of stats type
				inactive = OptAccountsInactive.no
		except ValueError as err:
			assert False, f"Incorrect value for argument 'inactive': {args.inactive}"
		
		total : int = await db.accounts_count(regions=regions, inactive=inactive, disabled=disabled, sample=sample)

		if args.distributed > 0:
			for i in range(args.distributed):
				accountQs[str(i)] = CounterQueue(maxsize=ACCOUNTS_Q_MAX)
				distributed = OptAccountsDistributed(i, args.distributed)
				account_workers.append(create_task(db.accounts_get_worker(accountQs[str(i)], regions=regions, 
														inactive=inactive, disabled=disabled, sample=sample,
														distributed=distributed)))
				export_workers.append(create_task(export(Q=cast(Queue[CSVExportable] | Queue[TXTExportable] | Queue[JSONExportable], 
															accountQs[str(i)]), 
														format=args.format, filename=f'{filename}.{i}', 
														force=force, append=args.append)))
		elif args.by_region:
			accountQs['all'] = CounterQueue(maxsize=ACCOUNTS_Q_MAX, count_items=False)
			# by region
			for region in regions:
				accountQs[region.name] = CounterQueue(maxsize=ACCOUNTS_Q_MAX)											
				export_workers.append(create_task(export(Q=cast(Queue[CSVExportable] | Queue[TXTExportable] | Queue[JSONExportable], 
																accountQs[region.name]), 
											format=args.format, filename=f'{filename}.{region.name}', 
											force=force, append=args.append)))
			
			# fetch accounts for all the regios
			account_workers.append(create_task(db.accounts_get_worker(accountQs['all'], regions=regions, 
														inactive=inactive, disabled=disabled, sample=sample)))
			# split by region
			export_workers.append(create_task(split_accountQ_by_region(Q_all=accountQs['all'], 
									regionQs=cast(dict[str, Queue[BSAccount]], accountQs))))
		else:
			accountQs['all'] = CounterQueue(maxsize=ACCOUNTS_Q_MAX)
			account_workers.append(create_task(db.accounts_get_worker(accountQs['all'], regions=regions, 
														inactive=inactive, disabled=disabled, sample=sample)))

			if filename != '-':
				filename += '.all'
			export_workers.append(create_task(export(Q=cast(Queue[CSVExportable] | Queue[TXTExportable] | Queue[JSONExportable], accountQs['all']), 
											format=args.format, filename=filename, 
											force=force, append=args.append)))
		
		bar : Task | None = None
		if not export_stdout:
			bar = create_task(alive_queue_bar(list(accountQs.values()), 'Exporting accounts', total=total, enrich_print=False))
			
		await wait(account_workers)
		for queue in accountQs.values():
			await queue.join() 
		if bar is not None:
			bar.cancel()
		for res in await gather(*account_workers):
			if isinstance(res, EventCounter):
				stats.merge_child(res)
			elif type(res) is BaseException:
				error(f'{db.driver}: accounts_get_worker() returned error: {res}')
		for worker in export_workers:
			worker.cancel()
		for res in await gather(*export_workers):
			if isinstance(res, EventCounter):
				stats.merge_child(res)
			elif type(res) is BaseException:
				error(f'export(format={args.format}) returned error: {res}')
		if not export_stdout:
			stats.print()

	except Exception as err:
		error(f'{err}')
	return False


async def cmd_accounts_remove(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		raise NotImplementedError

	except Exception as err:
		error(f'{err}')
	return False


async def split_accountQ_by_region(Q_all, regionQs : dict[str, Queue[BSAccount]], 
									progress: bool = False, 
									bar_title: str = 'Splitting account queue') -> EventCounter:
	debug('starting')
	stats : EventCounter = EventCounter('By region')
	try:
		with alive_bar(Q_all.qsize(), title=bar_title, manual=True, 
						enrich_print=False, disable=not progress) as bar:
			while True:
				account : BSAccount = await Q_all.get()
				try:
					if account.region is None:
						raise ValueError(f'account ({account.id}) does not have region defined')
					if account.region.name in regionQs: 
						await regionQs[account.region.name].put(account)
						stats.log(account.region.name)
					else:
						stats.log(f'excluded region: {account.region.name}')
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


async def create_accountQ(db: Backend, args : Namespace, 
							accountQ: Queue[BSAccount], 
							stats_type: StatsTypes | None, 
							bar_title = 'Processing accounts') -> None:
	"""Helper to make accountQ from arguments"""	
	try:
		regions	 	: set[Region]	= { Region(r) for r in args.region }
		accounts_N 		: int = 0
		accounts_added 	: int = 0

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
				inactive : OptAccountsInactive = OptAccountsInactive.default()
				try: 
					inactive = OptAccountsInactive(args.inactive)
				except ValueError as err:
					assert False, f"Incorrect value for argument 'inactive': {args.inactive}"

				accounts_N = await db.accounts_count(stats_type=stats_type, regions=regions, inactive=inactive,
													sample=args.sample, cache_valid=args.cache_valid)

		if accounts_N == 0:
			raise ValueError('No accounts found')		

		with alive_bar(accounts_N, title= bar_title, manual=True, enrich_print=False) as bar:
			
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
	except CancelledError as err:
		debug(f'Cancelled')
	except Exception as err:
		error(f'{err}')
	return None


def read_account_strs(accounts: list[str]) -> list[BSAccount] | None:
	res : list[BSAccount] = list()
	for a in accounts:
		try:
			res.append(BSAccount.from_str(a))
		except Exception as err:
			error(f'{err}')
	if len(res) == 0:
		return None
	return res