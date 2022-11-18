from argparse import ArgumentParser, Namespace
from configparser import ConfigParser
from typing import Optional, cast
import logging
from asyncio import create_task, gather, Queue, CancelledError, Task, sleep
from alive_progress import alive_bar		# type: ignore

from backend import Backend, OptAccountsInactive, ACCOUNTS_Q_MAX
from models import Account
from pyutils.eventcounter import EventCounter
from pyutils.utils import get_url, get_url_JSON_model, epoch_now
from blitzutils.models import WoTBlitzReplayJSON, Region
from blitzutils.wotinspector import WoTinspector

logger = logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug

WI_MAX_PAGES 	: int 				= 100
WI_MAX_OLD_REPLAYS: int 			= 30
WI_RATE_LIMIT	: Optional[float] 	= None
WI_AUTH_TOKEN	: Optional[str] 	= None

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
		
		return True
	except Exception as err:
		error(str(err))
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
		
		return True	
	except Exception as err:
		error(str(err))
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
		error(str(err))
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
		error(str(err))
	return False


def add_args_accounts_export(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		EXPORT_FORMAT 	= 'txt'
		EXPORT_FILE 	= 'accounts_export.txt'

		if config is not None and 'ACCOUNTS' in config.sections():
			configAccs 	= config['ACCOUNTS']
			EXPORT_FORMAT	= configAccs.get('export_format', EXPORT_FORMAT)
			EXPORT_FILE		= configAccs.get('export_file', EXPORT_FILE )

		# parser.add_argument('--format', type=str, choices=['json', 'txt', 'csv'], 
		# 					default=EXPORT_FORMAT, help='Accounts list file format')
		parser.add_argument('file', metavar='FILE', type=str, nargs=1, default=EXPORT_FILE, 
							help='File to export accounts to. Use \'-\' for STDIN')
		parser.add_argument('--disabled', action='store_true', default=False, help='Disabled accounts')
		parser.add_argument('--inactive', type=str, choices=[ o.value for o in OptAccountsInactive ], 
								default=OptAccountsInactive.default().value, help='Include inactive accounts')
		parser.add_argument('--region', type=str, choices=['any'] + [ r.value for r in Region ], 
								default=Region.API.value, help='Filter by region (default is API = eu + com + asia)')
		parser.add_argument('--sample', type=float, default=0, help='Sample accounts')

		return True	
	except Exception as err:
		error(str(err))
	return False


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
		error(str(err))
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

		elif args.accounts_cmd == 'remove':
			return await cmd_accounts_remove(db, args)

	except Exception as err:
		error(str(err))
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
			error(str(err))

		await accountQ.join()
		db_worker.cancel()
		worker_res : tuple[EventCounter | BaseException] = await gather(db_worker,return_exceptions=True)
		for res in worker_res:
			if type(res) is EventCounter:
				stats.merge_child(res)
			elif type(res) is BaseException:
				error(f'Backend ({db.name}) add_accounts_worker() returned error: {str(res)}')

		stats.print()

	except Exception as err:
		error(str(err))
	return False


async def accounts_add_worker(db: Backend, accountQ: Queue[list[int]]) -> EventCounter:
	"""worker to read accounts from queue and add those to backend"""
	debug('starting')
	stats : EventCounter = EventCounter(f'{db.name}')
	added 		: int
	not_added 	: int
	try:
		while True:
			players : list[int] = await accountQ.get()
			try:
				debug(f'Read {len(players)} from queue')
				stats.log('accounts total', len(players))
				try:
					accounts : list[Account] = list()
					for player in players:
						try:
							accounts.append(Account(id=player, added=epoch_now()))  # type: ignore
						except Exception as err:
							error(f'cound not create account object for account_id: {player}')
					added, not_added= await db.accounts_insert(accounts)
					stats.log('accounts added', added)
					stats.log('old accounts found', not_added)
				except Exception as err:
					stats.log('errors')
					error(f'Cound not add accounts do {db.name}: {str(err)}')
			except Exception as err:
				error(str(err))
			finally:
				accountQ.task_done()
	except CancelledError as err:
		debug(f'Cancelled')
	except Exception as err:
		error(str(err))
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
	force 		: bool  = args.force
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
		with alive_bar(replays, title="Fetching replays ", manual=True) as bar:
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
		error(str(err))
	finally:
		await wi.close()
	return stats


async def accounts_update_wi_spider_replays(db: Backend, wi: WoTinspector, args: Namespace,
                                           replay_idQ: Queue[str], pages: range) -> EventCounter:
	"""Spider replays.WoTinspector.com and feed found replay IDs into replayQ. Return stats"""
	debug('starting')
	stats: EventCounter = EventCounter('Crawler')
	max_old_replays: int = args.wi_max_old_replays
	force: bool = args.force
	old_replays: int = 0

	try:
		debug(f'Starting ({len(pages)} pages)')
		with alive_bar(len(pages), title= "Spidering replays") as bar:
			for page in pages:			
				try:
					if old_replays > max_old_replays:
						message(f'{max_old_replays} found. Stopping spidering for more')
						break
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
						res: WoTBlitzReplayJSON | None = await db.replay_get(replay_id)
						if res is not None:
							debug(f'Replay already in the {db.name}: {replay_id}')
							stats.log('old replays found')
							if not force:
								old_replays += 1
							continue
						else:
							await replay_idQ.put(replay_id)
							stats.log('new replays')
				except Exception as err:
					error(str(err))
				finally:
					bar()
	except CancelledError as err:
		debug(f'Cancelled')
	except Exception as err:
		error(str(err))
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
				replay : WoTBlitzReplayJSON | None = cast(WoTBlitzReplayJSON, await get_url_JSON_model(wi.session, url, WoTBlitzReplayJSON ))
				if replay is None:
					error(f'Could not fetch replay id: {replay_id}')
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
		error(str(err))	
	return stats


async def cmd_accounts_export(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')

		query_args : dict[str, str | int | float | bool ] = dict()
		
		disabled : bool =  args.disabled
		
		inactive : OptAccountsInactive = OptAccountsInactive.default()

		try: 
			inactive = OptAccountsInactive(args.inactive)
		except ValueError as err:
			assert False, f"Incorrect value for argument 'inactive': {args.inactive}"
		
		region : Region | None = None
		if args.region != 'any':
			region = Region(args.region)
		sample : float = args.sample
		
		async for account in db.accounts_get(region=region, inactive=inactive, disabled=disabled, sample=sample):
			print(account.json())

	except Exception as err:
		error(str(err))
	return False


async def cmd_accounts_remove(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		raise NotImplementedError
		

	except Exception as err:
		error(str(err))
	return False