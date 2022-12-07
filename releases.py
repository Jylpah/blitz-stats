from argparse import ArgumentParser, Namespace
from configparser import ConfigParser
from typing import Optional, cast, Iterable, Any
import logging
from asyncio import create_task, gather, Queue, CancelledError, Task, sleep

from aiohttp import ClientResponse
from datetime import date
from os.path import isfile

from pyutils.utils import export, JSONExportable, CSVExportable, TXTExportable
from pyutils import EventCounter
from backend import Backend
from blitzutils.models import WGBlitzRelease
from models import BSBlitzRelease
from models_import import WG_Release

logger = logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug

###########################################
# 
# add_args_releases functions  
#
###########################################


def add_args_releases(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		releases_parsers = parser.add_subparsers(dest='releases_cmd', 	
												title='releases commands',
												description='valid commands',
												help='releases help',
												metavar='add | edit | remove | list')
		releases_parsers.required = True
		add_parser = releases_parsers.add_parser('add', help="releases add help")
		if not add_args_releases_add(add_parser, config=config):
			raise Exception("Failed to define argument parser for: releases add")
		
		edit_parser = releases_parsers.add_parser('edit', help="releases edit help")
		if not add_args_releases_edit(edit_parser, config=config):
			raise Exception("Failed to define argument parser for: releases edit")
		
		remove_parser = releases_parsers.add_parser('remove', help="releases remove help")
		if not add_args_releases_remove(remove_parser, config=config):
			raise Exception("Failed to define argument parser for: releases remove")

		import_parser = releases_parsers.add_parser('import', help="releases import help")
		if not add_args_releases_import(import_parser, config=config):
			raise Exception("Failed to define argument parser for: releases import")

		export_parser = releases_parsers.add_parser('export', help="releases export help")
		if not add_args_releases_export(export_parser, config=config):
			raise Exception("Failed to define argument parser for: releases export")
				
		return True
	except Exception as err:
		error(f'add_args_releases(): {err}')
	return False


def add_args_releases_add(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		parser.add_argument('release', type=str, default=None, metavar='RELEASE',
							help='RELEASE to add')
		parser.add_argument('--cut-off', type=str, default=None, help='release cut-off time')
		parser.add_argument('--launch', type=str, default=None, help='release launch date')

		return True	
	except Exception as err:
		error(f'add_args_releases_add() : {err}')
	return False


def add_args_releases_edit(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		parser.add_argument('release', type=str,metavar='RELEASE', help='RELEASE to edit')
		parser.add_argument('--cut-off', type=str, default=None, help='new release cut-off time')
		parser.add_argument('--launch', type=str, default=None, help='new release launch date')
		return True	
	except Exception as err:
		error(f'add_args_releases_edit() : {err}')
	return False


def add_args_releases_remove(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		parser.add_argument('release', type=str, metavar='RELEASE', help='RELEASE to remove')
		return True	
	except Exception as err:
		error(f'add_args_releases_remove() : {err}')
	return False


def add_args_releases_import(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		import_parsers = parser.add_subparsers(dest='releases_import_backend', 	
														title='releases import backend',
														description='valid backends', 
														metavar=', '.join(Backend.list_available()))
		import_parsers.required = True

		for backend in Backend.get_registered():
			import_parser =  import_parsers.add_parser(backend.name, help=f'releases import {backend.name} help')
			if not backend.add_args_import(import_parser, config=config):
				raise Exception(f'Failed to define argument parser for: releases import {backend.name}')
		
		parser.add_argument('--import-type', metavar='IMPORT-TYPE', type=str, 
							default='BSBlitzRelease', choices=['BSBlitzRelease', 'WG_Release'], 
							help='Data format to import. Default is blitz-stats native format.')
		parser.add_argument('--sample', type=float, default=0, help='Sample size')
		parser.add_argument('--force', action='store_true', default=False, 
							help='Overwrite existing file(s) when exporting')
		parser.add_argument('--releases', type=str, metavar='RELEASE_MATCH', default=None, nargs='?',
							help='Search by RELEASE_MATCH. By default list all.')
		parser.add_argument('--since', type=str, metavar='LAUNCH_DATE', default=None, nargs='?',
							help='Import release launched after LAUNCH_DATE. By default, imports all releases.')
		return True	
	except Exception as err:
		error(f'add_args_releases_import() : {err}')
	return False


def add_args_releases_export(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		parser.add_argument('releases', type=str, metavar='RELEASE', default=None, nargs='?',
							help='Search by RELEASES_MATCH. By default list all.')
		parser.add_argument('--since', type=str, metavar='LAUNCH_DATE', default=None, nargs='?',
							help='Import release launched after LAUNCH_DATE. By default, imports all releases.')
		parser.add_argument('--format', type=str, choices=['json', 'txt', 'csv'], 
							metavar='FORMAT', default='txt', help='releases list format')
		parser.add_argument('--file', metavar='FILE', type=str, default='-', 
							help='File to export releases to. Use \'-\' for STDIN')
		parser.add_argument('--force', action='store_true', default=False, 
							help='Overwrite existing file(s) when exporting')
		return True	
	except Exception as err:
		error(f'add_args_releases_remove() : {err}')
	return False



###########################################
# 
# cmd_releases functions  
#
###########################################

async def cmd_releases(db: Backend, args : Namespace) -> bool:
	
	try:
		debug('starting')		
		if args.releases_cmd == 'add':
			debug('releases add')
			return await cmd_releases_add(db, args)		
		elif args.releases_cmd == 'edit':
			debug('releases edit')
			return await cmd_releases_edit(db, args)
		elif args.releases_cmd == 'remove':
			debug('releases remove')
			return await cmd_releases_remove(db, args)
		elif args.releases_cmd == 'import':
			debug('releases import')
			return await cmd_releases_import(db, args)	
		elif args.releases_cmd == 'export':
			debug('releases export')
			return await cmd_releases_export(db, args)		
		else:
			error(f'Unknown or missing subcommand: {args.releases_cmd}')

	except Exception as err:
		error(f'{err}')
	return False


async def cmd_releases_add(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		release : BSBlitzRelease | None = None
		try:
			release = BSBlitzRelease(release=args.release, launch_date=args.launch, cut_off=args.cut_off)
		except:
			debug(f'No valid release given as argument')

		if release is None:
			release = await db.release_get_latest()
			if release is None:
				raise ValueError('Could not find previous release and no new release set')
			return await db.release_insert(release.next())
		else:
			return await db.release_insert(release=release)
	except Exception as err:
		error(f'{err}')
	return False


async def cmd_releases_edit(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		release = BSBlitzRelease(release=args.release, launch_date=args.launch, cut_off=args.cut_off)
		return await db.release_update(release)		
	except Exception as err:
		error(f'{err}')
	return False


async def cmd_releases_remove(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		message(f'Removing release={args.release} in 3 seconds. Press CTRL+C to cancel')
		await sleep(3)
		release = BSBlitzRelease(release=args.release)
		if await db.release_delete(release=release):
			message(f'release {release.release} removed')
			return True
		else:
			error(f'Could not remove release {release.release}')		
	except Exception as err:
		error(f'{err}')
	return False


async def cmd_releases_export(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		releaseQ : Queue[BSBlitzRelease] = Queue(100)
		since 	 : date | None 			 = None
		export_worker : Task		
		export_worker = create_task(export(Q=cast(Queue[CSVExportable] | Queue[TXTExportable] | Queue[JSONExportable], releaseQ), 
											format=args.format, filename=args.file, force=args.force))
				
		if args.since is not None:
			since = date.fromisoformat(args.since)

		for release in await db.releases_get(release=args.releases, since=since):
			debug(f'adding release {release.release} to the export queue')
			await releaseQ.put(release)
		await releaseQ.join()
		export_worker.cancel()
		
		return True 
	except Exception as err:
		error(f'{err}')
	return False


async def cmd_releases_import(db: Backend, args : Namespace) -> bool:
	"""Import releases from other backend"""	
	try:
		stats 		: EventCounter 			= EventCounter('releases import')
		releaseQ 	: Queue[BSBlitzRelease]	= Queue(100)
		config 		: ConfigParser | None 	= None
		since 		: date | None 			= None
		releases 	: str  | None 			= args.releases

		if args.since is not None:
			since = date.fromisoformat(args.since)		

		importer : Task = create_task(db.releases_insert_worker(releaseQ=releaseQ, force=args.force))

		if args.import_config is not None and isfile(args.import_config):
			debug(f'Reading config from {args.config}')
			config = ConfigParser()
			config.read(args.config)

		kwargs : dict[str, Any] = Backend.read_args(args, args.releases_import_backend)
		if (import_db:= Backend.create(args.releases_import_backend, 
										config=config, copy_from=db, **kwargs)) is not None:
			if args.import_table is not None:
				import_db.set_table('RELEASES', args.import_table)
			elif db == import_db and db.table_releases == import_db.table_releases:
				raise ValueError('Cannot import from itself')
		else:
			raise ValueError(f'Could not init {args.releases_import_backend} to import releases from')

		release_type: type[WGBlitzRelease] = globals()[args.import_type]
		assert issubclass(release_type, WGBlitzRelease), "--import-type has to be subclass of blitzutils.models.WGBlitzRelease" 

		async for release in import_db.releases_export(release_type=release_type, since=since, 
														release=releases):
			await releaseQ.put(release)
			stats.log('read')

		await releaseQ.join()
		importer.cancel()
		worker_res : tuple[EventCounter|BaseException] = await gather(importer,return_exceptions=True)
		if type(worker_res[0]) is EventCounter:
			stats.merge_child(worker_res[0])
		elif type(worker_res[0]) is BaseException:
			error(f'releases insert worker threw an exception: {worker_res[0]}')
		stats.print()
		return True
	except Exception as err:
		error(f'{err}')	
	return False
