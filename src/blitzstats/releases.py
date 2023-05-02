from argparse import ArgumentParser, Namespace
from configparser import ConfigParser
from typing import Optional, cast, Iterable, Any
import logging
from asyncio import create_task, gather, Queue, CancelledError, Task, sleep

from aiohttp import ClientResponse
from datetime import date, datetime
from os.path import isfile

from pyutils 			import EventCounter, JSONExportable, CSVExportable, \
								TXTExportable, BucketMapper
from pyutils.exportable import export
from pyutils.utils 		import  is_alphanum
from blitzutils 		import WGBlitzRelease

from .backend import Backend, BSTableType, get_sub_type
from .models import BSBlitzRelease
from .models_import import WG_Release

logger = logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug

###########################################
# 
# add_args functions  
#
###########################################


def add_args(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		releases_parsers = parser.add_subparsers(dest='releases_cmd', 	
												title='releases commands',
												description='valid commands',
												help='releases help',
												metavar='add | edit | remove | export | list')
		releases_parsers.required = True
		add_parser = releases_parsers.add_parser('add', help="releases add help")
		if not add_args_add(add_parser, config=config):
			raise Exception("Failed to define argument parser for: releases add")
		
		edit_parser = releases_parsers.add_parser('edit', help="releases edit help")
		if not add_args_edit(edit_parser, config=config):
			raise Exception("Failed to define argument parser for: releases edit")
		
		remove_parser = releases_parsers.add_parser('remove', help="releases remove help")
		if not add_args_remove(remove_parser, config=config):
			raise Exception("Failed to define argument parser for: releases remove")

		import_parser = releases_parsers.add_parser('import', help="releases import help")
		if not add_args_import(import_parser, config=config):
			raise Exception("Failed to define argument parser for: releases import")

		export_parser = releases_parsers.add_parser('export', aliases=['list'], 
					      							help="releases export help")
		if not add_args_export(export_parser, config=config):
			raise Exception("Failed to define argument parser for: releases export")
				
		return True
	except Exception as err:
		error(f'add_args(): {err}')
	return False


def add_args_add(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		parser.add_argument('release', type=str, default=None, metavar='RELEASE',
							help='RELEASE to add')
		parser.add_argument('--cut-off', type=str, default=None, help='release cut-off time')
		parser.add_argument('--launch', type=str, default=None, help='release launch date')

		return True	
	except Exception as err:
		error(f'add_args_add() : {err}')
	return False


def add_args_edit(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		parser.add_argument('release', type=str,metavar='RELEASE', help='RELEASE to edit')
		parser.add_argument('--cut-off', type=str, default=None, help='new release cut-off time')
		parser.add_argument('--launch', type=str, default=None, help='new release launch date')
		return True	
	except Exception as err:
		error(f'add_args_edit() : {err}')
	return False


def add_args_remove(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		parser.add_argument('release', type=str, metavar='RELEASE', help='RELEASE to remove')
		parser.add_argument('--force', action='store_true', default=False, 
							help='do not wait before removing releases ')
		return True	
	except Exception as err:
		error(f'add_args_remove() : {err}')
	return False


def add_args_import(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		import_parsers = parser.add_subparsers(dest='import_backend', 	
												title='releases import backend',
												description='valid import backends', 
												metavar=', '.join(Backend.list_available()))
		import_parsers.required = True

		for backend in Backend.get_registered():
			import_parser =  import_parsers.add_parser(backend.driver, help=f'releases import {backend.driver} help')
			if not backend.add_args_import(import_parser, config=config):
				raise Exception(f'Failed to define argument parser for: releases import {backend.driver}')
		
		parser.add_argument('--import-model', metavar='IMPORT-TYPE', type=str, required=True,
							choices=['BSBlitzRelease', 'WG_Release'], 
							help='data format to import. Default is blitz-stats native format.')
		parser.add_argument('--sample', type=int, default=0, metavar='SAMPLE', help='sample size')
		parser.add_argument('--force', action='store_true', default=False, 
							help='overwrite existing file(s) when exporting')
		parser.add_argument('--releases', type=str, metavar='RELEASE_MATCH', default=None, nargs='?',
							help='search by RELEASE_MATCH. By default list all.')
		parser.add_argument('--since', type=str, metavar='LAUNCH_DATE', default=None, nargs='?',
							help='import release launched after LAUNCH_DATE. By default, imports all releases.')
		return True	
	except Exception as err:
		error(f'{err}')
	return False



def add_args_export(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		parser.add_argument('release_match', type=str, metavar='RELEASE_MATCH', default=None, nargs='?',
							help='Search by RELEASE_MATCH. By default list all.')
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
		error(f'{err}')
	return False



def read_args_releases(strs: list[str] | None) -> list[BSBlitzRelease] | None:
	"""Read releases from arguments"""
	debug('starting')
	try:
		releases : list[BSBlitzRelease] = list()
		if strs is not None:			
			for r in strs:
				try:
					releases.append(BSBlitzRelease(release=r))
				except Exception as err:
					error(f'{err}')
			return releases
	except Exception as err:
		error(f'{err}')
	return None
	

###########################################
# 
# cmd functions  
#
###########################################

async def cmd(db: Backend, args : Namespace) -> bool:
	
	try:
		debug('starting')		
		if args.releases_cmd == 'add':
			debug('releases add')
			return await cmd_add(db, args)		
		elif args.releases_cmd == 'edit':
			debug('releases edit')
			return await cmd_edit(db, args)
		elif args.releases_cmd == 'remove':
			debug('releases remove')
			return await cmd_remove(db, args)
		elif args.releases_cmd == 'import':
			debug('releases import')
			return await cmd_import(db, args)	
		elif args.releases_cmd == 'export':
			debug('releases export')
			return await cmd_export(db, args)		
		else:
			error(f'Unknown or missing subcommand: {args.releases_cmd}')

	except Exception as err:
		error(f'{err}')
	return False


async def cmd_add(db: Backend, args : Namespace) -> bool:
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


async def cmd_edit(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		release = BSBlitzRelease(release=args.release, launch_date=args.launch, cut_off=args.cut_off)
		update : dict[str, Any] = release.dict(exclude_unset=True, exclude_none=True)
		del update['release']
		return await db.release_update(release, update=update)
	except Exception as err:
		error(f'{err}')
	return False


async def cmd_remove(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		message(f'Removing release {args.release} in 3 seconds. Press CTRL+C to cancel')
		await sleep(3)
		release = BSBlitzRelease(release=args.release)
		if await db.release_delete(release=release.release):
			message(f'release {release.release} removed')
			return True
		else:
			error(f'Could not remove release {release.release}')		
	except Exception as err:
		error(f'{err}')
	return False


async def cmd_export(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		releaseQ : Queue[BSBlitzRelease] = Queue(100)
		since 	 : int = 0 
		export_worker : Task		
		export_worker = create_task(export(Q=cast(Queue[CSVExportable] | Queue[TXTExportable] | Queue[JSONExportable], releaseQ), 
											format=args.format, filename=args.file, force=args.force))
				
		if args.since is not None:
			since = int(datetime.combine(date.fromisoformat(args.since), datetime.min.time()).timestamp())

		async for release in db.releases_get(release_match=args.release_match, since=since):
			debug(f'adding release {release.release} to the export queue')
			await releaseQ.put(release)
		await releaseQ.join()
		export_worker.cancel()
		
		return True 
	except Exception as err:
		error(f'{err}')
	return False


async def cmd_import(db: Backend, args : Namespace) -> bool:
	"""Import releases from other backend"""	
	try:
		assert is_alphanum(args.import_model), f'invalid --import-model: {args.import_model}'

		stats 			: EventCounter 					= EventCounter('releases import')
		releaseQ 		: Queue[BSBlitzRelease]			= Queue(100)
		import_db   	: Backend | None 				= None
		import_backend 	: str 							= args.import_backend
		
		write_worker : Task = create_task(db.releases_insert_worker(releaseQ=releaseQ, force=args.force))

		if (import_db := Backend.create_import_backend(driver=import_backend, 
														args=args, 
														import_type=BSTableType.Releases, 
														copy_from=db,
														config_file=args.import_config)) is None:
			raise ValueError(f'Could not init {import_backend} to import releases from')

		async for release in import_db.releases_export(sample=args.sample):
			await releaseQ.put(release)
			stats.log('read')

		await releaseQ.join()
		write_worker.cancel()
		worker_res : tuple[EventCounter|BaseException] = await gather(write_worker,return_exceptions=True)
		if type(worker_res[0]) is EventCounter:
			stats.merge_child(worker_res[0])
		elif type(worker_res[0]) is BaseException:
			error(f'releases insert worker threw an exception: {worker_res[0]}')
		stats.print()
		return True
	except Exception as err:
		error(f'{err}')	
	return False


async def get_releases(db: Backend, releases: list[str]) -> list[BSBlitzRelease]:
	debug(f'starting, releases={releases}')
	res : list[BSBlitzRelease] = list()
	try:		
		for r in releases:
			if (rel := await db.release_get(r)) is not None:
				res.append(rel)			
		res = [ *set(res) ]
	except Exception as err:
		error(f'{err}')	
	return res


## REFACTOR: Use sortedcollections.NearestDict instead. Should be faster. 

async def release_mapper(db: Backend) -> BucketMapper[BSBlitzRelease]:
	"""Fetch all releases and create a release BucketMapper object"""
	releases : BucketMapper[BSBlitzRelease] = BucketMapper[BSBlitzRelease](attr='cut_off')
	async for r in db.releases_get():
		releases.insert(r)
	return releases
