from argparse import ArgumentParser, Namespace
from configparser import ConfigParser
from typing import Optional, cast, Iterable
import logging
from asyncio import create_task, gather, Queue, CancelledError, Task
from aiohttp import ClientResponse
from datetime import date

from pyutils.utils import get_timestamp, export, JSONExportable, CSVExportable, TXTExportable


from backend import Backend
from models import BSBlitzRelease

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
		parser.add_argument('--cut-off', type=str, default='0', help='release cut-off time')
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


def add_args_releases_export(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		parser.add_argument('release', type=str, metavar='RELEASE', default=None, nargs='?',
							help='Search release. By default list all.')
		parser.add_argument('--format', type=str, choices=['json', 'txt', 'csv'], 
							metavar='FORMAT', default='txt', help='releases list format')
		parser.add_argument('--file', metavar='FILE', type=str, default='-', 
							help='File to export accounts to. Use \'-\' for STDIN')
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
		release : str | None = args.releases_add_release
		
		rel :  BSBlitzRelease | None
		if release is None:
			rel = await db.release_get_latest()
			if rel is None:
				raise ValueError('Could not find previous release and no new release set')
			return await db.release_insert(rel.next())
		else:
			# cut_off : int = 0
			# if args.cut_off is not None:
			# 	cut_off = get_timestamp(args.cut_off)
			# launch : date | None = None
			# if args.launch is not None:
			# 	launch = date.fromisoformat(args.launch)
			return await db.release_insert(BSBlitzRelease(release=release, launch_date=args.launch, 
															cut_off=args.cut_off))
	except Exception as err:
		error(f'{err}')
	return False


async def cmd_releases_edit(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')

		return True 
	except Exception as err:
		error(f'{err}')
	return False


async def cmd_releases_remove(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')

		return True 
	except Exception as err:
		error(f'{err}')
	return False


async def cmd_releases_export(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		releaseQ : Queue[BSBlitzRelease] = Queue(100)
		export_worker : Task		
		export_worker = create_task(export(Q=cast(Queue[CSVExportable] | Queue[TXTExportable] | Queue[JSONExportable], releaseQ), 
											format=args.format, filename=args.file, force=args.force))
		
		for release in await db.releases_get(release=args.release):
			debug(f'adding release {release.release} to the export queue')
			await releaseQ.put(release)
		await releaseQ.join()
		export_worker.cancel()
		
		return True 
	except Exception as err:
		error(f'{err}')
	return False

