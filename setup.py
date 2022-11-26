from argparse import ArgumentParser, Namespace
from configparser import ConfigParser
from typing import Optional, cast, Iterable
import logging
from asyncio import create_task, gather, Queue, CancelledError, Task
from aiohttp import ClientResponse

from backend import Backend

logger = logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug

###########################################
# 
# add_args_accouts functions  
#
###########################################


def add_args_setup(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		setup_parsers = parser.add_subparsers(dest='setup_cmd', 	
												title='setup commands',
												description='valid commands',
												help='setup help',
												metavar='init')
		setup_parsers.required = True
		init_parser = setup_parsers.add_parser('init', help="setup init help")
		if not add_args_setup_init(init_parser, config=config):
			raise Exception("Failed to define argument parser for: setup init")		
				
		return True
	except Exception as err:
		error(f'add_args_setup(): {err}')
	return False


## -

def add_args_setup_init(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		# parser.add_argument('backend', type=str, default=None, choices=Backend.list_available(), 
		# 					dest='setup_init_backend', metavar='BACKEND',
		# 					help='BACKEND to initialize')
		return True	
	except Exception as err:
		error(f'add_args_replays_export() : {err}')
	return False

###########################################
# 
# cmd_accouts functions  
#
###########################################

async def cmd_setup(db: Backend, args : Namespace) -> bool:
	
	try:
		debug('starting')		
		if args.setup_cmd == 'init':
			debug('setup init')
			return await cmd_setup_init(db, args)		
		else:
			error('setup: unknown or missing subcommand')

	except Exception as err:
		error(str(err))
	return False


async def cmd_setup_init(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		await db.init()
		return True 
	except Exception as err:
		error(str(err))
	return False
