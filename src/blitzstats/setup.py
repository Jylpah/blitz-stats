from argparse import ArgumentParser, Namespace
from configparser import ConfigParser
from typing import Optional
import logging

from .backend import Backend, BSTableType

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


def add_args(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		setup_parsers = parser.add_subparsers(dest='setup_cmd', 	
												title='setup commands',
												description='valid commands',
												help='setup help',
												metavar='init')
		setup_parsers.required = True
		init_parser = setup_parsers.add_parser('init', help="setup init help")
		if not add_args_init(init_parser, config=config):
			raise Exception("Failed to define argument parser for: setup init")		
				
		return True
	except Exception as err:
		error(f'{err}')
	return False


## -

def add_args_init(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		tables : list[str] = ['all' ] + sorted([ tt.name for tt in BSTableType ])
		parser.add_argument('setup_init_tables', nargs='*',
							default='all', 
							choices=tables, 
		 					metavar='TABLE [TABLE...]',
		 					help='TABLE(S) to initialize: ' + ", ".join(tables))
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
		if args.setup_cmd == 'init':
			debug('setup init')
			return await cmd_init(db, args)		
		else:
			error('setup: unknown or missing subcommand')

	except Exception as err:
		error(f'{err}')
	return False


async def cmd_init(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		tables: list[str] = args.setup_init_tables

		if 'all' in tables:
			tables = [ tt.name for tt in BSTableType ]
		await db.init(tables=tables)
		return True 
	except Exception as err:
		error(f'{err}')
	return False
