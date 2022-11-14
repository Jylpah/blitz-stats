from argparse import ArgumentParser, Namespace
from configparser import ConfigParser
from typing import Optional, cast
import logging
from asyncio import create_task, gather, Queue, CancelledError, Task, sleep
from alive_progress import alive_bar		# type: ignore

from backend import Backend, OptAccountsInactive
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

###########################################
# 
# add_args_ functions  
#
###########################################

def add_args_tank_stats(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		
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
				
		return True
	except Exception as err:
		error(str(err))
	return False


def add_args_tank_stats_update(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		parser.add_argument('--region', type=str, choices=['any'] + [ r.name for r in Region ], 
							default=Region.API.name, help='Filter by region (default is API = eu + com + asia)')
		parser.add_argument('--sample', type=float, default=0, metavar='SAMPLE',
							help='Update tank stats for SAMPLE of accounts. If 0 < SAMPLE < 1, SAMPLE defines a % of users')
		parser.add_argument('--distributed', '--dist',type=str, dest='distributed', metavar='I:N', 
							default=None, help='Distributed update for accounts: id % N == I')
		parser.add_argument('--check-invalid', dest='chk_invalid', action='store_true', default=False, help='Re-check invalid accounts')
		parser.add_argument('--check-inactive', dest='chk_inactive', action='store_true', default=False, help='Re-check inactive accounts')
		return True
	except Exception as err:
		error(str(err))
	return False

def add_args_tank_stats_prune(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	return True


def add_args_tank_stats_import(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	return True


def add_args_tank_stats_export(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	return True


###########################################
# 
# cmd_ functions  
#
###########################################

async def cmd_tank_stats(db: Backend, args : Namespace) -> bool:
	try:
		debug('tank-stats')

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
	try:
		return True
	except Exception as err:
		error(str(err))
	return False