#!/usr/bin/env python3

# Script fetch Blitz player stats and tank stats

from datetime import datetime
from typing import Optional
from backend import Backend
from mongobackend import MongoBackend
from pyutils import MultilevelFormatter
from configparser import ConfigParser
import logging
from argparse import ArgumentParser
import sys
from os import chdir, linesep
from os.path import isfile, dirname
from asyncio import run
from yappi import start, stop, get_func_stats, set_clock_type, COLUMNS_FUNCSTATS 	# type: ignore

import accounts 
import replays 
import releases 
import tank_stats 
import player_achievements 
import setup 

# import blitzutils as bu
# import utils as su

# from blitzutils import BlitzStars, WG, WoTinspector, RecordLogger

logging.getLogger("asyncio").setLevel(logging.DEBUG)
logger = logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug

# Utils 
def get_datestr(_datetime: datetime = datetime.now()) -> str:
	return _datetime.strftime('%Y%m%d_%H%M')


# main() -------------------------------------------------------------

async def main(argv: list[str]):
	# set the directory for the script
	global logger, error, debug, verbose, message, db, wi, bs, MAX_PAGES

	chdir(dirname(sys.argv[0]))
	
	# Default params
	WG_APP_ID 	= 'wg-app-id-missing'
	CONFIG 		= 'blitzstats.ini'	
	LOG 		= 'blitzstats.log'
	# THREADS 	= 20    # make it module specific?
	BACKEND 	: Optional[str] = None
	WG_RATE_LIMIT : float = 10

	config : Optional[ConfigParser] = None

	parser = ArgumentParser(description='Fetch and manage WoT Blitz stats', add_help=False)
	arggroup_verbosity = parser.add_mutually_exclusive_group()
	arggroup_verbosity.add_argument('--debug', '-d', dest='LOG_LEVEL', action='store_const', 
									const=logging.DEBUG, help='Debug mode')
	arggroup_verbosity.add_argument('--verbose', '-v', dest='LOG_LEVEL', action='store_const', 
									const=logging.INFO, help='Verbose mode')
	arggroup_verbosity.add_argument('--silent', '-s', dest='LOG_LEVEL', action='store_const', 
									const=logging.CRITICAL, help='Silent mode')
	parser.add_argument('--log', type=str, nargs='?', default=None, const=f"{LOG}_{get_datestr()}", 
						help='Enable file logging')	
	parser.add_argument('--config', type=str, default=CONFIG, metavar='CONFIG', help='Read config from CONFIG')
	parser.set_defaults(LOG_LEVEL=logging.WARNING)

	args, argv = parser.parse_known_args()

	try:
		# setup logging
		logger.setLevel(args.LOG_LEVEL)
		logger_conf: dict[int, str] = { 
			logging.INFO: 		'%(message)s',
			logging.WARNING: 	'%(message)s',
			# logging.ERROR: 		'%(levelname)s: %(message)s'
		}
		MultilevelFormatter.setLevels(logger, fmts=logger_conf, 
							fmt='%(levelname)s: %(funcName)s(): %(message)s', 
							log_file=args.log)
		error 		= logger.error
		message		= logger.warning
		verbose		= logger.info
		debug		= logger.debug

		if args.config is not None and isfile(args.config):
			debug(f'Reading config from {args.config}')
			config = ConfigParser()
			config.read(args.config)
			if 'GENERAL' in config.sections():
				debug('Reading config section GENERAL')
				configDef = config['GENERAL']
				BACKEND = configDef.get('backend', None)
			## Is this really needed here? 
			# if 'WG' in config.sections():
			# 	configWG 		= config['WG']
			# 	WG_APP_ID		= configWG.get('wg_app_id', WG_APP_ID)
			# 	WG_RATE_LIMIT	= configWG.getfloat('rate_limit', WG_RATE_LIMIT)
		else:
			debug("No config file found")		

		debug(f"Args parsed: {str(args)}")
		debug(f"Args not parsed yet: {str(argv)}")

		# Parse command args
		parser.add_argument('-h', '--help', action='store_true',  
							help='Show help')
		parser.add_argument('--profile', type=int, default=0, metavar='N',
							help='Profile performance for N slowest function calls')
		parser.add_argument('--backend', type=str, choices=Backend.list_available(), 
							default=BACKEND, help='Choose backend to use')		

		cmd_parsers = parser.add_subparsers(dest='main_cmd', 
											title='main commands',
											description='valid subcommands',
											metavar='accounts | tank-stats | player-achievements | replays | tankopedia | releases | setup')
		cmd_parsers.required = True

		accounts_parser 			= cmd_parsers.add_parser('accounts', aliases=['ac'], help='accounts help')
		tank_stats_parser 			= cmd_parsers.add_parser('tank-stats',aliases=['ts'],  help='tank-stats help')
		player_achievements_parser 	= cmd_parsers.add_parser('player-achievements', aliases=['pa'], help='player-achievements help')
		replays_parser 				= cmd_parsers.add_parser('replays', help='replays help')
		tankopedia_parser 			= cmd_parsers.add_parser('tankopedia', help='tankopedia help')
		releases_parser 			= cmd_parsers.add_parser('releases', help='releases help')
		setup_parser 				= cmd_parsers.add_parser('setup', help='setup help')
		
		if not accounts.add_args(accounts_parser, config):
			raise Exception("Failed to define argument parser for: accounts")
		if not replays.add_args(replays_parser, config):
			raise Exception("Failed to define argument parser for: replays")
		if not releases.add_args(releases_parser, config):
			raise Exception("Failed to define argument parser for: releases")
		if not setup.add_args_setup(setup_parser, config):
			raise Exception("Failed to define argument parser for: setup")
		if not tank_stats.add_args(tank_stats_parser, config):
			raise Exception("Failed to define argument parser for: tank-stats")
		if not player_achievements.add_args(player_achievements_parser, config):
			raise Exception("Failed to define argument parser for: player-achievements")


		debug('parsing full args')
		args = parser.parse_args(args=argv)
		if args.help:
			parser.print_help()
		debug('arguments given:')
		debug(str(args))

		backend : Backend | None  = Backend.create(args.backend, config=config)
		assert backend is not None, 'Could not initialize backend'

		if args.profile > 0:
			print('Starting profiling...')
			set_clock_type('cpu')
			start(builtins=True)

		if args.main_cmd == 'accounts':			
			await accounts.cmd(backend, args)
		elif args.main_cmd == 'tank-stats':
			await tank_stats.cmd(backend, args)
		elif args.main_cmd == 'player-achievements':
			await player_achievements.cmd(backend, args)
		elif args.main_cmd == 'replays':
			await replays.cmd(backend, args)
		elif args.main_cmd == 'releases':
			await releases.cmd(backend, args)
		elif args.main_cmd == 'tankopedia':
			raise NotImplementedError
		elif args.main_cmd == 'setup':
			await setup.cmd_setup(backend, args)
		else:
			parser.print_help()

		if args.profile > 0:
			print('Stopping profiling')
			stop()
			stats = get_func_stats().sort(sort_type='ttot', sort_order='desc')			
			print_all(stats, sys.stdout, args.profile)
			
	except Exception as err:
		error(f'{err}')
	

def print_all(stats, out, limit: int | None =None) -> None:
	if stats.empty():
		return
	sizes = [150, 12, 8, 8, 8]
	columns = dict(zip(range(len(COLUMNS_FUNCSTATS)), zip(COLUMNS_FUNCSTATS, sizes)))
	show_stats = stats
	if limit:
		show_stats = stats[:limit]
	out.write(linesep)
	for stat in show_stats:
		stat._print(out, columns) 

########################################################
# 
# main() entry
#
########################################################

if __name__ == "__main__":
	#asyncio.run(main(sys.argv[1:]), debug=True)
	run(main(sys.argv[1:]))

