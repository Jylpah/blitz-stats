#!/usr/bin/env python3

# Script fetch Blitz player stats and tank stats

from curses import meta
from datetime import datetime
from typing import Optional
from pyutils.multilevelformatter import MultilevelFormatter, set_mlevel_logging
from configparser import ConfigParser
import models
import accounts as acc
import logging
import argparse

import sys
import json
import os
import inspect
import pprint
import aiohttp
import asyncio
import aiofiles
import aioconsole
import motor.motor_asyncio
import ssl
import lxml
import re

import time
import xmltodict
import collections
import pymongo

import blitzutils as bu
import utils as su
from bs4 import BeautifulSoup
from blitzutils import BlitzStars, WG, WoTinspector, RecordLogger

logging.getLogger("asyncio").setLevel(logging.DEBUG)
logger = logging.getLogger()
error 	= logging.error
verbose_std	= logging.warning
verbose	= logging.info
debug	= logging.debug

# main() -------------------------------------------------------------

async def main(argv: list[str]):
	# set the directory for the script
	global logger, error, debug, verbose, verbose_std,db, wi, bs, MAX_PAGES

	os.chdir(os.path.dirname(sys.argv[0]))
	
	# Default params
	WG_APP_ID 	= 'cd770f38988839d7ab858d1cbe54bdd0'
	CONFIG 		= 'blitz-stats.ini'	
	LOG 		= 'blitz-stats.log'
	THREADS 	= 20    # make it module specific?
	BACKEND 	: Optional[str] = None

	WG_RATE_LIMIT : float = 10

	config : Optional[ConfigParser] = None

	parser = argparse.ArgumentParser(description='Fetch and manage WoT Blitz stats', add_help=False)
	arggroup_verbosity = parser.add_mutually_exclusive_group()
	arggroup_verbosity.add_argument('-d', '--debug',dest='LOG_LEVEL', action='store_const', const=logging.DEBUG,  
									help='Debug mode')
	arggroup_verbosity.add_argument('--verbose', dest='LOG_LEVEL', action='store_const', const=logging.INFO,
									help='Verbose mode')
	arggroup_verbosity.add_argument('--silent', dest='LOG_LEVEL', action='store_const', const=logging.CRITICAL,
									help='Silent mode')
	parser.add_argument('--log', type=str, nargs='?', default=None, const=f"{LOG}_{su.get_datestr()}", 
						help='Enable file logging')
	parser.add_argument('--config', type=str, default=CONFIG, 
						help='Read config from CONFIG')
	parser.set_defaults(LOG_LEVEL=logging.WARNING)

	args, argv = parser.parse_known_args()


	try:
		if args.config is not None and os.path.isfile(args.config):
			config = ConfigParser()
			config.read(args.config)
			if 'DEFAULT' in config.sections():
				configDef = config['DEFAULT']
				BACKEND = configDef.get('backend', None)
			if 'MONGO' in config.sections():
				configMongo = config['MONGO']				
				M_SERVER 	: Optional[str] = configMongo.get('server', None)
				M_PORT 		: Optional[int] = configMongo.getint('port', None)
				M_TLS 		: bool 			= configMongo.getboolean('tls', False)
				M_CERT_REQ 	: int  			= configMongo.getint('tls_req', 0)
				M_AUTH 		: Optional[str] = configMongo.get('auth', None)
				M_NAME 		: Optional[str] = configMongo.get('name', None)
				M_USER 		: Optional[str] = configMongo.get('user', None)
				M_PASSWD 	: Optional[str] = configMongo.get('password', None)
				M_CERT		: Optional[str] = configMongo.get('cert', None)
				M_CA		: Optional[str] = configMongo.get('ca', None)
			if 'WG' in config.sections():
				configWG 		= config['WG']
				WG_APP_ID		= configWG.get('wg_app_id', WG_APP_ID)
				WG_RATE_LIMIT	= configWG.getfloat('rate_limit', WG_RATE_LIMIT)
			
		# setup logging
		logger.setLevel(args.LOG_LEVEL)
		logger_conf: dict[int, str] = { 
			logging.INFO: 		'%(message)s',
			logging.WARNING: 	'%(message)s',
			logging.ERROR: 		'%(levelname)s: %(message)s'
		}
		set_mlevel_logging(logger, fmts=logger_conf, 
							fmt='%(levelname)s: %(funcName)s(): %(message)s', 
							log_file=args.log)
		error 		= logger.error
		verbose_std	= logger.warning
		verbose		= logger.info
		debug		= logger.debug

		debug(f"Args parsed: {str(args)}")
		debug(f"Args not parsed yet: {str(argv)}")

		# Parse command args
		parser.add_argument('-h', '--help', action='store_true',  
							help='Show help')
		parser.add_argument('--backend', type=str, choices=['mongodb', 'postgresql', 'files'], 
							default=BACKEND, help='Choose backend to use')
		parser.add_argument('--force', action='store_true', default=False, help='Force action')
		## MOVE TO MODULE OPTIONS?? 
		parser.add_argument('--threads', type=int, default=THREADS, 
							help='Set number of asynchronous threads')
		parser.add_argument('--wg-app-id', type=str, default=WG_APP_ID,
							help='Set WG APP ID')
		cmd_parsers = parser.add_subparsers(dest='main_cmd', 
												title='main commands',
												description='valid subcommands',
												metavar='')

		accounts_parser = cmd_parsers.add_parser('accounts', aliases=['acc'], help='accounts help')
		stats_parser = cmd_parsers.add_parser('stats', help='stats help')
		tankopedia_parser = cmd_parsers.add_parser('tankopedia', help='tankopedia help')
		setup_parser = cmd_parsers.add_parser('setup', help='setup help')
		
		if not acc.add_args_accounts(accounts_parser, config):
			raise Exception("Failed to define argument parser for: accounts")

		args = parser.parse_args(args=argv)
		if args.help:
			parser.print_help()
		debug(str(args))
	
		if args.main_cmd == 'accounts':				
			# how to handle errors / stats? 
			await acc.cmd_accounts(args, config)				
			
		elif args.main_cmd == 'stats':
			pass
		elif args.main_cmd == 'tankopedia':
			pass
		elif args.main_cmd == 'setup':
			pass

	except Exception as err:
		error(str(err))
	


### main()
if __name__ == "__main__":
   #asyncio.run(main(sys.argv[1:]), debug=True)
   asyncio.run(main(sys.argv[1:]))