#!/usr/bin/env python3

# Script fetch Blitz player stats and tank stats

from datetime import datetime
import sys
import argparse
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
import logging
import time
import xmltodict
import collections
import pymongo
import configparser
import blitzutils as bu
import blitzstatsutils as su
from bs4 import BeautifulSoup
from blitzutils import BlitzStars, WG, WoTinspector, RecordLogger

logging.getLogger("asyncio").setLevel(logging.DEBUG)

# main() -------------------------------------------------------------

async def main(argv):
	# set the directory for the script
	os.chdir(os.path.dirname(sys.argv[0]))

	global db, wi, bs, MAX_PAGES

	# Default params
	WG_APP_ID = 'cd770f38988839d7ab858d1cbe54bdd0'
	CONFIG = 'blitz-stats.ini'	
	LOG = 'blitz-stats.log'
	THREADS = 20
	BACKEND = None

	parser = argparse.ArgumentParser(description='Fetch and manage WoT Blitz stats')
	arggroup_verbosity = parser.add_mutually_exclusive_group()
	arggroup_verbosity.add_argument('-d', '--debug', action='store_true', default=False, 
									help='Debug mode')
	arggroup_verbosity.add_argument('-v', '--verbose', action='store_true', default=False, 
									help='Verbose mode')
	arggroup_verbosity.add_argument('-s', '--silent', action='store_true', default=False,
									help='Silent mode')

	parser.add_argument('-l', '--log', type=str, nargs='?', default=None, 
						const=LOG, help='Enable file logging')
	parser.add_argument('--force', action='store_true', default=False, help='Force action')
	parser.add_argument('--threads', type=int, default=THREADS, 
						help='Set number of asynchronous threads')
	parser.add_argument('--backend', type=str, choices=['mongodb', 'postgresql', 'files'], 
						default=BACKEND, help='Choose backend to use')

	args, argv = parser.parse_known_args()

	print("args: " + str(args))
	print("argv: " + str(argv))
	 
 	# MAX_RETRIES = 3
	# CACHE_VALID = 24*3600*5   # 5 days
	# SLEEP = 1
	# REPLAY_N = 0
    # db = None
    # wi = None
    # bs = None
    # WI_STOP_SPIDER = False
    # WI_old_replay_N = 0
    # WI_old_replay_limit = 25
	# MAX_PAGES 	= 500
	# DB_SERVER 	= 'localhost'
	# DB_PORT 	= 27017
	# DB_TLS		= False
	# DB_CERT_REQ = False
	# DB_AUTH 	= 'admin'
	# DB_NAME 	= 'BlitzStats'
	# DB_USER		= 'mongouser'
	# DB_PASSWD 	= None
	# DB_CERT 	= None
	# DB_CA 		= None


# MOVE TO UTILS
def get_date_suffix(_datetime: datetime = datetime.now()) -> str:
	return _datetime.strftime(format='%Y%m%d_%H%M')

### main()
if __name__ == "__main__":
   #asyncio.run(main(sys.argv[1:]), debug=True)
   asyncio.run(main(sys.argv[1:]))