#!/usr/bin/env python3

# Script fetch Blitz player stats and tank stats

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
	LOG = 'blitz-stats'
	THREADS = 20
	BACKEND = None



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