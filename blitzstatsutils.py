#!/usr/bin/env python3

import sys, os, json, time,  base64, urllib, inspect, hashlib, re, string, random
import asyncio, aiofiles, aiohttp, aiosqlite, lxml, collections,  datetime, logging
import blitzutils as bu

#####################################################################
#                                                                   #
# Constants                                                         #
#                                                                   #
#####################################################################

N_WORKERS = 4

logging.getLogger("asyncio").setLevel(logging.DEBUG)

FILE_CONFIG = 'blitzstats.ini'

MODE_TANK_STATS         = 'tank_stats'
MODE_PLAYER_STATS       = 'player_stats'
MODE_PLAYER_ACHIEVEMENTS= 'player_achievements'
MODE_TANKOPEDIA			= 'tankopedia'
MODE_BS_PLAYER_STATS 	= 'player_stats_BS'
MODE_BS_TANK_STATS		= 'tank_stats_BS'

MODE_ARCHIVE            = '_archive'
DB_STR_ARCHIVE          = '_Archive'
DB_C_ACCOUNTS   		= 'WG_Accounts'
DB_C_UPDATES            = 'WG_Releases'
DB_C_PLAYER_STATS		= 'WG_PlayerStats'
DB_C_PLAYER_ACHIVEMENTS	= 'WG_PlayerAchievements'
DB_C_TANK_STATS     	= 'WG_TankStats'
DB_C_STATS_2_DEL        = 'Stats2Delete'
DB_C_BS_PLAYER_STATS   	= 'BS_PlayerStats'
DB_C_BS_TANK_STATS     	= 'BS_PlayerTankStats'
DB_C_TANKS     			= 'Tankopedia'
DB_C_TANK_STR			= 'WG_TankStrs'
DB_C_ERROR_LOG			= 'ErrorLog'
DB_C_UPDATE_LOG			= 'UpdateLog'

DB_C = {    MODE_TANK_STATS             : DB_C_TANK_STATS, 
            MODE_PLAYER_STATS           : DB_C_PLAYER_STATS,
            MODE_PLAYER_ACHIEVEMENTS    : DB_C_PLAYER_ACHIVEMENTS 
        }

DB_C_ARCHIVE = dict()
for mode in DB_C:
    DB_C_ARCHIVE[mode] = DB_C[mode] + DB_STR_ARCHIVE

STR_MODES = {    
    MODE_TANK_STATS             : 'Tank Stats', 
    MODE_PLAYER_STATS           : 'Player Stats',
    MODE_PLAYER_ACHIEVEMENTS    : 'Player Achievements', 
    MODE_TANKOPEDIA             : 'Tankopedia',
    MODE_BS_TANK_STATS          : 'Blitzstart Tank Stats',
    MODE_BS_PLAYER_STATS        : 'Blitzstars Player Stats'
}
modes = list(STR_MODES.keys())
for stat_type in modes:
    STR_MODES[stat_type + MODE_ARCHIVE] = STR_MODES[stat_type] + ' (Archive)'


UPDATE_FIELD = { MODE_TANK_STATS			: 'updated_WGtankstats',
				 MODE_PLAYER_STATS			: 'updated_WGplayerstats',
				 MODE_PLAYER_ACHIEVEMENTS	: 'updated_WGplayerachievements',				 
				 MODE_BS_PLAYER_STATS 		: 'updated_BSplayerstats',
				 MODE_BS_TANK_STATS			: 'updated_BStankstats',
                 'default'					: 'updated'
				}

FIELD_NEW_STATS   = '_updated'


UPDATE_ALL = 'ALL'

CACHE_VALID     = 7*24*3600   # 7 days
DEFAULT_SAMPLE  = 1000
QUEUE_LEN       = 10000
DEFAULT_BATCH   = 1000

TODAY               = datetime.datetime.utcnow().date()
DEFAULT_DAYS_DELTA  = datetime.timedelta(days=90)
DATE_DELTA          = datetime.timedelta(days=7)
STATS_START_DATE    = datetime.datetime(2014,1,1)


#####################################################################
#                                                                   #
# utils                                                             #
#                                                                   #
#####################################################################


def def_value_zero():
    return 0


def get_mode_str(stat_type: str, archive : bool = False) -> str:
    try:
        ret = STR_MODES[stat_type]
        if archive == True:
            return ret + ' (Archive)'
        else:
            return ret
    except Exception as err:
        bu.error(exception=err)