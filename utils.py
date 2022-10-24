import pymongo
import motor.motor_asyncio
import logging
from bson import ObjectId
import blitzutils as bu
from datetime import datetime, timedelta
from typing import Optional

error 	= logging.error
verbose_std	= logging.warning
verbose	= logging.info
debug	= logging.debug

def get_datestr(_datetime: datetime = datetime.now()) -> str:
	return _datetime.strftime('%Y%m%d_%H%M')

############### OLD #################################

#####################################################################
#                                                                   #
# Constants                                                         #
#                                                                   #
#####################################################################

N_WORKERS = 4

FILE_CONFIG = 'blitzstats.ini'

MODE_TANK_STATS         = 'tank_stats'
MODE_PLAYER_STATS       = 'player_stats'
MODE_PLAYER_ACHIEVEMENTS= 'player_achievements'
MODE_TANKOPEDIA			= 'tankopedia'
MODE_ACCOUNTS           = 'accounts'
MODE_BS_PLAYER_STATS 	= 'player_stats_BS'
MODE_BS_TANK_STATS		= 'tank_stats_BS'
#MODE_ARCHIVE            = '_archive'
MODE_LATEST             = '_latest'

#DB_STR_ARCHIVE          = '_Archive'
DB_STR_LATEST          = '_Latest'

DB_C_ACCOUNTS   		= 'WG_Accounts'
DB_C_UPDATES            = 'WG_Releases'
DB_C_PLAYER_STATS		= 'WG_PlayerStats'
DB_C_PLAYER_ACHIVEMENTS	= 'WG_PlayerAchievements'
DB_C_TANK_STATS     	= 'WG_TankStats'
DB_C_STATS_2_DEL        = 'Stats2Delete'
DB_C_BS_PLAYER_STATS   	= 'BS_PlayerStats'
DB_C_BS_TANK_STATS     	= 'BS_PlayerTankStats'
DB_C_TANKOPEDIA     			= 'Tankopedia'
DB_C_TANK_STR			= 'WG_TankStrs'
DB_C_ERROR_LOG			= 'ErrorLog'
DB_C_UPDATE_LOG			= 'UpdateLog'
DB_C_ACCOUNT_LOG        = 'AccountLog'
DB_C_REPLAYS            = 'Replays'

DB_C = {    MODE_TANK_STATS             : DB_C_TANK_STATS, 
            MODE_PLAYER_STATS           : DB_C_PLAYER_STATS,
            MODE_PLAYER_ACHIEVEMENTS    : DB_C_PLAYER_ACHIVEMENTS 
        }
modes_tmp = list(DB_C.keys())
for mode in modes_tmp:
    DB_C[mode + MODE_LATEST] = DB_C[mode] + DB_STR_LATEST

DB_C_LATEST = dict()
for mode in DB_C:
    DB_C_LATEST[mode] = DB_C[mode] + DB_STR_LATEST

STR_MODES = {    
    MODE_TANK_STATS             : 'Tank Stats', 
    MODE_PLAYER_STATS           : 'Player Stats',
    MODE_PLAYER_ACHIEVEMENTS    : 'Player Achievements', 
    MODE_TANKOPEDIA             : 'Tankopedia',
    MODE_ACCOUNTS               : 'Accounts',
    MODE_BS_TANK_STATS          : 'Blitzstart Tank Stats',
    MODE_BS_PLAYER_STATS        : 'Blitzstars Player Stats'
}
modes = list(STR_MODES.keys())
for stat_type in modes:
    STR_MODES[stat_type + MODE_LATEST] = STR_MODES[stat_type] + ' (Archive)'


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
DEFAULT_BATCH   = 100

TODAY               = datetime.utcnow().date()
DEFAULT_DAYS_DELTA  = timedelta(days=90)
DATE_DELTA          = timedelta(days=7)
STATS_START_DATE    = datetime(2014,1,1)


#####################################################################
#                                                                   #
# utils                                                             #
#                                                                   #
#####################################################################


def def_value_zero():
    return 0


def get_mode_str(stat_type: str, archive : bool = False) -> Optional[str]:
    try:
        ret = STR_MODES[stat_type]
        if archive == True:
            return ret + ' (Archive)'
        else:
            return ret
    except Exception as err:
        error(str(err))
    return None


async def init_db_indices(db: motor.motor_asyncio.AsyncIOMotorDatabase):
    """Create DB indices"""
    try:
        verbose_std('Adding index: ' + DB_C_BS_PLAYER_STATS + ': account_id: 1, last_battle_time: -1')
        await db[DB_C_BS_PLAYER_STATS].create_index([('account_id', pymongo.ASCENDING), ('last_battle_time', pymongo.DESCENDING)], background=True)
        
        verbose_std('Adding index: ' + DB_C_BS_TANK_STATS + ': account_id: 1, tank_id: 1, last_battle_time: -1')
        await db[DB_C_BS_TANK_STATS].create_index([('account_id', pymongo.ASCENDING), ('tank_id', pymongo.ASCENDING), ('last_battle_time', pymongo.DESCENDING)], background=True)
        
        verbose_std('Adding index: ' + DB_C_BS_TANK_STATS + ': tank_id: 1, last_battle_time: -1')
        await db[DB_C_BS_TANK_STATS].create_index([('tank_id', pymongo.ASCENDING), ('last_battle_time', pymongo.DESCENDING)], background=True)

        # Prune list
        verbose_std('Adding index: ' + DB_C_STATS_2_DEL + ': type: 1, id: 1')
        await db[DB_C_STATS_2_DEL].create_index([('type', pymongo.ASCENDING), ('id', pymongo.ASCENDING)], background=True)

        # Accounts
        verbose_std('Adding index: ' + DB_C_ACCOUNTS + ': last_battle_time: -1')
        await db[DB_C_ACCOUNTS].create_index([ ('last_battle_time', pymongo.DESCENDING) ], background=True)

        # Replays
        verbose_std('Adding index: ' + DB_C_REPLAYS + ': data.summary.battle_start_timestamp: 1, data.summary.vehicle_tie: 1')
        await db[DB_C_REPLAYS].create_index([('data.summary.battle_start_timestamp', pymongo.ASCENDING), ('data.summary.vehicle_tier', pymongo.ASCENDING)], background=True)	

        ## WG Tank Stats
        for db_collection in [ DB_C_TANK_STATS , DB_C_TANK_STATS + DB_STR_LATEST]:
            
            verbose_std('Adding index: ' + db_collection + ': tank_id: 1, account_id: 1, last_battle_time: -1')
            await db[db_collection].create_index([('tank_id', pymongo.ASCENDING), ('account_id', pymongo.ASCENDING), ('last_battle_time', pymongo.DESCENDING)], background=True, unique=True)
            
            verbose_std('Adding index: ' + db_collection + ': tank_id: 1, last_battle_time: -1')
            await db[db_collection].create_index([('tank_id', pymongo.ASCENDING), ('last_battle_time', pymongo.DESCENDING)], background=True)
            
            verbose_std('Adding index: ' + db_collection + ': account_id: 1, last_battle_time: -1')
            await db[db_collection].create_index([('account_id', pymongo.ASCENDING), ('last_battle_time', pymongo.DESCENDING)], background=True)

            # verbose_std('Adding index: ' + db_collection + ': tank_id: 1, ' + FIELD_NEW_STATS + ': 1, account_id: 1')
            # await db[db_collection].create_index([ ('tank_id', pymongo.ASCENDING), (FIELD_NEW_STATS, pymongo.ASCENDING), ('account_id', pymongo.ASCENDING)], partialFilterExpression={FIELD_NEW_STATS:  {'$exists': True}}, background=True)

        ## WG Player Achievements
        for db_collection in [ DB_C_PLAYER_ACHIVEMENTS , DB_C_PLAYER_ACHIVEMENTS + DB_STR_LATEST]:
            verbose_std('Adding index: ' + db_collection + ': account_id: 1, updated: -1')
            await db[db_collection].create_index([('account_id', pymongo.ASCENDING), ('updated', pymongo.DESCENDING)], background=True, unique=True)

            # verbose_std('Adding index: ' + db_collection + ': ' + FIELD_NEW_STATS + ' : -1 (partial)')
            # await db[db_collection].create_index([(FIELD_NEW_STATS, pymongo.DESCENDING)], partialFilterExpression={FIELD_NEW_STATS:  {'$exists': True}}, background=True)

        # Tankopedia
        verbose_std('Adding index: ' + DB_C_TANKOPEDIA + ': tank_id: 1, tier: -1')
        await db[DB_C_TANKOPEDIA].create_index([('tank_id', pymongo.ASCENDING), ('tier', pymongo.DESCENDING)], background=True)
        
        verbose_std('Adding index: ' + DB_C_TANKOPEDIA + ': name: TEXT')
        await db[DB_C_TANKOPEDIA].create_index([('name', pymongo.TEXT)], background=True)

        # Update Log
        verbose_std('Adding index: ' + DB_C_UPDATE_LOG + ': mode: 1, account_id: 1, updated: -1')
        await db[DB_C_ACCOUNT_LOG].create_index([ ('mode', pymongo.ASCENDING), ('account_id', pymongo.ASCENDING), ('updated', pymongo.DESCENDING)], background=True)
        

        # Account Log
        verbose_std('Adding index: ' + DB_C_ACCOUNT_LOG + ': mode: 1, account_id: 1, updated: -1')
        await db[DB_C_ACCOUNT_LOG].create_index([ ('mode', pymongo.ASCENDING), ('account_id', pymongo.ASCENDING), ('updated', pymongo.DESCENDING)], background=True)
        
        # Error log
        verbose_std('Adding index: ' + DB_C_ERROR_LOG + ': type: 1, account_id: 1')
        await db[DB_C_ERROR_LOG].create_index([ ('type', pymongo.ASCENDING), ('account_id', pymongo.ASCENDING)], background=True)


        return True
    except Exception as err:
        error(f"Unexpected Exception: {str(err)}")
    return False


async def update_log(db : motor.motor_asyncio.AsyncIOMotorDatabase, action: str, stat_type : str, update: Optional[str] = None):
	"""Log successfully finished status update"""
	try:
		dbc = db[DB_C_UPDATE_LOG]
		await dbc.insert_one( { 'action': action, 'stat_type': stat_type, 'update': update,  'updated': bu.NOW() } )
	except Exception as err:
		error(f"Unexpected Exception: {str(err)}")
		return False
	return True


def mk_id(account_id: int, last_battle_time: int, tank_id: int = 0) -> ObjectId:
	return ObjectId(hex(account_id)[2:].zfill(10) + hex(tank_id)[2:].zfill(6) + hex(last_battle_time)[2:].zfill(8))