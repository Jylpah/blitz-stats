#!/usr/bin/env python3.8

# Script Prune stats from the DB per release 

import sys, os, argparse, datetime, json, inspect, pprint, aiohttp, asyncio, aiofiles
import aioconsole, re, logging, time, xmltodict, collections, pymongo, motor.motor_asyncio
import ssl, configparser
from datetime import date
import blitzutils as bu
from blitzutils import BlitzStars

N_WORKERS = 5
MAX_RETRIES = 3
logging.getLogger("asyncio").setLevel(logging.DEBUG)

FILE_CONFIG = 'blitzstats.ini'

DB_C_ACCOUNTS   		= 'WG_Accounts'
DB_C_UPDATES            = 'WG_Releases'
DB_C_PLAYER_STATS		= 'WG_PlayerStats'
DB_C_PLAYER_ACHIVEMENTS	= 'WG_PlayerAchievements'
DB_C_TANK_STATS     	= 'WG_TankStats'
DB_C_TANK_STATS_DEL     = 'WG_TankStats_duplicates'
DB_C_BS_PLAYER_STATS   	= 'BS_PlayerStats'
DB_C_BS_TANK_STATS     	= 'BS_PlayerTankStats'
DB_C_TANKS     			= 'Tankopedia'
DB_C_TANK_STR			= 'WG_TankStrs'
DB_C_ERROR_LOG			= 'ErrorLog'
DB_C_UPDATE_LOG			= 'UpdateLog'

CACHE_VALID = 24*3600*7   # 7 days

bs = None

TODAY = datetime.datetime.utcnow().date()
DEFAULT_DAYS_DELTA = datetime.timedelta(days=90)
DATE_DELTA = datetime.timedelta(days=7)
STATS_START_DATE = datetime.datetime(2014,1,1)

STATS_EXPORTED = 0

# main() -------------------------------------------------------------


async def main(argv):
    # set the directory for the script
    os.chdir(os.path.dirname(sys.argv[0]))

    parser = argparse.ArgumentParser(description='Prune stats from the DB by update')
    parser.add_argument('--mode', default='tank_stats', choices=['player_stats', 'tank_stats'], help='Select type of stats to export')
    parser.add_argument( '-f', '--force', 	action='store_true', default=False, help='Force changes i.e. DELETE DATA')
    parser.add_argument('updates', metavar='X.Y [Z.D ...]', type=str, nargs='+', help='List of updates to prune')
    arggroup = parser.add_mutually_exclusive_group()
    arggroup.add_argument( '-d', '--debug', 	action='store_true', default=False, help='Debug mode')
    arggroup.add_argument( '-v', '--verbose', 	action='store_true', default=False, help='Verbose mode')
    arggroup.add_argument( '-s', '--silent', 	action='store_true', default=False, help='Silent mode')
    
    args = parser.parse_args()
    bu.set_log_level(args.silent, args.verbose, args.debug)
    bu.set_progress_step(1000)
    
    try:
		## Read config
        config = configparser.ConfigParser()
        config.read(FILE_CONFIG)
        configDB    = config['DATABASE']
        DB_SERVER   = configDB.get('db_server', 'localhost')
        DB_PORT     = configDB.getint('db_port', 27017)
        DB_SSL      = configDB.getboolean('db_ssl', False)
        DB_CERT_REQ = configDB.getint('db_ssl_req', ssl.CERT_NONE)
        DB_AUTH     = configDB.get('db_auth', 'admin')
        DB_NAME     = configDB.get('db_name', 'BlitzStats')
        DB_USER     = configDB.get('db_user', None)
        DB_PASSWD   = configDB.get('db_password', None)
        DB_CERT		= configDB.get('db_ssl_cert_file', None)
        DB_CA		= configDB.get('db_ssl_ca_file', None)
    except Exception as err:
        bu.error('Error reading config file', err)

    try:
        #### Connect to MongoDB
        if (DB_USER==None) or (DB_PASSWD==None):
            client = motor.motor_asyncio.AsyncIOMotorClient(DB_SERVER,DB_PORT, ssl=DB_SSL, ssl_cert_reqs=DB_CERT_REQ, ssl_certfile=DB_CERT, tlsCAFile=DB_CA)
        else:
            client = motor.motor_asyncio.AsyncIOMotorClient(DB_SERVER,DB_PORT, authSource=DB_AUTH, username=DB_USER, password=DB_PASSWD, ssl=DB_SSL, ssl_cert_reqs=DB_CERT_REQ, ssl_certfile=DB_CERT, tlsCAFile=DB_CA)
        
        db = client[DB_NAME]
        bu.debug(str(type(db)))

        tasks = []
        updateQ = await mk_updateQ(db, args.updates)
        if args.mode == 'player_stats':
            if args.filename == None: 
                args.filename = 'player_stats'
                for i in range(N_WORKERS):
                    tasks.append(asyncio.create_task(prune_player_stats_BS(i, db, updateQ, args)))
            elif args.mode == 'tank_stats':
                if args.filename == None: 
                    args.filename = 'tank_stats'                  
                for i in range(N_WORKERS):
                    tasks.append(asyncio.create_task(prune_tank_stats_WG(i, db, updateQ, args)))
                    bu.debug('Task ' + str(i) + ' started')
            
            bu.debug('Waiting for statsworkers to finish')
            await updateQ.join()
            
            bu.finish_progress_bar()

            bu.debug('Cancelling workers')
            for task in tasks:
                task.cancel()
            bu.debug('Waiting for workers to cancel')
            if len(tasks) > 0:
                await asyncio.gather(*tasks, return_exceptions=True)
        
        print_stats(args.mode)
            
    except asyncio.CancelledError as err:
        bu.error('Queue gets cancelled while still working.')
    except Exception as err:
        bu.error('Unexpected Exception: ' + str(type(err)) + ' : ' + str(err))

    return None

async def mk_updateQ(db : motor.motor_asyncio.AsyncIOMotorDatabase, updates2process : list) -> asyncio.Queue:
    """Create update queue for database queries"""

    if (len(updates2process) == 0):
        return None
    bu.debug(str(updates2process))
    updates2process = set(updates2process)
    updateQ = asyncio.Queue()
    try:
        dbc = db[DB_C_UPDATES]
        cursor = dbc.find( {} , { '_id' : 0 })
        cut_off_prev = 0
        async for doc in cursor:
            cut_off = doc['Cut-off']
            update = doc['Release']
            if update in updates2process:
                if (cut_off == None) or (cut_off == 0):
                    cut_off = bu.NOW()
                await updateQ.put([update, cut_off_prev, cut_off])
                updates2process.remove(update)
            cut_off_prev = cut_off
    except Exception as err:
        bu.error(exception=err)
    if len(updates2process) > 0:
        bu.error('Unknown update values give: ' + ', '.join(updates2process))
    return updateQ


def print_stats(stats_type = ""):
    bu.verbose_std(str(STATS_EXPORTED) + ' stats exported (' + stats_type + ')')


async def q_tank_stats_WG(workerID: int, db: motor.motor_asyncio.AsyncIOMotorDatabase, updateQ: asyncio.Queue, args : argparse.Namespace):
    """Async Worker to fetch player stats"""
    global STATS_EXPORTED
    dbc = db[DB_C_WG_TANK_STATS]
    
    filename = args.filename
    tier = args.tier
    all_data = args.all
    export_type = args.type

    tanks = await get_tanks_DB_tier(db, tier)
    bu.debug('[' + str(workerID) + '] ' + str(len(tanks))  + ' tanks in DB')

    while True:
        item = await updateQ.get()
        bu.debug('[' + str(workerID) + '] ' + str(updateQ.qsize())  + ' periods to process')
        
        try:
            dayA = item[0]
            dayB = item[1]
            timeA = int(time.mktime(dayA.timetuple()))
            timeB = int(time.mktime(dayB.timetuple()))
            if export_type == 'newer':
                datestr = dayA.isoformat()
            else:
                datestr = dayB.isoformat()
            fn = filename + '_' + datestr + '.jsonl'

            bu.debug('[' + str(workerID) + '] Start: ' + str(timeA) + ' End: ' + str(timeB))

            async with aiofiles.open(fn, 'w', encoding="utf8") as fp:
                for tank_id in tanks:
                    if all_data:
                        cursor = dbc.find({ '$and': [{'last_battle_time': {'$lt': timeB}}, {'last_battle_time': {'$gte': timeA}}, {'tank_id': tank_id } ] })
                    else:
                        pipeline = [ {'$match': { '$and': [{'last_battle_time': {'$lte': timeB}}, {'last_battle_time': {'$gt': timeA}}, {'tank_id': tank_id } ] }},
                                {'$sort': {'last_battle_time': -1}},
                                {'$group': { '_id': '$account_id',
                                            'doc': {'$first': '$$ROOT'}}},
                                {'$replaceRoot': {'newRoot': '$doc'}}, 
                                {'$project': {'_id': False}} ]
                        cursor = dbc.aggregate(pipeline, allowDiskUse=True)
                    
                    async for doc in cursor:
                        bu.print_progress()
                        await fp.write(json.dumps(doc, ensure_ascii=False) + '\n')
                        STATS_EXPORTED += 1
                        
        except Exception as err:
            bu.error('[' + str(workerID) + '] Unexpected Exception: ' + str(type(err)) + ' : ' + str(err))
        finally:
            bu.verbose_std('\n[' + str(workerID) + '] File write complete: ' + fn)
            updateQ.task_done()

    return None


async def prune_player_stats_BS(workerID: int, db: motor.motor_asyncio.AsyncIOMotorDatabase, updateQ: asyncio.Queue, args : argparse.Namespace):
    """Async Worker to fetch player stats"""
    global STATS_EXPORTED
    bu.error('NOT IMPLEMENTED YET')
    sys.exit(1)
    
    dbc = db[DB_C_BS_PLAYER_STATS]
    filename = args.filename
    all_data = args.all

    while True:
        item = await updateQ.get()
        bu.debug('[' + str(workerID) + '] ' + str(updateQ.qsize())  + ' periods to process')
        try:
            dayA = item[0]
            dayB = item[1]
            timeA = int(time.mktime(dayA.timetuple()))
            timeB = int(time.mktime(dayB.timetuple()))
            datestr = dayB.isoformat()
            fn = filename + '_' + datestr + '.jsonl'

            bu.debug('[' + str(workerID) + '] Start: ' +
                    str(timeA) + ' End: ' + str(timeB))

            async with aiofiles.open(fn, 'w', encoding="utf8") as fp:
                id_step = int(5e7)
                for id in range(0, int(4e9), id_step):
                    if all_data:
                        cursor = dbc.find({ '$and': 
                            [{'last_battle_time': {'$lte': timeB}}, {'last_battle_time': {'$gt': timeA}},
                             {'account_id': {'$lte': id + id_step}}, {'account_id': {'$gt': id}}]})
                    else:
                        pipeline = [{'$match': {
                            '$and': [{'last_battle_time': {'$lte': timeB}}, {'last_battle_time': {'$gt': timeA}},
                                {'account_id': {'$lte': id + id_step}}, {'account_id': {'$gt': id}}]}},
                                {'$sort': {'last_battle_time': -1}},
                                {'$group': {'_id': '$account_id',
                                            'doc': {'$first': '$$ROOT'}}},
                                {'$replaceRoot': {'newRoot': '$doc'}},
                                {'$project': {'achievements': False, 'clan': False}}]
                        cursor = dbc.aggregate(pipeline, allowDiskUse=False)

                    async for doc in cursor:
                        bu.print_progress()
                        await fp.write(json.dumps(doc, ensure_ascii=False) + '\n')
                        STATS_EXPORTED += 1

                    bu.debug('[' + str(workerID) + '] write iteration complete')
                bu.debug('[' + str(workerID) + '] File write complete')

        except Exception as err:
            bu.error('[' + str(workerID) + '] Unexpected Exception: ' + str(type(err)) + ' : ' + str(err))
        finally:
            bu.debug('[' + str(workerID) + '] File write complete')
            updateQ.task_done()

    return None


async def get_tanks_DB(db: motor.motor_asyncio.AsyncIOMotorDatabase):
    """Get tank_ids of tanks in the DB"""
    dbc = db[DB_C_WG_TANK_STATS]
    return await dbc.distinct('tank_id')
    

async def get_tanks_DB_tier(db: motor.motor_asyncio.AsyncIOMotorDatabase, tier: int):
    """Get tank_ids of tanks in a particular tier"""
    dbc = db[DB_C_TANKOPEDIA]
    tanks = list()
    
    if (tier == None):
        tanks = await get_tanks_DB(db)
        
    elif (tier <= 10) and (tier > 0):
        cursor = dbc.find({'tier': tier}, {'_id': 1})
        async for tank in cursor:
            try:
                tanks.append(tank['_id'])
            except Exception as err:
                bu.error('Unexpected error: ' +
                         str(type(err)) + ' : ' + str(err))
    return tanks


# main()
if __name__ == "__main__":
    #asyncio.run(main(sys.argv[1:]), debug=True)
    asyncio.run(main(sys.argv[1:]))
