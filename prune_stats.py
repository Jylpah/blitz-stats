#!/usr/bin/env python3.8

# Script Prune stats from the DB per release 

import sys, os, argparse, datetime, json, inspect, pprint, aiohttp, asyncio, aiofiles
import aioconsole, re, logging, time, xmltodict, collections, pymongo, motor.motor_asyncio
import ssl, configparser
from datetime import date
import blitzutils as bu
from blitzutils import BlitzStars

N_WORKERS = 4
MAX_RETRIES = 3
logging.getLogger("asyncio").setLevel(logging.DEBUG)

FILE_CONFIG = 'blitzstats.ini'

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

DB_COLLECTIONS = {  'tank_stats'             : DB_C_TANK_STATS, 
                    'player_achievements'   : DB_C_PLAYER_ACHIVEMENTS 
                }

CACHE_VALID = 24*3600*7   # 7 days

bs = None

TODAY = datetime.datetime.utcnow().date()
DEFAULT_DAYS_DELTA = datetime.timedelta(days=90)
DATE_DELTA = datetime.timedelta(days=7)
STATS_START_DATE = datetime.datetime(2014,1,1)

STATS_PRUNED = dict()
DUPS_FOUND = dict()
for mode in DB_COLLECTIONS.keys():
    STATS_PRUNED[mode]  = 0
    DUPS_FOUND[mode]    = 0


# main() -------------------------------------------------------------


async def main(argv):
    # set the directory for the script
    os.chdir(os.path.dirname(sys.argv[0]))

    parser = argparse.ArgumentParser(description='Prune stats from the DB by update')
    parser.add_argument('--mode', default=['tank_stats'], nargs='+', choices=DB_COLLECTIONS.keys(), help='Select type of stats to export')
    parser.add_argument( '-n', '--no_analyze', 	action='store_true', default=False, help='Skip analyzing the database (default FALSE: i.e. to analyze)')
    parser.add_argument( '-p', '--prune', 	action='store_true', default=False, help='Actually Prune database i.e. DELETE DATA (default is FALSE)')
    parser.add_argument('updates', metavar='X.Y [Z.D ...]', type=str, nargs='*', help='List of updates to prune')
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

        await db[DB_C_STATS_2_DEL].create_index('id')	

        if not args.no_analyze:
            tasks = []
            updates = await mk_update_list(db, args.updates)
            tankQ = None
            for u in updates:           
                bu.verbose_std('Processing update ' + u['update'])
                if bu.is_normal():
                    bu.set_counter('Stats processed: ')   
                while True:
                    if  'tank_stats' in args.mode:
                        tankQ    = await mk_tankQ(db)

                    workers = 0
                    while workers < N_WORKERS:
                        # if args.analyze == 'player_stats':
                        #     bu.error('NOT IMPLEMENTED YET')
                        #     tasks.append(asyncio.create_task(prune_player_stats_WG(i, db, tankQ, start, end)))
                        if  'tank_stats' in args.mode:
                            tasks.append(asyncio.create_task(prune_tank_stats_WG(workers, db, u, tankQ)))
                            bu.debug('Task ' + str(workers) + ' started')
                            workers += 1
                        elif 'player_achivements' in args.mode:
                            tasks.append(asyncio.create_task(prune_player_achievements_WG(workers, db, u)))
                            bu.debug('Task ' + str(workers) + ' started')
                            workers += 1                    
            
                    bu.debug('Waiting for workers to finish')
                    await tankQ.join()            
                    bu.debug('Cancelling workers')
                    for task in tasks:
                        task.cancel()
                    bu.debug('Waiting for workers to cancel')
                    if len(tasks) > 0:
                        await asyncio.gather(*tasks, return_exceptions=True)
                    if tankQ.empty():
                        break
    
        # do the actual pruning and DELETE DATA
        if args.prune:
            bu.verbose_std('Starting to prune in 3 seconds. Press CTRL + C to CANCEL')
            for i in range(3):
                print(str(i) + '  ', end='')
                time.sleep(1)
            print('')
            await prune_stats(db, args)

        bu.finish_progress_bar()
        print_stats(args.mode)
            
    except asyncio.CancelledError as err:
        bu.error('Queue gets cancelled while still working.')
    except Exception as err:
        bu.error('Unexpected Exception', exception=err)

    return None


async def mk_update_list(db : motor.motor_asyncio.AsyncIOMotorDatabase, updates2process : list) -> list:
    """Create update queue for database queries"""

    if (len(updates2process) == 0):
        bu.error('No updates given to prune')
        return list()
    bu.debug(str(updates2process))
    # if len(updates2process) == 1:
    #     p_updates_since = re.compile('\\d+\\.\\d+\\+$')
    #     if p_updates_since.match(updates2process[0]) != None:
    #         updates2process[0] = updates2process[0][:-1]
    #         updates_since = True
    updates2process = set(updates2process)
    updates = list()
    try:
        bu.debug('Fetching updates from DB')        
        dbc = db[DB_C_UPDATES]
        cursor = dbc.find( {} , { '_id' : 0 }).sort('Date', pymongo.ASCENDING )
        cut_off_prev = 0
        bu.debug('Iterating over updates')
        first_update_found = False
        async for doc in cursor:
            cut_off = doc['Cut-off']
            update = doc['Release']
            if update + '+' in updates2process:
                updates2process.remove(update + '+')
                first_update_found = True
            if update + '-' in updates2process:
                updates2process = set()
                # stop finding updates to prunes
                break
            if first_update_found or (update in updates2process):
                if (cut_off == None) or (cut_off == 0):
                    cut_off = bu.NOW()
                updates.append({'update': update, 'start': cut_off_prev, 'end': cut_off})
                try:
                    updates2process.remove(update)
                except KeyError as err:
                    bu.debug('If using X.Y+ or X.Y- arguments KeyError is normal', exception=err)
            cut_off_prev = cut_off

    except Exception as err:
        bu.error(exception=err)
    if len(updates2process) > 0:
        bu.error('Unknown update values give: ' + ', '.join(updates2process))
    return updates


async def mk_tankQ(db : motor.motor_asyncio.AsyncIOMotorDatabase) -> asyncio.Queue:
    """Create TANK queue for database queries"""

    tankQ = asyncio.Queue()
    try:
        for tank_id in await get_tanks_DB(db):
            await tankQ.put(tank_id)            
    except Exception as err:
        bu.error(exception=err)
    bu.debug('Tank queue created: ' + str(tankQ.qsize()))
    return tankQ


async def mk_accountQ(db : motor.motor_asyncio.AsyncIOMotorDatabase, step: int = 1e7) -> asyncio.Queue:
    """Create ACCOUNT_ID queue for database queries"""    
    accountQ = asyncio.Queue()
    try:
        for min in range(0,4e9-step, step):
            await accountQ.put({'min': min, 'max': min + step})            
    except Exception as err:
        bu.error(exception=err)
    bu.debug('Account_id queue created')    
    return accountQ


def print_stats(stat_types : list = list()):
    for stat_type in stat_types:
        bu.verbose_std(stat_type + ': ' + str(DUPS_FOUND[stat_type]) + ' new duplicates found')
    for stat_type in stat_types:
        bu.verbose_std(stat_type + ': ' + str(STATS_PRUNED[stat_type]) + ' duplicates removed')


async def prune_tank_stats_WG(workerID: int, db: motor.motor_asyncio.AsyncIOMotorDatabase, update: dict, tankQ: asyncio.Queue):
    """Async Worker to fetch player tank stats"""
    dbc = db[DB_C_TANK_STATS]
    mode = 'tank_stats'

    try:
        start   = update['start']
        end     = update['end']
        update  = update['update']
    except Exception as err:
        bu.error('Unexpected Exception: ' + str(type(err)) + ' : ' + str(err), id=workerID)
        return None    

    while not tankQ.empty():
        try:
            tank_id = await tankQ.get()
            bu.debug(str(tankQ.qsize())  + ' tanks to process', id=workerID)
                
            pipeline = [ {'$match': { '$and': [  {'tank_id': tank_id }, {'last_battle_time': {'$lte': end}}, {'last_battle_time': {'$gt': start}} ] }},
                         { '$project' : { 'account_id' : 1, 'tank_id' : 1, 'last_battle_time' : 1}},
                         { '$sort': {'account_id': 1, 'last_battle_time': -1} }
                        ]
            cursor = dbc.aggregate(pipeline, allowDiskUse=True)

            account_id_prev = -1
            entry_prev = mk_log_entry(mode, account_id_prev, -1, -1)        
            dups_counter = 0
            async for doc in cursor:
                if bu.is_normal():
                    bu.print_progress()
                account_id = doc['account_id']
                if bu.is_debug():
                    entry = mk_log_entry(mode, account_id, doc['last_battle_time'], doc['tank_id'])
                if account_id == account_id_prev:
                    # Older doc found!
                    if bu.is_debug():
                        bu.debug('Duplicate found: --------------------------------')
                        bu.debug(entry + ' : Old (to be deleted)')
                        bu.debug(entry_prev + ' : Newer')
                    await add_stat2del(workerID, db, mode, doc['_id'])
                    dups_counter += 1
                account_id_prev = account_id 
                if bu.is_debug():
                    entry_prev = entry

        except Exception as err:
            bu.error('Unexpected Exception: ' + str(type(err)) + ' : ' + str(err), id=workerID)
        finally:
            bu.debug('Tank_id=' + str(tank_id) + ' processed: ' + str(dups_counter) + ' duplicates found', id = workerID)
            tankQ.task_done()

    return None


async def prune_player_achievements_WG(workerID: int, db: motor.motor_asyncio.AsyncIOMotorDatabase, update: dict):
    """Async Worker to fetch player achievement stats"""
    dbc = db[DB_C_PLAYER_ACHIVEMENTS]
    mode = 'player_achievements'
    try:
        start   = update['start']
        end     = update['end']
        update  = update['update']
        
        pipeline = [ {'$match': { '$and': [  {'updated': {'$lte': end}}, {'updated': {'$gt': start}} ] }},
                        {'$sort': {'account_id': 1, 'updated': -1} } ]
        cursor = dbc.aggregate(pipeline, allowDiskUse=True)

        account_id_prev = -1 
        entry_prev = mk_log_entry(mode, account_id_prev, -1)
        dups_counter = 0       
        async for doc in cursor:    
            if bu.is_normal():
                bu.print_progress()
            account_id = doc['account_id']
            if bu.is_debug():
                entry = mk_log_entry(mode, account_id, doc['last_battle_time'])
            if account_id == account_id_prev:
                # Older doc found!
                if bu.is_debug():
                    bu.debug('Duplicate found: --------------------------------')
                    bu.debug(entry + ' : Old (to be deleted)')
                    bu.debug(entry_prev + ' : Newer')
                await add_stat2del(workerID, db, mode, doc['_id'])
                dups_counter += 1                
            account_id_prev = account_id
            if bu.is_debug():
                entry_prev = entry 

    except Exception as err:
        bu.error('Unexpected Exception', exception=err, id=workerID)
    finally:
        bu.debug( mode + ': ' + str(dups_counter) + ' duplicates found for update ' + update, id = workerID)          

    return None


async def add_stat2del(workerID: int, db: motor.motor_asyncio.AsyncIOMotorDatabase, stat_type: str, id: str):
    """Adds _id of the stat record to be deleted in into DB_C_STATS_2_DEL"""
    global DUPS_FOUND
    dbc = db[DB_C_STATS_2_DEL]
    try:
        await dbc.insert_one({'type': stat_type, 'id': id})
        DUPS_FOUND[stat_type] += 1
    except Exception as err:
        bu.error(exception=err, id=workerID)
    return None


async def prune_stats(db: motor.motor_asyncio.AsyncIOMotorDatabase, args : argparse.Namespace):
    """Execute DB pruning and DELETING DATA"""
    global STATS_PRUNED
    try:
        
        dbc = db[DB_C_STATS_2_DEL]
        for stat_type in args.mode:            
            dbc2prune = db[DB_COLLECTIONS[stat_type]]
            cursor = dbc.find({'type' : stat_type})
            async for doc in cursor:
                id = doc['id']
                try:
                    await dbc2prune.delete_one({'_id' : id})
                except Exception as err:
                    bu.error('Failed to delete duplicate _id=' + id, exception=err)
                try:
                    await dbc.delete_one({ '_id' : doc['_id'] })
                except Exception as err:
                    bu.error(exception=err)
                STATS_PRUNED[stat_type] += 1

    except Exception as err:
        bu.error(exception=err)
    return None


def mk_log_entry(stat_type: str = None, account_id=None, last_battle_time=None, tank_id = None):
    try:
        entry = stat_type + ': '
        if (account_id != None):
            entry = entry + 'account_id=' + str(account_id)
        if (tank_id != None):
            entry = entry + ': tank_id=' + str(tank_id)
        entry = entry + ': last_battle_time=' + str(last_battle_time)
        return entry
    except Exception as err:
        bu.error(exception=err)
        return None

async def get_tanks_DB(db: motor.motor_asyncio.AsyncIOMotorDatabase):
    """Get tank_ids of tanks in the DB"""
    dbc = db[DB_C_TANK_STATS]
    return await dbc.distinct('tank_id')
    

# main()
if __name__ == "__main__":
    #asyncio.run(main(sys.argv[1:]), debug=True)
    asyncio.run(main(sys.argv[1:]))
