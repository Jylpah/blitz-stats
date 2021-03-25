#!/usr/bin/env python3

# Script Prune stats from the DB per release 

import sys, os, argparse, datetime, json, inspect, pprint, aiohttp, asyncio, aiofiles
import aioconsole, re, logging, time, xmltodict, collections, pymongo, motor.motor_asyncio
import ssl, configparser, random
from datetime import date
import blitzutils as bu
import blitzstatsutils as su
from blitzutils import BlitzStars, RecordLogger, WG

N_WORKERS = 4

logging.getLogger("asyncio").setLevel(logging.DEBUG)

FILE_CONFIG = 'blitzstats.ini'

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

FIELD_NEW_STATS = '_updated'

MODE_TANK_STATS         = 'tank_stats'
MODE_PLAYER_STATS       = 'player_stats'
MODE_PLAYER_ACHIEVEMENTS= 'player_achievements'
MODE_ARCHIVE            = '_archive'

STR_MODES = {    
    MODE_TANK_STATS             : 'Tank Stats', 
    MODE_PLAYER_STATS           : 'Player Stats',
    MODE_PLAYER_ACHIEVEMENTS    : 'Player Achievements' 
}
modes = list(STR_MODES.keys())
for stat_type in modes:
    STR_MODES[stat_type + MODE_ARCHIVE] = STR_MODES[stat_type] + ' (Archive)'


DB_C = {    MODE_TANK_STATS             : DB_C_TANK_STATS, 
            MODE_PLAYER_STATS           : DB_C_PLAYER_STATS,
            MODE_PLAYER_ACHIEVEMENTS    : DB_C_PLAYER_ACHIVEMENTS 
        }

DB_C_ARCHIVE = dict()
for mode in DB_C:
    DB_C_ARCHIVE[mode] = DB_C[mode] + DB_STR_ARCHIVE

CACHE_VALID     = 7*24*3600   # 7 days
DEFAULT_SAMPLE  = 1000
QUEUE_LEN       = 10000
DEFAULT_BATCH   = 1000

bs = None

TODAY = datetime.datetime.utcnow().date()
DEFAULT_DAYS_DELTA = datetime.timedelta(days=90)
DATE_DELTA = datetime.timedelta(days=7)
STATS_START_DATE = datetime.datetime(2014,1,1)

def def_value_zero():
    return 0


#####################################################################
#                                                                   #
# main()                                                            #
#                                                                   #
#####################################################################

async def main(argv):
    # set the directory for the script
    start_time = time.time()
    current_dir = os.getcwd()
    os.chdir(os.path.dirname(sys.argv[0]))

    parser = argparse.ArgumentParser(description='Manage DB stats')
    parser.add_argument('--mode', default=['tank_stats'], nargs='+', choices=DB_C.keys(), help='Select type of stats to process')
    
    arggroup_action = parser.add_mutually_exclusive_group(required=True)
    arggroup_action.add_argument( '--analyze',  action='store_true', default=False, help='Analyze the database for duplicates')
    arggroup_action.add_argument( '--check', 	action='store_true', default=False, help='Check the analyzed duplicates')
    arggroup_action.add_argument( '--prune', 	action='store_true', default=False, help='Prune database for the analyzed duplicates i.e. DELETE DATA')
    arggroup_action.add_argument( '--snapshot',	action='store_true', default=False, help='Snapshot latest stats from the archive')
    arggroup_action.add_argument( '--archive',	action='store_true', default=False, help='Archive latest stats')
    arggroup_action.add_argument( '--clean',	action='store_true', default=False, help='Clean latest stats from old stats')
    arggroup_action.add_argument( '--reset', 	action='store_true', default=False, help='Reset the analyzed duplicates')

    parser.add_argument('--opt_tanks',   default=None, nargs='*', type=str, help='List of tank_ids for other options. Use "tank_id+" to start from a tank_id')
    parser.add_argument('--opt_archive', action='store_true', default=False, help='Process stats archive (--mode=tank_stats only)')

    arggroup_verbosity = parser.add_mutually_exclusive_group()
    arggroup_verbosity.add_argument( '-d', '--debug', 	action='store_true', default=False, help='Debug mode')
    arggroup_verbosity.add_argument( '-v', '--verbose', action='store_true', default=False, help='Verbose mode')
    arggroup_verbosity.add_argument( '-s', '--silent', 	action='store_true', default=False, help='Silent mode')

    parser.add_argument('--sample', type=int, default=None, help='Sample size. Default=' + str(DEFAULT_SAMPLE) + ' . 0: check ALL.')
    parser.add_argument('-l', '--log', action='store_true', default=False, help='Enable file logging')
    parser.add_argument('updates', metavar='X.Y [Z.D ...]', type=str, nargs='*', help='List of updates to prune')
    args = parser.parse_args()

    try:
        bu.set_log_level(args.silent, args.verbose, args.debug)
        bu.set_progress_step(100)
        if args.snapshot or args.archive:
            args.log = True
        if args.log:
            datestr = datetime.datetime.now().strftime("%Y%m%d_%H%M")
            await bu.set_file_logging(bu.rebase_file_args(current_dir, 'manage_stats_' + datestr + '.log'))

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

        await db[DB_C_STATS_2_DEL].create_index('id', background=True)	

        if  args.analyze or args.check or args.prune:
            updates = await mk_update_list(db, args.updates)
        else:
            updates = list()

        if args.analyze:
            bu.verbose_std('Starting to ANALYZE duplicates in 3 seconds. Press CTRL + C to CANCEL')
            bu.wait(3)
            await analyze_stats(db, updates, args)
        
        elif args.check:
            bu.verbose_std('Starting to CHECK duplicates in 3 seconds. Press CTRL + C to CANCEL')
            bu.wait(3)         
            await check_stats(db, updates, args)
                    
        elif args.prune:
            # do the actual pruning and DELETE DATA
            bu.verbose_std('Starting to PRUNE in 3 seconds. Press CTRL + C to CANCEL')
            bu.wait(3)
            await prune_stats(db, updates, args)
        
        elif args.snapshot:
            bu.verbose_std('Starting to SNAPSHOT ' + ', '.join(args.mode) + ' in 3 seconds. Press CTRL + C to CANCEL')
            bu.wait(3)
            if input('Write "SNAPSHOT" if you prefer to snapshot stats: ') == 'SNAPSHOT':
                if MODE_PLAYER_ACHIEVEMENTS in args.mode:
                    bu.verbose_std('Starting to SNAPSHOT PLAYER ACHIEVEMENTS')
                    await snapshot_player_achivements(db, args)
                if MODE_TANK_STATS in args.mode:
                    bu.verbose_std('Starting to SNAPSHOT TANK STATS')
                    await snapshot_tank_stats(db, args)
            else:
                bu.error('Invalid input given. Exiting...')

        elif args.archive:
            bu.verbose_std('Starting to ARCHIVE stats in 3 seconds')
            bu.verbose_std('Run ANALYZE + PRUNE before archive')
            bu.verbose_std('Press CTRL + C to CANCEL')
            bu.wait(3)
            if MODE_PLAYER_ACHIEVEMENTS in args.mode:
                await archive_player_achivements(db, args)
            if MODE_TANK_STATS in args.mode:
                await archive_tank_stats(db, args)
        
        elif args.reset:
            bu.verbose_std('Starting to RESET duplicates in 3 seconds. Press CTRL + C to CANCEL')
            bu.wait(3)         
            await reset_duplicates(db, updates, args)
             
    except KeyboardInterrupt:
        bu.finish_progress_bar()
        bu.verbose_std('\nExiting..')
    except asyncio.CancelledError as err:
        bu.error('Queue gets cancelled while still working.')
    except Exception as err:
        bu.error('Unexpected Exception', exception=err)
    bu.verbose_std(time_elapsed(start_time, 'Total execution time'))
    return None


def time_elapsed(start: float, prefix: str = 'Time elapsed') -> str:
    try:
        return prefix + ': ' + time.strftime("%H:%M:%S", time.gmtime(time.time() - start))
    except Exception as err:
        bu.error(exception=err)
        return 'time_elapsed(): ERROR'


def mk_update_entry(update: str, start: int, end: int)  -> dict:
    """Make update entry to the update list to process"""
    if (end == None) or (end == 0):
        end = bu.NOW()
    return {'update': update, 'start': start, 'end': end }


def mk_dups_Q_entry(ids: list, from_db: bool = False) -> dict:
    """Make a prune task for prune queue"""
    if (ids == None) or (len(ids) == 0):
        return None
    return { 'ids': ids, 'from_db': from_db }


def mk_dup_db_entry(stat_type: str, _id=str) -> dict:
    return  {'type': stat_type, 'id': _id} 


async def get_latest_update(db: motor.motor_asyncio.AsyncIOMotorDatabase) -> dict:
    try:
        dbc = db[DB_C_UPDATES]
        cursor = dbc.find().sort('Date',-1).limit(2)
        updates = await cursor.to_list(2)
        doc = updates.pop(0)
        update = doc['Release']
        end = doc['Cut-off']
        doc = updates.pop(0)
        start = doc['Cut-off']
        return mk_update_entry(update, start, end)
    except Exception as err:
        bu.error(exception=err)   


async  def mk_update_list(db : motor.motor_asyncio.AsyncIOMotorDatabase, updates2process : list) -> list:
    """Create update queue for database queries"""
    if (len(updates2process) == 0):
        bu.verbose_std('Processing the latest update')
        return [ await get_latest_update(db) ]
    elif (len(updates2process) == 1) and (updates2process[0] == su.UPDATE_ALL):
        bu.verbose_std('Processing ALL data')
        return [ None ]
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
        last_update_found  = False
        async for doc in cursor:
            cut_off = doc['Cut-off']
            update = doc['Release']
            if update + '+' in updates2process:
                updates2process.remove(update + '+')
                first_update_found = True
            if update + '-' in updates2process:
                if not first_update_found:
                    bu.error('"update_A-" can only be used in conjuction with "update_B+"')
                updates2process.remove(update + '-')
                last_update_found = True
            ## first_update_found has to be set for the update- to work
            if first_update_found or (update in updates2process):
                if (cut_off == None) or (cut_off == 0):
                    cut_off = bu.NOW()
                updates.append(mk_update_entry(update, cut_off_prev, cut_off))
                try: 
                    if not first_update_found:
                        updates2process.remove(update)
                except KeyError as err:
                    bu.error(exception=err)
                if last_update_found:
                    first_update_found = False
                    last_update_found = False
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


async def mk_accountQ(step: int = int(5e7)) -> asyncio.Queue:
    """Create ACCOUNT_ID queue for database queries"""    
    accountQ = asyncio.Queue()
    try:
        id_max  = WG.ACCOUNT_ID_MAX
        res     = list()        
        for min in range(0,id_max-step, step):
            res.append({'min': min, 'max': min + step})
        random.shuffle(res) # randomize the results for better ETA estimate.
        for item in res:
            await accountQ.put(item)     
    except Exception as err:
        bu.error(exception=err)
    bu.debug('Account_id queue created')    
    return accountQ


async def mk_account_tankQ(db: motor.motor_asyncio.AsyncIOMotorDatabase, 
                            tank_ids: list = None, archive: bool = False, 
                            account_id_step: int = int(5e7)) -> asyncio.Queue:
    """Create a queue of ACCOUNT_ID * TANK_ID queue for database queries"""    
    retQ = asyncio.Queue()
    try:
        if tank_ids == None:
            tank_ids = await get_tanks_DB(db, archive)
        id_max = WG.ACCOUNT_ID_MAX
        res = list()       
        for min in range(0,id_max-account_id_step, account_id_step):
            for tank_id in tank_ids:
                res.append( { 'tank_id': tank_id, 'account_id': {'min': min, 'max': min + account_id_step } } )
        random.shuffle(res)   # randomize the results for better ETA estimate.
        for item in res: 
            await retQ.put(item)
    except Exception as err:
        bu.error('Failed to create account_id * tank_id queue', exception=err)
        return None
    bu.debug('account_id * tank_id queue created')    
    return retQ


# def print_stats_analyze(stat_types : list = list()):
#     for stat_type in stat_types:
#         if DUPS_FOUND[stat_type] == 0:
#             bu.verbose_std(stat_type + ': No duplicates found')
#         else:    
#             bu.verbose_std(stat_type + ': ' + str(DUPS_FOUND[stat_type]) + ' new duplicates found')
#         DUPS_FOUND[stat_type] = 0
    

# def print_stats_prune(stats_pruned : dict):
#     """Print end statistics of the pruning operation"""
#     try:
#         for stat_type in stats_pruned:
#             bu.verbose_std(stat_type + ': ' + str(stats_pruned[stat_type]) + ' duplicates removed')
#             stats_pruned[stat_type] = 0
#     except Exception as err:
#         bu.error(exception=err)
#     return None    


async def analyze_stats(db: motor.motor_asyncio.AsyncIOMotorDatabase, 
                        updates: list, args: argparse.Namespace = None) -> RecordLogger:
    """--analyze: top-level func for analyzing stats for duplicates"""
    try:
        rl = RecordLogger('Analyze')
        if MODE_TANK_STATS in args.mode:
            rl.merge(await analyze_tank_stats(db, updates, args))
        
        if MODE_PLAYER_ACHIEVEMENTS in args.mode:
            rl.merge(await analyze_player_achievements(db, updates, args))            

    except Exception as err:
        bu.error(exception=err)
    bu.log(rl.print(do_print=False))
    rl.print()
    return None     


async def reset_duplicates(db: motor.motor_asyncio.AsyncIOMotorDatabase, 
                        updates: list, args: argparse.Namespace = None) -> RecordLogger:
    """--reset: reset/delete duplicates info from the DB"""
    try:
        rl = RecordLogger('Reset')
        archive = args.opt_archive
        dbc = db[DB_C_STATS_2_DEL]

        for stat_type in args.mode:
            if archive:
                stat_type = stat_type + MODE_ARCHIVE
            rl_mode = RecordLogger(get_mode_str(stat_type) + ' duplicates')
            res = await dbc.delete_many({'type': stat_type})
            if res != None:
                rl_mode.log('OK', res.deleted_count)
            else:
                rl_mode.log('FAILED')
            rl.merge(rl_mode)  

    except Exception as err:
        bu.error(exception=err)
    bu.log(rl.print(do_print=False))
    rl.print()
    return None


async def analyze_tank_stats(db: motor.motor_asyncio.AsyncIOMotorDatabase, 
                             updates: list = list(), args: argparse.Namespace = None)  -> RecordLogger:
    """--analyze: top-level func for analyzing stats for duplicates. DOES NOT PRUNE"""
    try:        
        archive = args.opt_archive
        stat_type = MODE_TANK_STATS
        rl = RecordLogger(get_mode_str(stat_type, archive))
        dupsQ = asyncio.Queue(QUEUE_LEN)
        dups_saver = asyncio.create_task(save_dups_worker(db, stat_type, dupsQ, archive))
        
        for u in updates: 
            try:
                if u == None:
                    update_str = su.UPDATE_ALL
                    bu.verbose_std('Analyzing ' + get_mode_str(stat_type, archive) + ' for duplicates. (ALL DATA)')
                else:
                    update_str = u['update']
                    bu.verbose_std('Analyzing ' + get_mode_str(stat_type, archive) + ' for duplicates. Update ' + update_str)
                    
                account_tankQ = await mk_account_tankQ(db)                
                bu.set_progress_bar('Stats processed:', account_tankQ.qsize(), step=50, slow=True)   
                
                workers = []
                for workerID in range(0, N_WORKERS):
                    workers.append(asyncio.create_task(find_dup_tank_stats_worker(db, account_tankQ, dupsQ, u, workerID, archive)))
                
                await account_tankQ.join()
                bu.finish_progress_bar()
                bu.debug('Waiting for workers to finish')
                if len(workers) > 0:
                    i = 0
                    for rl_worker in await asyncio.gather(*workers, return_exceptions=True):
                        bu.debug('Merging find_dup_tank_stats_worker\'s RecordLogger', id=i)
                        rl.merge(rl_worker)                
        
            except Exception as err:
                bu.error('Update: ' + update_str, exception=err)    
        
        await dupsQ.join()
        dups_saver.cancel()
        for rl_task in await asyncio.gather(*[dups_saver], return_exceptions=True):
            rl.merge(rl_task)
    
    except Exception as err:
        bu.error(exception=err)
    return rl


async def analyze_player_achievements(db: motor.motor_asyncio.AsyncIOMotorDatabase,
                                      updates: list, args: argparse.Namespace = None) -> RecordLogger:
    try:
        archive     = args.opt_archive
        stat_type   = MODE_PLAYER_ACHIEVEMENTS
        rl          = RecordLogger(get_mode_str(stat_type, archive))
        dupsQ       = asyncio.Queue(QUEUE_LEN)
        dups_saver  = asyncio.create_task(save_dups_worker(db, stat_type, dupsQ, archive))
        
        for u in updates: 
            try:
                if u == None:
                    update_str = su.UPDATE_ALL
                    bu.verbose_std('Analyzing ' + get_mode_str(stat_type, archive) + ' for duplicates. (ALL DATA)')
                else:
                    update_str = u['update']
                    bu.verbose_std('Analyzing ' + get_mode_str(stat_type, archive) + ' for duplicates. Update ' + u['update'])
                accountQ = await mk_accountQ()
                lenQ = accountQ.qsize()                
                bu.set_progress_bar('Stats processed:', lenQ, step=5, slow=True)  
                # bu.set_counter('Stats processed: ')   
                workers = []

                for workerID in range(0, N_WORKERS):
                    workers.append(asyncio.create_task(find_dup_player_achivements_worker(db, accountQ, dupsQ, u, workerID, archive)))
                
                await accountQ.join()
                bu.debug('Waiting for workers to finish')
                if len(workers) > 0:
                    for res in await asyncio.gather(*workers, return_exceptions=True):
                        rl.merge(res) 
                bu.finish_progress_bar()
        
            except Exception as err:
                bu.error('Update: ' + update_str, exception=err)    
        
        await dupsQ.join()
        dups_saver.cancel()
        for rl_task in await asyncio.gather(*[dups_saver], return_exceptions=True):
            rl.merge(rl_task)
    
    except Exception as err:
        bu.error(exception=err)
    return rl


async def save_dups_worker( db: motor.motor_asyncio.AsyncIOMotorDatabase, 
                            stat_type: str, dupsQ: asyncio.Queue, archive: bool = False)  -> RecordLogger:
    """Save duplicates information to the DB"""
    try:
        dbc = db[DB_C_STATS_2_DEL]
        rl = RecordLogger('Save duplicate info')
        if archive:
            stat_type = stat_type + MODE_ARCHIVE
        while True:
            dups = await dupsQ.get()
            try:                
                res = await dbc.insert_many( [ mk_dup_db_entry(stat_type, dup_id) for dup_id in dups['ids'] ] )
                rl.log('Saved', len(res.inserted_ids))
            except Exception as err:
                bu.error(exception=err)
                rl.log('Errors')
            dupsQ.task_done()
    
    except asyncio.CancelledError:
        bu.debug('Duplicate queue is empty')
    except Exception as err:
        bu.error(exception=err)
    return rl


async def get_dups_worker( db: motor.motor_asyncio.AsyncIOMotorDatabase, 
                            stat_type: str, dupsQ: asyncio.Queue, 
                            sample: int = 0, archive = False)  -> RecordLogger:
    """Read duplicates info from the DB"""
    try:
        dbc = db[DB_C_STATS_2_DEL]
        count = 0
        if archive:
            stat_type = stat_type + MODE_ARCHIVE
        rl = RecordLogger('Fetch ' + get_mode_str(stat_type))

        pipeline = [  { '$match': {'type' : stat_type} } ]
        if (sample != None) and (sample > 0):
            pipeline.append({ '$sample': { 'size': sample } })

        cursor = dbc.aggregate(pipeline, allowDiskUse=True )
        dups = await cursor.to_list(DEFAULT_BATCH)
        
        while dups:
            try:
                ids  =  [ dup['id']   for dup in dups ]                
                await dupsQ.put( mk_dups_Q_entry( ids, True ) )
                count += len(dups)
                rl.log('Read', len(dups))
            except Exception as err:
                rl.log('Errors')
                bu.error(exception=err)
            finally:
                dups = await cursor.to_list(DEFAULT_BATCH)
    
    except (asyncio.CancelledError):
        bu.debug('Cancelled before finishing')
    except Exception as err:
        bu.error(exception=err)
    bu.debug('Exiting. Added ' + str(count) + ' duplicates')
    return rl


# ## DEPRECIATED
# async def analyze_tank_stats_worker(db: motor.motor_asyncio.AsyncIOMotorDatabase, 
#                                     update_record: dict, workerID: int, 
#                                     tankQ: asyncio.Queue, prune : bool = False):
#     """Worker to analyze duplicates in tank stats"""
#     try:
#         dbc         = db[DB_C_TANK_STATS]
#         stat_type   = MODE_TANK_STATS
#         rl          = RecordLogger('Analyze Tank Stats')    
#         start       = update_record['start']
#         end         = update_record['end']
#         update      = update_record['update']
#     except Exception as err:
#         bu.error(exception=err, id=workerID)
#         return None    

#     while not tankQ.empty():
#         try:
#             tank_id = await tankQ.get()
#             bu.debug('Update ' + update + ': ' + str(tankQ.qsize())  + ' tanks to process', id=workerID)
                
#             pipeline = [    {'$match': { '$and': [  {'tank_id': tank_id }, 
#                                 {'last_battle_time': {'$gt': start}}, 
#                                 {'last_battle_time': {'$lte': end}} ] }},
#                             { '$project' : { 'account_id' : 1, 'last_battle_time' : 1}},
#                             { '$sort': {'account_id': pymongo.ASCENDING, 'last_battle_time': pymongo.DESCENDING} }
#                         ]
#             cursor = dbc.aggregate(pipeline, allowDiskUse=True)

#             account_id_prev = -1
#             entry_prev = mk_log_entry(stat_type, { 'account_id': -1})        
#             async for doc in cursor:
#                 bu.print_progress()
#                 account_id = doc['account_id']
#                 if bu.is_debug():
#                     entry = mk_log_entry(stat_type, {   'account_id' : account_id, 
#                                                         'last_battle_time' : doc['last_battle_time'], 
#                                                         'tank_id' : tank_id})
#                 if account_id == account_id_prev:
#                     # Older doc found!
#                     if bu.is_debug():
#                         bu.debug('Duplicate found: --------------------------------', id=workerID)
#                         bu.debug(entry + ' : Old (to be deleted)', id=workerID)
#                         bu.debug(entry_prev + ' : Newer', id=workerID)
#                     await add_stat2del(db, stat_type, doc['_id'], workerID, prune)
#                     rl.log(stat_type + ' duplicates')
#                 account_id_prev = account_id 
#                 entry_prev = entry

#         except Exception as err:
#             bu.error(exception=err, id=workerID)
#         finally:            
#             tankQ.task_done()
#     return rl


# ## DEPRECIATED
# async def analyze_player_achievements_worker_DEPRECIATED(db: motor.motor_asyncio.AsyncIOMotorDatabase, 
#                                              update: dict, workerID: int = None, prune : bool = False)  -> RecordLogger:
#     """Async Worker to fetch player achievement stats"""
#     dbc = db[DB_C_PLAYER_ACHIVEMENTS]
#     stat_type = MODE_PLAYER_ACHIEVEMENTS

#     try:
#         rl      = RecordLogger('Analyze Player Achievements')
#         start   = update['start']
#         end     = update['end']
#         update  = update['update']
        
#         bu.debug('Update: ' + update + ' Start: ' + str(start) + ' End: ' + str(end))
        
#         pipeline = [    {'$match': { '$and': [  {'updated': {'$lte': end}}, {'updated': {'$gt': start}} ] }},
#                         { '$project' : { 'account_id' : 1, 'updated' : 1}},
#                         { '$sort': {'account_id': pymongo.ASCENDING, 'updated': pymongo.DESCENDING} } 
#                     ]
#         cursor = dbc.aggregate(pipeline, allowDiskUse=True)

#         account_id_prev = -1 
#         entry_prev = mk_log_entry(stat_type, { 'account_id': -1}) 
               
#         async for doc in cursor:             
#             bu.print_progress()
#             account_id = doc['account_id']
#             if bu.is_debug():
#                 entry = mk_log_entry(stat_type, { 'account_id' : account_id, 
#                                                   'updated' : doc['updated']})
#             if account_id == account_id_prev:
#                 # Older doc found!
#                 if bu.is_debug():
#                     bu.debug('Duplicate found: --------------------------------', id=workerID)
#                     bu.debug(entry + ' : Old (to be deleted)', id=workerID)
#                     bu.debug(entry_prev + ' : Newer', id=workerID)
#                 await add_stat2del(db, stat_type, doc['_id'], workerID, prune)
#                 rl.log(stat_type + ' duplicates')                
#             account_id_prev = account_id
#             entry_prev = entry 

#     except Exception as err:
#         bu.error('Unexpected Exception', exception=err, id=workerID)
#     return rl


# async def analyze_player_stats_worker(db: motor.motor_asyncio.AsyncIOMotorDatabase, 
#                                       update: dict, workerID: int, prune : bool = False) -> RecordLogger:
#     bu.error('NOT IMPLEMENTED YET')
#     sys.exit(1)


async def check_stats(db: motor.motor_asyncio.AsyncIOMotorDatabase, 
                      updates: list, args: argparse.Namespace = None) -> RecordLogger:
    """Parallel check for the analyzed player achivements duplicates"""
    try:
        rl      = RecordLogger('Check duplicates')        
        archive = args.opt_archive
        sample  = args.sample
        if sample == None:
            sample = DEFAULT_SAMPLE

        for stat_type in args.mode:
            for u in updates:
                try:
                    if u == None:
                        bu.verbose_std('Checking ' + get_mode_str(stat_type) +  ' duplicates. (ALL DATA)')                         
                    else:
                        bu.verbose_std('Checking ' + get_mode_str(stat_type) +  ' duplicates for update ' + u['update'])
        
                    bu.verbose_std('Counting duplicates ... ', eol=False)
                    N_dups = await count_dups2prune(db, stat_type, archive)
                    bu.verbose_std(str(N_dups) + ' found')
                    dupsQ = asyncio.Queue(QUEUE_LEN)
                    fetcher = asyncio.create_task(get_dups_worker(db, stat_type, dupsQ, sample, archive))

                    if (sample > 0) and (sample < N_dups):            
                        header = 'Checking sample of duplicates: ' 
                    else:
                        sample = N_dups
                        header = 'Checking ALL duplicates: '
                    bu.set_progress_bar(header, sample, 100, slow=True)            
                    bu.verbose(header)
                            
                    workers = []
                    for workerID in range(0, N_WORKERS):
                        if stat_type == MODE_TANK_STATS:
                            workers.append(asyncio.create_task( check_dup_tank_stat_worker(db, dupsQ, u, workerID, archive )))
                        elif stat_type == MODE_PLAYER_ACHIEVEMENTS:
                            workers.append(asyncio.create_task( check_dup_player_achievements_worker(db, dupsQ, u, workerID, archive )))
                    
                    await asyncio.wait([fetcher])
                    for rl_task in await asyncio.gather(*[fetcher]):
                        rl.merge(rl_task)

                    await dupsQ.join()
                    for worker in workers:
                        worker.cancel()
                    for rl_worker in await asyncio.gather(*workers):
                        rl.merge(rl_worker)

                    bu.finish_progress_bar()
                except Exception as err:
                    bu.error(exception=err)
    except Exception as err:
        bu.error(exception=err)
    bu.log(rl.print(do_print=False))
    rl.print()
    return None


def get_mode_str(stat_type: str, archive : bool = False) -> str:
    try:
        ret = STR_MODES[stat_type]
        if archive == True:
            return ret + ' (Archive)'
        else:
            return ret
    except Exception as err:
        bu.error(exception=err)
        

# async def check_player_achievements(db: motor.motor_asyncio.AsyncIOMotorDatabase, 
#                                     update_record: dict = None, sample: int = DEFAULT_SAMPLE)  -> RecordLogger:
#     """Parallel check for the analyzed player achievement duplicates"""
#     try:
#         rl = RecordLogger('Check Player Achivements')
        
#         dbc_dups    = db[DB_C_STATS_2_DEL]
#         stat_type   = MODE_PLAYER_ACHIEVEMENTS
        
#         if update_record == None:
#             update_str = 'ALL updates'
#         else: 
#             update_str = 'update ' + update_record['update']

#         bu.verbose_std('Checking Player Achievement duplicates for ' + update_str)
#         bu.verbose_std('Counting duplicates ... ', eol=False)
#         N_dups = await dbc_dups.count_documents({'type': stat_type})
#         bu.verbose_std(str(N_dups) + ' found')
        
#         if (sample > 0) and (sample < N_dups):            
#             header = 'Checking sample of duplicates: ' 
#         else:
#             sample = N_dups
#             header = 'Checking ALL duplicates: '
        
#         if bu.is_normal():
#             bu.set_progress_bar(header, sample, 100, slow=True)            
#         else:
#             bu.verbose_std(header)
                
#         worker_tasks = list()
#         if sample < N_dups:
#             for sub_sample in split_int(sample, N_WORKERS):
#                 worker_tasks.append(asyncio.create_task(check_player_achievement_worker(db, update_record, sub_sample )))
#         else:
#             worker_tasks.append(asyncio.create_task(check_player_achievement_worker(db, update_record)))

#         if len(worker_tasks) > 0:
#             for rl_task in await asyncio.gather(*worker_tasks):
#                 rl.merge(rl_task)
                
#         if bu.is_normal():
#             bu.finish_progress_bar()
#     except Exception as err:
#         bu.error(exception=err)
#     return rl


# async def check_player_achievement_worker(db: motor.motor_asyncio.AsyncIOMotorDatabase, 
#                                           update_record: dict = None, sample: int = 0)  -> RecordLogger:
#     """Worker to check Player Achievement duplicates. Returns results in a dict"""
#     try:
#         rl          = RecordLogger('Check Player Achievements')
#         _id = 'undefined'
#         dbc         = db[DB_C_PLAYER_ACHIVEMENTS]
#         dbc_dups    = db[DB_C_STATS_2_DEL]
#         stat_type   = MODE_PLAYER_ACHIEVEMENTS
        
#         if update_record != None:
#             update  = update_record['update']
#             start   = update_record['start']
#             end     = update_record['end']        
#         else:
#             update = su.UPDATE_ALL
#             start  = 0
#             end    = bu.NOW()

#         pipeline = [ {'$match': { 'type': stat_type}} ]
#         if sample > 0:
#             pipeline.append({'$sample' : {'size': sample }})
#         cursor = dbc_dups.aggregate(pipeline, allowDiskUse=False)
       
#         async for dup in cursor:
#             try:                
#                 _id = dup['id']
#                 if bu.is_normal():   ## since the --verbose causes far more logging 
#                     bu.print_progress()
#                 dup_stat        = await dbc.find_one({'_id': _id})
#                 updated         = dup_stat['updated']
#                 account_id      = dup_stat['account_id']
#                 if updated > end or updated <= start:
#                     bu.verbose('The duplicate not within the defined update. Skipping')
#                     rl.log('Skipped duplicates')
#                     continue
                
#                 bu.verbose(str_dups_player_achievements(update, account_id, updated, is_dup=True))
#                 newer_stat = await dbc.find_one({ '$and': [ {'account_id': account_id}, 
#                                                             {'updated': { '$gt': updated }}, 
#                                                             { 'updated': { '$lte': end }}] })
                
#                 if newer_stat == None:
#                     rl.log('Invalid duplicates')
#                     bu.verbose(str_dups_player_achievements(update, account_id, updated, status='INVALID DUPLICATE: _id=' + _id))                    
#                 else:
#                     rl.log('Valid duplicates')
#                     bu.verbose(str_dups_player_achievements(update, account_id, updated, is_dup=True))                
                                                    
#             except Exception as err:
#                 bu.error('Error checking duplicates. Mode=' + stat_type + ' _id=' + _id, err)
                

#     except Exception as err:
#         bu.error('Mode=' + stat_type + ' _id=' + _id, err)

#     if sample == 0:
#         sample = rl.sum(['Invalid duplicates', 'Valid duplicates', 'Skipped duplicates' ])
#     rl.log('Total', sample)
#     return rl 


# async def check_tank_stats(db: motor.motor_asyncio.AsyncIOMotorDatabase,
#                             updates: list, args: argparse.Namespace = None) -> RecordLogger:
# #                            update: dict, sample: int = DEFAULT_SAMPLE):
#     """Parallel check for the analyzed tank stat duplicates"""
#     try:
#         rl = RecordLogger('Check Tank stats')        
#         dbc_dups    = db[DB_C_STATS_2_DEL]
#         stat_type   = MODE_TANK_STATS        
#         archive = args.opt_archive
 
#         for u in updates:
#             bu.verbose_std('Checking Tank Stats duplicates for update ' + u['update'])
#             bu.verbose_std('Counting duplicates ... ', eol=False)
#             sample  = args.sample
#             N_dups = await dbc_dups.count_documents({'type': stat_type})
#             bu.verbose_std(str(N_dups) + ' found')
#             dupsQ = asyncio.Queue(QUEUE_LEN)
#             dups_worker = asyncio.create_task(get_dups_worker(db, stat_type, dupsQ, sample, archive))

#             if (sample > 0) and (sample < N_dups):            
#                 header = 'Checking sample of duplicates: ' 
#             else:
#                 sample = N_dups
#                 header = 'Checking ALL duplicates: '
            
#             if bu.is_normal():
#                 bu.set_progress_bar(header, sample, 100, slow=True)            
#             else:
#                 bu.verbose_std(header)
                    
#             tasks = []
#             for workerID in range(0, N_WORKERS):
#                 tasks.append(asyncio.create_task( check_dup_tank_stat_worker(db, dupsQ, u, workerID, archive )))
                
#             await dupsQ.join()
#             rl.merge(await asyncio.gather(*[ dups_worker ]))
#             
#             for rl_task in await asyncio.gather(*tasks, ):
#                 rl.merge(rl_task)

#         if bu.is_normal():
#             bu.finish_progress_bar()
#     except Exception as err:
#         bu.error(exception=err)
#         return rl


async def check_dup_tank_stat_worker(db: motor.motor_asyncio.AsyncIOMotorDatabase, 
                                     dupsQ: asyncio.Queue, update_record: dict = None,
                                     ID: int = 0, archive = False,) -> RecordLogger:
    """Worker to check Tank Stats duplicates. Returns results in a dict"""
    try:
        rl = RecordLogger(get_mode_str(MODE_TANK_STATS, archive) + ' duplicates')
        if archive:
            dbc = db[DB_C_ARCHIVE[MODE_TANK_STATS]]
        else:
            dbc = db[DB_C[MODE_TANK_STATS]]
        
        if update_record != None:
            update  = update_record['update']
            start   = update_record['start']
            end     = update_record['end']            
        else:
            if archive:
                bu.error('Trying to check duplicates in the whole Archieve. Must define an update.', id=ID)
                rl.log('CRITICAL ERROR')
                return rl
            update = su.UPDATE_ALL
            start  = 0
            end    = bu.NOW()

        while True:
            dups = await dupsQ.get()
            bu.debug(str(len(dups['ids'])) + ' duplicates fetched from queue', id=ID)
            for _id in dups['ids']:
                try:
                    bu.print_progress()
                    dup_stat  = await dbc.find_one({'_id': _id})
                    if dup_stat == None:
                        rl.log('Not Found')
                        bu.error('Could not find duplicate _id=' + _id, id=ID)
                        continue
                    last_battle_time= dup_stat['last_battle_time']
                    account_id      = dup_stat['account_id']
                    tank_id         = dup_stat['tank_id']

                    if last_battle_time > end or last_battle_time <= start:
                        bu.verbose('The duplicate is not within update ' +  update + '. Skipping')
                        rl.log('Skipped')
                        continue                
                    
                    newer_stat = await dbc.find_one({ '$and': [ {'tank_id': tank_id}, 
                                                                {'account_id': account_id},
                                                                {'last_battle_time': { '$gt': last_battle_time }}, 
                                                                {'last_battle_time': { '$lte': end }}
                                                                ] })
                    if newer_stat == None:
                        rl.log('Invalid')
                        bu.verbose(str_dups_tank_stats(update, account_id, tank_id, last_battle_time, status='INVALID DUPLICATE: _id=' + _id))                    
                    else:
                        rl.log('OK')
                        bu.verbose(str_dups_tank_stats(update, account_id, tank_id, last_battle_time, is_dup=True))
                        last_battle_time= newer_stat['last_battle_time']
                        account_id      = newer_stat['account_id']
                        tank_id         = newer_stat['tank_id']
                        bu.verbose(str_dups_tank_stats(update, account_id, tank_id, last_battle_time, is_dup=None))
                    bu.debug('A duplicate processed', id=ID)
                except Exception as err:
                    rl.log('Errors')
                    bu.error('Error checking ' + get_mode_str(MODE_TANK_STATS, archive) + ' duplicates. _id=' + _id, err, id=ID)
            dupsQ.task_done()
    
    except asyncio.CancelledError as err:
        bu.debug('Cancelling', id=ID)
    except Exception as err:
        bu.error('Mode=' + stat_type, exception=err, id=ID)

    total = rl.sum(['OK', 'Invalid', 'Skipped'])
    rl.log('Total', total)
    return rl
            

async def check_dup_player_achievements_worker(db: motor.motor_asyncio.AsyncIOMotorDatabase, 
                                               dupsQ: asyncio.Queue, update_record: dict = None,
                                               ID: int = 0, archive = False,) -> RecordLogger:
    """Worker to check Player Achivement duplicates. Returns results in a dict"""
    try:
        stat_type   = MODE_PLAYER_ACHIEVEMENTS
        rl          = RecordLogger(get_mode_str(stat_type, archive))
        if archive:
            dbc = db[DB_C_ARCHIVE[stat_type]]
        else:
            dbc = db[DB_C[stat_type]]
        
        if update_record != None:
            update  = update_record['update']
            start   = update_record['start']
            end     = update_record['end']            
        else:
            if archive:
                bu.error('Trying to check duplicates in the whole Archieve. Must define an update.', id=ID)
                rl.log('CRITICAL ERROR')
                return rl
            update = su.UPDATE_ALL
            start  = 0
            end    = bu.NOW()           

        while True:
            dups = await dupsQ.get()
            bu.debug('Duplicate candidate read from the queue', id=ID)
            for _id in dups['ids']:
                try:
                    bu.debug('Checking _id=' + _id, id=ID)
                    bu.print_progress()
                    dup_stat  = await dbc.find_one({'_id': _id})
                    if dup_stat == None:
                        rl.log('Not Found')
                        bu.error('Could not find duplicate _id=' + _id, id=ID)
                        continue

                    updated         = dup_stat['updated']
                    account_id      = dup_stat['account_id']
                    
                    if updated > end or updated <= start:
                        bu.verbose('The duplicate is not within update ' +  update + '. Skipping')
                        rl.log('Skipped')
                        continue
                
                    newer_stat = await dbc.find_one({ '$and': [ {'account_id': account_id},
                                                                {'updated': { '$gt': updated }}, 
                                                                {'updated': { '$lte': end }}
                                                                ] })
                    if newer_stat == None:
                        rl.log('Invalid')
                        bu.error(str_dups_player_achievements(update, account_id, updated, status='INVALID DUPLICATE: _id=' + _id))                    
                    else:
                        rl.log('OK')
                        if bu.is_verbose():
                            bu.verbose(str_dups_player_achievements(update, account_id, updated, is_dup=True))
                            updated         = newer_stat['updated']
                            account_id      = newer_stat['account_id']                    
                            bu.verbose(str_dups_player_achievements(update, account_id, updated, is_dup=None))

                except Exception as err:
                    rl.log('Errors')
                    bu.error('Error checking ' + get_mode_str(MODE_PLAYER_ACHIEVEMENTS, archive) + ' duplicates. _id=' + str(_id), err, id=ID)
            dupsQ.task_done()
    
    except asyncio.CancelledError as err:
        bu.debug('Cancelling', id=ID)
    except Exception as err:
        bu.error('Mode=' + stat_type, err)

    total = rl.sum(['OK', 'Invalid', 'Skipped'])
    rl.log('Total', total)
    return rl
            


def split_int(total:int, N: int) -> list:
    try:
        res = list()
        if N == None or N <= 0 or N > total:
            bu.debug('Invalid argument N')
            return res
        left = total
        for _ in range(N-1):
            sub_total = int(total/N) 
            res.append(sub_total)
            left -= sub_total
        res.append(left)    
    except Exception as err:
        bu.error(exception=err)
    return res


# def print_dups_stats(stat_type: str, dups_total: int, sample: int, dups_ok: int = 0, dups_nok: int = 0, dups_skipped: int= 0):
#     try:
#         sample_str = (str(sample)  +  " (" + '{:.2f}'.format(sample/dups_total*100) + "%)") if sample > 0 else "all"        
#         bu.verbose_std('Total ' + str(dups_total) + ' ' + stat_type +' duplicates. Checked ' + sample_str + " duplicates, skipped " + str(dups_skipped))
#         bu.verbose_std("OK: " + str(dups_ok) + " Errors: " + str(dups_nok))
#         return dups_nok == 0
#     except Exception as err:
#         bu.error(exception=err)


async def find_update(db: motor.motor_asyncio.AsyncIOMotorDatabase, updates : list = None, time: int = -1):
    try:
        if updates == None:
            updates = mk_update_list(db, [ "6.0+" ])
        update = None
        for u in reversed(updates):
            # find the correct update
            if time  > u['start'] and time <= u['end']:
                update  = u                
                break
        return update
    except Exception as err:
        bu.error(exception=err)   


def str_dups_player_achievements(update : str, account_id: int,  updated: int, 
                                 is_dup: bool = None, status: str = None):
    """Return string of duplicate status of player achivement stat"""
    try:
        if status == None:
            if is_dup == None:
                status = 'Newer stat'
            if is_dup:
                status = 'Duplicate' 
            else:
                status = 'NOT DUPLICATE'    
        
        return('Update: {:s} account_id={:<10d} updated={:d} : {:s}'.format(update, account_id, updated, status) )
        
    except Exception as err:
        bu.error(exception=err)
        return "ERROR"


def str_dups_tank_stats(update : str, account_id: int, tank_id: int, last_battle_time: int, is_dup: bool = None, status: str = None):
    try:
        if status == None:
            if is_dup == None:
                status = 'Newer stat'
            if is_dup:
                status = 'Duplicate' 
            else:
                status = 'NOT DUPLICATE'    
        
        return('Update: {:s} account_id={:<10d} tank_id={:<5d} latest_battle_time={:d} : {:s}'.format(update, account_id, tank_id, last_battle_time, status) )
    
    except Exception as err:
        bu.error(exception=err)
        return "ERROR"


async def add_stat2del(db: motor.motor_asyncio.AsyncIOMotorDatabase, stat_type: str, id: str, workerID: int = None,  prune : bool = False) -> int:
    """Adds _id of the stat record to be deleted in into DB_C_STATS_2_DEL"""
    dbc = db[DB_C_STATS_2_DEL]
    dbc2prune = db[DB_C[stat_type]]
    try:
        if prune:
            res = await dbc2prune.delete_one( { '_id': id } )
            return res.deleted_count
        else:
            await dbc.insert_one({'type': stat_type, 'id': id})
            return 1
    except Exception as err:
        bu.error(exception=err, id=workerID)
    return 0


# async def prune_stats_serial(db: motor.motor_asyncio.AsyncIOMotorDatabase, args : argparse.Namespace):
#     """Execute DB pruning and DELETING DATA. Does NOT verify whether there are newer stats"""
#     global STATS_PRUNED
#     try:
#         batch_size = 200
#         dbc_prunelist = db[DB_C_STATS_2_DEL]
#         for stat_type in args.mode:
#             dbc_2_prune = db[DB_C[stat_type]]
#             #DB_FILTER = {'type' : stat_type}
#             stats2prune = await dbc_prunelist.count_documents({'type' : stat_type})
#             bu.debug('Pruning ' + str(stats2prune) + ' ' + stat_type)
#             bu.set_progress_bar(stat_type + ' pruned: ', stats2prune, slow=True)
#             time.sleep(2)
#             cursor = dbc_prunelist.find({'type' : stat_type}).batch_size(batch_size)
#             docs = await cursor.to_list(batch_size)
#             while docs:
#                 ids = set()
#                 for doc in docs:
#                     ids.add(doc['id'])
#                     bu.print_progress()
#                 if len(ids) > 0:
#                     try:
#                         res = await dbc_2_prune.delete_many( { '_id': { '$in': list(ids) } } )
#                         STATS_PRUNED[stat_type] += res.deleted_count
#                     except Exception as err:
#                         bu.error('Failure in deleting ' + stat_type, exception=err)
#                     try:
#                         await dbc_prunelist.delete_many({ 'type': stat_type, 'id': { '$in': list(ids) } })
#                     except Exception as err:
#                         bu.error('Failure in clearing stats-to-be-pruned table')
#                 docs = await cursor.to_list(batch_size)
#             bu.finish_progress_bar()

#     except Exception as err:
#         bu.error(exception=err)
#     return None

async def count_dups2prune(db: motor.motor_asyncio.AsyncIOMotorDatabase, stat_type:str, archive: bool = False) -> int:
    try:
        dbc = db[DB_C_STATS_2_DEL]
        if archive:
            stat_type = stat_type + MODE_ARCHIVE
        return await dbc.count_documents({'type' : stat_type})
    except Exception as err:
        bu.error(exception=err)
    return None


async def prune_stats(db: motor.motor_asyncio.AsyncIOMotorDatabase, updates: list = list(), args : argparse.Namespace = None, stat_type: str = None, pruneQ: asyncio.Queue = None):
    """Parellen DB pruning, DELETES DATA. Does NOT verify whether there are newer stats"""
    #global STATS_PRUNED
    try:
        rl = RecordLogger('Prune')
        archive = args.opt_archive
        sample = args.sample

        ## TWO MODES: from DB, real-time? 
        ## MAKE SURE ARCHIVE IS NEVER EVER PRUNED WITHOUT UPDATE LIMIT

        bu.error('NOT FINALIZED YET')
        sys.exit(1)

        if stat_type == None:
            modes = args.mode
        else:
            modes = [ stat_type ]

        for u in updates: 
            try:
                if u == None:                    
                    update_str = '(ALL DATA)'
                else:
                    update_str = u['update']
                bu.verbose_std('Analyzing ' + get_mode_str(stat_type, archive) + ' for duplicates. Update ' + update_str)
   
                for stat_type in modes:
                    N_stats2prune = await count_dups2prune(db, stat_type, archive)
                    bu.debug('Pruning ' + str(N_stats2prune) + ' ' + get_mode_str(stat_type, archive))            
                    bu.set_progress_bar(get_mode_str(stat_type, archive) + ' pruned: ', N_stats2prune, step = 1000, slow=True)
                    pruneQ = asyncio.Queue(QUEUE_LEN)
                    fetcher = asyncio.create_task(get_dups_worker(db, stat_type, pruneQ, sample, archive=archive))
                    workers = list()
                    for workerID in range(0, N_WORKERS):
                        workers.append(asyncio.create_task(prune_stats_worker(db, stat_type, pruneQ, workerID, archive=archive)))                    

                    await asyncio.wait([fetcher])
                    for rl_task in await asyncio.gather(*[fetcher], return_exceptions=True):
                        rl.merge(rl_task)
                
                    await pruneQ.join()
                    if len(workers) > 0:
                        for worker in workers:
                            worker.cancel()
                        for res_rl in await asyncio.gather(*workers):
                            rl.merge(res_rl)
                    bu.finish_progress_bar()
            except Exception as err:
                bu.error(exception=err)

    except Exception as err:
        bu.error(exception=err)
    bu.log(rl.print(do_print=False))
    rl.print()
    return None


# async def prune_stats_worker_OLD(db: motor.motor_asyncio.AsyncIOMotorDatabase, pruneQ: asyncio.Queue, ID: int = 1) -> dict:
#     """Paraller Worker for pruning stats"""
    
#     bu.error('DEPRECIATED')
#     sys.exit(1)

#     bu.debug('Started', id=ID)
#     rl = RecordLogger('Prune stats')
#     try:
#         dbc_prunelist = db[DB_C_STATS_2_DEL]       
        
#         while True:
#             prune_task  = await pruneQ.get()
#             try:                 
#                 stat_type   = prune_task['stat_type']
#                 ids         = prune_task['ids']
#                 dbc_2_prune = db[DB_C[stat_type]]

#                 for _id in ids:
#                     try:
#                         res = await dbc_2_prune.delete_one( { '_id': _id } )
#                         if res.deleted_count == 1:                        
#                             rl.log(stat_type + ' pruned')
#                             bu.print_progress()
#                         else:
#                             bu.error('Could not find ' + stat_type + ' _id=' + _id)
#                             rl.log('Error: Not found ' + stat_type)
#                         await dbc_prunelist.delete_one({ '$and': [ {'type': stat_type}, {'id': _id }]})
#                     except Exception as err:
#                         rl.log('Error pruning ' + stat_type)
#                         bu.error(exception=err, id=ID)
#             except Exception as err:
#                 bu.error(exception=err, id=ID)
#             pruneQ.task_done()        
#     except (asyncio.CancelledError):
#         bu.debug('Prune queue is empty', id=ID)
#     except Exception as err:
#         bu.error(exception=err, id=ID)
#     return rl


async def prune_stats_worker(db: motor.motor_asyncio.AsyncIOMotorDatabase, 
                             stat_type : str, pruneQ: asyncio.Queue, 
                             update_record: dict = None, ID: int = 0, 
                             archive: bool = False, check=False) -> dict:
    """Paraller Worker for pruning stats"""
    
    try:
        bu.debug('Started', id=ID)
        rl              = RecordLogger(get_mode_str(stat_type, archive))
        dbc_prunelist   = db[DB_C_STATS_2_DEL]       
        if archive:
            dbc_2_prune = db[DB_C_ARCHIVE[stat_type]]
            stat_type   = stat_type + MODE_ARCHIVE
            dbc_check   = None
        else:
            dbc_2_prune = db[DB_C[stat_type]]
            dbc_check   = db[DB_C_ARCHIVE[stat_type]]
        
        if update_record != None:
            update  = update_record['update']
            start   = update_record['start']
            end     = update_record['end']            
        else:
            if archive:
                bu.error('Trying to check duplicates in the whole Archieve. Must define an update.', id=ID)
                rl.log('CRITICAL ERROR')
                return rl
            update = None
            start  = 0
            end    = bu.NOW()

        while True:
            prune_task  = await pruneQ.get()
            try:
                ids     = prune_task['ids']
                from_db = prune_task['from_db']
                
                if check and dbc_check != None:
                    cursor = dbc_check.find( {'_id': { '$in': ids }}) 
                    res = await cursor.to_list(DEFAULT_BATCH)
                    if len(ids) != len(res):
                        bu.error('Not all stats to be pruned can be found from ' + get_mode_str(stat_type, True))
                        rl.log('Archive check failed', len(ids))
                        pruneQ.task_done()
                        continue

                if update != None:
                    if stat_type == su.MODE_TANK_STATS:
                        res = await dbc_2_prune.delete_many({ '$and': [ { '_id': { '$in':  ids} }, 
                                                            { 'last_battle_time': { '$gt': start }} , 
                                                            { 'last_battle_time': { '$lte': end }} ] })
                    elif stat_type == su.MODE_PLAYER_ACHIEVEMENTS:
                        res = await dbc_2_prune.delete_many({ '$and': [ { '_id': { '$in':  ids} }, 
                                                            { 'updated': { '$gt': start }} , 
                                                            { 'updated': { '$lte': end }} ] })
                    else:
                        bu.error('Unsupported stat_type')
                        sys.exit(1)
                else:
                    res = await dbc_2_prune.delete_many( { '_id': { '$in': ids } } )

                not_deleted = len(ids) - res.deleted_count
                rl.log('Pruned', res.deleted_count)
                bu.print_progress(res.deleted_count)
                if not_deleted != 0:
                    cursor = dbc_2_prune.find({ '_id': { '$in': ids }}, { '_id': True } ) 
                    docs_not_deleted = set( await cursor.to_list(DEFAULT_BATCH))
                    docs_deleted = list(set(ids) - docs_not_deleted)
                    bu.error('Could not prune all ' + get_mode_str(stat_type, archive) + ': pruned=' + str(res.deleted_count) + ' NOT pruned=' + str(not_deleted))
                    rl.log('NOT pruned', not_deleted)
                else:
                    docs_deleted = ids
                if from_db:
                    await dbc_prunelist.delete_many({ '$and': [ {'type': stat_type}, {'id': { '$in': docs_deleted } } ]})
                
            except Exception as err:
                bu.error(exception=err, id=ID)
                rl.log('Error')
            
            pruneQ.task_done()        # is executed even when 'continue' is called

    except (asyncio.CancelledError):
        bu.debug('Prune queue is empty', id=ID)
    except Exception as err:
        bu.error(exception=err, id=ID)
    return rl


async def get_tanks_DB(db: motor.motor_asyncio.AsyncIOMotorDatabase, archive=False) -> list:
    """Get tank_ids of tanks in the DB"""
    try:
        if archive: 
            collection = DB_C_ARCHIVE[MODE_TANK_STATS]
        else:
            collection = DB_C[MODE_TANK_STATS]
        dbc = db[collection]
        return sorted(await dbc.distinct('tank_id'))
    except Exception as err:
        bu.error('Could not fetch tank_ids', exception=err)
    return None


async def get_tank_name(db: motor.motor_asyncio.AsyncIOMotorDatabase, tank_id: int) -> str:
    """Get tank name from DB's Tankopedia"""
    try:
        dbc = db[DB_C_TANKS]
        res = await dbc.find_one( { 'tank_id': int(tank_id)}, { '_id': 0, 'name': 1} )
        return res['name']
    except Exception as err:
        bu.debug(exception=err)
    return None


async def get_tanks_opt(db: motor.motor_asyncio.AsyncIOMotorDatabase, option: list = None, archive=False):
    """read option and return tank_ids"""
    try:
        TANK_ID_MAX = int(10e6)
        tank_id_start = TANK_ID_MAX
        tank_ids = set()
        p = re.compile(r'^(\d+)(\+)?$')
        for tank in option:
            try:
                m = p.match(tank).groups()
                if m[0] == None:
                    raise Exception('Invalid tank_id given' + str(tank))
                if m[1] != None:
                    tank_id_start = min(int(m[0]), tank_id_start)
                else:
                    tank_ids.add(int(m[0]))
            except Exception as err:
                bu.error('Invalid tank_id give: ' + tank, exception=err)        
        if tank_id_start < TANK_ID_MAX:            
            all_tanks = await get_tanks_DB(db, archive)
            tank_ids_start = [ tank_id for tank_id in all_tanks if tank_id >= tank_id_start ]
            tank_ids.update(tank_ids_start)        
        return list(tank_ids)
    except Exception as err:
        bu.error('Returning empty list', exception=err)
    return list()


async def archive_player_achivements(db: motor.motor_asyncio.AsyncIOMotorDatabase, args: argparse.Namespace = None):
    bu.error('Not implemented yet: --archive --mode player_achievements')
    return None


async def archive_tank_stats(db: motor.motor_asyncio.AsyncIOMotorDatabase, args: argparse.Namespace = None):
    try:
        dbc                 = db[DB_C[MODE_TANK_STATS]]
        archive_collection  = DB_C_ARCHIVE[MODE_TANK_STATS]
        
        rl = RecordLogger('Archive Tank stats')
        N_updated_stats = await dbc.count_documents({ FIELD_NEW_STATS : { '$exists': True } })
        bu.set_progress_bar('Archiving tank stats', N_updated_stats, step = 1000, slow=True )  ## After MongoDB fixes $merge cursor: https://jira.mongodb.org/browse/DRIVERS-671
        pipeline = [ {'$match': { FIELD_NEW_STATS : { '$exists': True } } },
                    { '$unset': FIELD_NEW_STATS },                                  
                    { '$merge': { 'into': archive_collection, 'on': '_id', 'whenMatched': 'keepExisting' }} ]
        cursor = dbc.aggregate(pipeline, allowDiskUse=True)
        s = 0
        async for _ in cursor:      ## This one does not work yet until MongoDB fixes $merge cursor: https://jira.mongodb.org/browse/DRIVERS-671
            bu.print_progress()
            s +=1
        rl.log('Archived', s) 

        ## Clean the latest stats # TO DO  
        
    except Exception as err:
        bu.error(exception=err)
    finally:
        bu.finish_progress_bar()        
        bu.log(rl.print(do_print=False))
        rl.print()
    return None


async def clean_tank_stats(db: motor.motor_asyncio.AsyncIOMotorDatabase):
    """Clean the Latest stats from older stats"""
    try: 
        dbc      = db[DB_C[MODE_TANK_STATS]]
        rl       = RecordLogger('Clean tank stats')
        q_dirty  = {FIELD_NEW_STATS: { '$exists': True}}
        n_dirty  = await dbc.count_documents(q_dirty)
        
        bu.set_progress_bar('Finding stats to clean', n_dirty, slow=True)
        account_tankQ = await mk_account_tankQ(db)
        pruneQ  = asyncio.Queue(QUEUE_LEN)

        workers = list()
        scanners = list()
        for workerID in range(0,N_WORKERS):
            scanners.append(asyncio.create_task(find_dup_tank_stats_worker(db, account_tankQ, pruneQ, None, workerID, archive=False)))
            workers.append(asyncio.create_task(prune_stats_worker(db, MODE_TANK_STATS, pruneQ, workerID, check=True)))        

        bu.debug('Waiting for workers to finish')
        await account_tankQ.join()
        if len(scanners) > 0:
            for res in await asyncio.gather(*scanners, return_exceptions=True):
                rl.merge(res)
        
        await pruneQ.join()
        bu.debug('Cancelling workers')
        for worker in workers:
            worker.cancel()
        if len(workers) > 0:
            for res in await asyncio.gather(*workers, return_exceptions=True):
                rl.merge(res)          
       
    except Exception as err:
        bu.error(exception=err)
    finally:
        bu.finish_progress_bar()
        rl.print()
        return rl
    
## REVISE ALGO: 
# 1) find only stats with (FIELD_UPDATE)
# 2) make a account_tankQ (not range). use set()
# 3) Find older stats and put to the pruce Q
# 4) Prune
# 5) Unset FIELD_UPDATE
async def find_dup_tank_stats_worker(  db: motor.motor_asyncio.AsyncIOMotorDatabase, 
                                        account_tankQ: asyncio.Queue, dupsQ: asyncio.Queue, 
                                        update_record: dict = None, ID: int = 0, archive = False) -> RecordLogger:
    """Worker to find duplicates to prune"""
    try:
        rl       = RecordLogger('Find tank stat duplicates')
        update = 'N/A'
        if archive:
            dbc      = db[DB_C_ARCHIVE[MODE_TANK_STATS]]
        else:
            dbc      = db[DB_C[MODE_TANK_STATS]]
        
        if update_record != None:
            update  = update_record['update']
            start   = update_record['start']
            end     = update_record['end']        
        else:
            if archive:
                bu.error('CRITICAL !!!! TRYING TO PRUNE OLD TANK STATS FROM ARCHIVE !!!! EXITING...')
                sys.exit(1)
            update = su.UPDATE_ALL

        while not account_tankQ.empty():
            try:
                wp = await account_tankQ.get()
                account_id_min = wp['account_id']['min']
                account_id_max = wp['account_id']['max']
                tank_id        = wp['tank_id']

                bu.debug('tank_id=' + str(tank_id) + ' account_id=' + str(account_id_min) + '-' + str(account_id_max), id=ID)
                match_stage = [ { 'tank_id': tank_id }, 
                                {'account_id': { '$gte': account_id_min}}, 
                                {'account_id': { '$lt' : account_id_max}} ]
                if update_record != None:
                    match_stage.append( {'last_battle_time': {'$gt': start}} )
                    match_stage.append( {'last_battle_time': {'$lte': end}} )

                pipeline = [{ '$match': { '$and': match_stage } }, 
                            { '$sort': { 'last_battle_time': pymongo.DESCENDING } }, 
                            { '$group': { '_id': '$account_id', 
                                            'all_ids': {'$push': '$_id' },
                                            'len': { "$sum": 1 } } },                           
                            { '$match': { 'len': { '$gt': 1 } } }, 
                            { '$project': { 'ids': {  '$slice': [  '$all_ids', 1, '$len' ] } } }
                        ]
                cursor = dbc.aggregate(pipeline, allowDiskUse=True)
                async for res in cursor:
                    await dupsQ.put(mk_dups_Q_entry(res['ids']))
                    rl.log('Found', len(res['ids']))
                bu.print_progress()                    

            except Exception as err:
                bu.error('Update=' + update, exception=err)
            account_tankQ.task_done()

    except Exception as err:
        bu.error(exception=err)
    return rl    


async def find_dup_player_achivements_worker(db: motor.motor_asyncio.AsyncIOMotorDatabase, 
                                        accountQ: asyncio.Queue, dupsQ: asyncio.Queue, 
                                        update_record: dict = None, ID: int = 0, 
                                        archive = False) -> RecordLogger:
    """Worker to find player achivement duplicates to prune"""
    try:
        rl       = RecordLogger('Find player achivement duplicates')
        update = 'N/A'
        if archive:
            dbc      = db[DB_C_ARCHIVE[MODE_PLAYER_ACHIEVEMENTS]]
        else:
            dbc      = db[DB_C[MODE_PLAYER_ACHIEVEMENTS]]
        
        if update_record != None:
            update  = update_record['update']
            start   = update_record['start']
            end     = update_record['end']        
        else:
            if archive:
                bu.error('CRITICAL !!!! TRYING TO PRUNE OLD TANK STATS FROM ARCHIVE !!!! EXITING...')
                sys.exit(1)
            update = su.UPDATE_ALL
        
        while not accountQ.empty():
            try:
                accounts = await accountQ.get()

                match_stage = [ {'account_id': { '$gte': accounts['min']}}, 
                                {'account_id': { '$lt' : accounts['max'] }} ]
                if update_record != None:
                    match_stage.append( {'updated': {'$gt': start}} )
                    match_stage.append( {'updated': {'$lte': end}} )

                pipeline = [{ '$match': { '$and': match_stage } }, 
                            { '$sort': { 'updated': pymongo.DESCENDING } }, 
                            { '$group': { '_id': '$account_id', 
                                            'all_ids': {'$push': '$_id' },
                                            'len': { "$sum": 1 } } },                           
                            { '$match': { 'len': { '$gt': 1 } } }, 
                            { '$project': { 'ids': {  '$slice': [  '$all_ids', 1, '$len' ] } } }
                        ]
                cursor = dbc.aggregate(pipeline, allowDiskUse=True)
                async for res in cursor:
                    n = len(res['ids'])
                    await dupsQ.put(mk_dups_Q_entry(res['ids']))
                    # bu.print_progress(n)
                    rl.log('Found', n)                    
                bu.print_progress()
            except Exception as err:
                bu.error('Update=' + update, exception=err)
            finally:
                accountQ.task_done()

    except Exception as err:
        bu.error(exception=err)
    return rl  


async def snapshot_player_achivements(db: motor.motor_asyncio.AsyncIOMotorDatabase, args: argparse.Namespace = None):
    try:
        bu.verbose_std('Creating a snapshot of the latest player achievements')
        rl = RecordLogger('Snapshot')
        
        accountQ = await mk_accountQ()
        bu.set_progress_bar('Stats processed:', accountQ.qsize(), step=2, slow=True)  
        workers = list()
        for workerID in range(0, N_WORKERS):
            workers.append(asyncio.create_task(snapshot_player_achivements_worker(db, accountQ, workerID)))

        await accountQ.join()

        bu.finish_progress_bar()
        for rl_worker in await asyncio.gather(*workers):
            rl.merge(rl_worker)

    except Exception as err:
        bu.error(exception=err)
    bu.log(rl.print(do_print=False))
    rl.print()
    return None


async def snapshot_player_achivements_worker(db: motor.motor_asyncio.AsyncIOMotorDatabase, 
                                             accountQ: asyncio.Queue, ID: int = 0 ) -> RecordLogger:  
    """Worker to snapshot tank stats"""  
    try:
        target_collection = DB_C_PLAYER_ACHIVEMENTS
        dbc_archive       = db[DB_C_ARCHIVE[MODE_PLAYER_ACHIEVEMENTS]]
        rl                = RecordLogger(get_mode_str(MODE_PLAYER_ACHIEVEMENTS))

        while not accountQ.empty():
            try:
                wp = await accountQ.get()
                account_id_min = wp['min']
                account_id_max = wp['max']
                
                pipeline = [ {'$match': { '$and': [  {'account_id': {'$gte': account_id_min}}, {'account_id': {'$lt': account_id_max}} ] }},
                             {'$sort': {'updated': pymongo.DESCENDING}},
                             {'$group': { '_id': '$account_id',
                                          'doc': {'$first': '$$ROOT'}}},
                             {'$replaceRoot': {'newRoot': '$doc'}}, 
                             {'$merge': { 'into': target_collection, 'on': '_id', 'whenMatched': 'keepExisting' }} ]
                cursor = dbc_archive.aggregate(pipeline, allowDiskUse=True)
                # s = 0
                async for _ in cursor:      
                    # s += 1   ## This one does not work yet until MongoDB fixes $merge cursor: https://jira.mongodb.org/browse/DRIVERS-671
                    pass
                n = await dbc_archive.count_documents({'$match': { '$and': [  {'account_id': {'$gte': account_id_min}}, {'account_id': {'$lt': account_id_max}} ] }} )                
                rl.log('Snapshotted', n)
                bu.debug('account_id range: ' + str(account_id_min) + '-' + str(account_id_max) + ' processed', id=ID)
                bu.print_progress()
            except Exception as err:
                bu.error(exception=err, id=ID)
            accountQ.task_done()

    except Exception as err:
        bu.error(exception=err, id=ID)
    return rl


async def snapshot_tank_stats(db: motor.motor_asyncio.AsyncIOMotorDatabase, args: argparse.Namespace = None):
    try:
        bu.verbose_std('Creating a snapshot of the latest tank stats')
        rl = RecordLogger('Snapshot')
        if args.opt_tanks != None:
            tank_ids = await get_tanks_opt(db, args.opt_tanks, archive=True)
        else:
            tank_ids = None
        account_tankQ = await mk_account_tankQ(db, tank_ids, archive=True)
        bu.set_progress_bar('Stats processed:', account_tankQ.qsize(), step=50, slow=True)  
        workers = list()
        for workerID in range(0, N_WORKERS):
            workers.append(asyncio.create_task(snapshot_tank_stats_worker(db, account_tankQ, workerID)))

        await account_tankQ.join()

        bu.finish_progress_bar()
        for rl_worker in await asyncio.gather(*workers):
            rl.merge(rl_worker)

    except Exception as err:
        bu.error(exception=err)
    bu.log(rl.print(do_print=False))
    rl.print()
    return None
    

async def snapshot_tank_stats_worker(db: motor.motor_asyncio.AsyncIOMotorDatabase, 
                                     account_tankQ: asyncio.Queue, ID: int = 0 ) -> RecordLogger:  
    """Worker to snapshot tank stats"""  
    try:
        target_collection = DB_C_TANK_STATS
        dbc_archive       = db[DB_C_ARCHIVE[MODE_TANK_STATS]]
        rl                = RecordLogger(get_mode_str(MODE_TANK_STATS))

        while not account_tankQ.empty():
            try:
                wp = await account_tankQ.get()
                account_id_min = wp['account_id']['min']
                account_id_max = wp['account_id']['max']
                tank_id        = wp['tank_id']

                if bu.is_verbose(True):                                        
                    tank_name = await get_tank_name(db, tank_id)
                    if tank_name == None:
                        tank_name = 'Tank name not found'                    
                    info_str = 'Processing tank (' + tank_name + ' (' +  str(tank_id) + '):'
                    bu.log(info_str)
                
                pipeline = [ {'$match': { '$and': [ {'tank_id': tank_id }, {'account_id': {'$gte': account_id_min}}, {'account_id': {'$lt': account_id_max}} ] }},
                             {'$sort': {'last_battle_time': pymongo.DESCENDING}},
                             {'$group': { '_id': '$account_id',
                                          'doc': {'$first': '$$ROOT'}}},
                             {'$replaceRoot': {'newRoot': '$doc'}}, 
                             {'$merge': { 'into': target_collection, 'on': '_id', 'whenMatched': 'keepExisting' }} ]
                cursor = dbc_archive.aggregate(pipeline, allowDiskUse=True)
                # s = 0
                async for _ in cursor:      
                    # s += 1   ## This one does not work yet until MongoDB fixes $merge cursor: https://jira.mongodb.org/browse/DRIVERS-671
                    pass
                # rl.log('Snapshotted', s)
                n = await dbc_archive.count_documents({'$match': { '$and': [ {'tank_id': tank_id }, {'account_id': {'$gte': account_id_min}}, {'account_id': {'$lt': account_id_max}} ] }} )                
                rl.log('Snapshotted', n)
                bu.print_progress()
            except Exception as err:
                bu.error(exception=err, id=ID)
            account_tankQ.task_done()

    except Exception as err:
        bu.error(exception=err, id=ID)
    return rl


# def mk_log_entry(stat_type: str = None, account_id=None, last_battle_time=None, tank_id = None):
def mk_log_entry(stat_type: str = None, stats: dict = None):
    try:
        entry = stat_type + ': '
        for key in stats:
            entry = entry + ' : ' + key + '=' + str(stats[key])
        return entry
    except Exception as err:
        bu.error(exception=err)
        return None


# main()
if __name__ == "__main__":
    #asyncio.run(main(sys.argv[1:]), debug=True)
    asyncio.run(main(sys.argv[1:]))
