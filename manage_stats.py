#!/usr/bin/env python3

# Script Prune stats from the DB per release 

import sys, os, argparse, datetime, json, inspect, pprint, aiohttp, asyncio, aiofiles
import aioconsole, re, logging, time, xmltodict, collections, pymongo, motor.motor_asyncio
import ssl, configparser
from datetime import date
import blitzutils as bu
from blitzutils import BlitzStars, RecordLogger

N_WORKERS = 4

logging.getLogger("asyncio").setLevel(logging.DEBUG)

FILE_CONFIG = 'blitzstats.ini'

DB_C_ARCHIVE            = '_archive'
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


MODE_TANK_STATS         = 'tank_stats'
MODE_PLAYER_STATS       = 'player_stats'
MODE_PLAYER_ACHIEVEMENTS= 'player_achievements'

FIELD_UPDATED = '_updated'

DB_C = {    MODE_TANK_STATS             : DB_C_TANK_STATS, 
            MODE_PLAYER_STATS           : DB_C_PLAYER_STATS,
            MODE_PLAYER_ACHIEVEMENTS    : DB_C_PLAYER_ACHIVEMENTS 
        }

DB_C_ARCHIVE = dict()
for mode in DB_C:
    DB_C_ARCHIVE[mode] = DB_C[mode] + DB_C_ARCHIVE

CACHE_VALID = 24*3600*7   # 7 days
DEFAULT_SAMPLE = 1000

bs = None

TODAY = datetime.datetime.utcnow().date()
DEFAULT_DAYS_DELTA = datetime.timedelta(days=90)
DATE_DELTA = datetime.timedelta(days=7)
STATS_START_DATE = datetime.datetime(2014,1,1)

STATS_PRUNED = dict()
DUPS_FOUND = dict()

for stat_type in DB_C.keys():
    STATS_PRUNED[stat_type]  = 0
    DUPS_FOUND[stat_type]    = 0

def def_value_zero():
    return 0

# main() -------------------------------------------------------------


async def main(argv):
    # set the directory for the script
    current_dir = os.getcwd()
    os.chdir(os.path.dirname(sys.argv[0]))

    parser = argparse.ArgumentParser(description='Manage DB stats')
    parser.add_argument('--mode', default=['tank_stats'], nargs='+', choices=DB_C.keys(), help='Select type of stats to export')
    
    arggroup_action = parser.add_mutually_exclusive_group(required=True)
    arggroup_action.add_argument( '--analyze', action='store_true', default=False, help='Analyze the database for duplicates')
    arggroup_action.add_argument( '--check', 	action='store_true', default=False, help='Check the analyzed duplicates')
    arggroup_action.add_argument( '--prune', 	action='store_true', default=False, help='Prune database for the analyzed duplicates i.e. DELETE DATA')
    arggroup_action.add_argument( '--snapshot',	action='store_true', default=False, help='Snapshot latest stats from the archive')
    arggroup_action.add_argument( '--archive',	action='store_true', default=False, help='Archive latest stats')
    
    parser.add_argument('--opt_tanks', default=None, nargs='*', type=str, help='List of tank_ids for other options. Use "tank_id+" to start from a tank_id')

    arggroup_verbosity = parser.add_mutually_exclusive_group()
    arggroup_verbosity.add_argument( '-d', '--debug', 	action='store_true', default=False, help='Debug mode')
    arggroup_verbosity.add_argument( '-v', '--verbose', action='store_true', default=False, help='Verbose mode')
    arggroup_verbosity.add_argument( '-s', '--silent', 	action='store_true', default=False, help='Silent mode')

    parser.add_argument('--sample', type=int, default=DEFAULT_SAMPLE, help='Sample size. Default=' + str(DEFAULT_SAMPLE) + ' . 0: check ALL.')
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

        await db[DB_C_STATS_2_DEL].create_index('id')	

        #if args.analyze or args.check:
        #    updates = await mk_update_list(db, args.updates)
        updates = await mk_update_list(db, args.updates)

        if args.analyze:
            tasks = []
            tankQ = None
            bu.verbose_std('Starting to analyse stats in 3 seconds. Press CTRL + C to CANCEL')
            bu.wait(3)
            for u in updates: 
                bu.verbose_std('Processing update ' + u['update'] + ':' + ', '.join(args.mode))
                bu.set_counter('Stats processed: ')   

                if  MODE_TANK_STATS in args.mode:
                    tankQ    = await mk_tankQ(db)

                workers = 0
                # Does not work in parallel        
                if MODE_PLAYER_ACHIEVEMENTS in args.mode:
                    tasks.append(asyncio.create_task(analyze_player_achievements_WG(workers, db, u, args.prune)))
                    bu.debug('Task ' + str(workers) + ' started: analyze_player_achievements_WG()')
                    workers += 1
                if MODE_PLAYER_STATS in args.mode:
                    # NOT IMPLEMENTED YET
                    tasks.append(asyncio.create_task(analyze_player_stats_WG(workers, db, u, args.prune)))
                    bu.debug('Task ' + str(workers) + ' started: analyze_player_stats_WG()')
                    workers += 1
                while workers < N_WORKERS:
                    if MODE_TANK_STATS in args.mode:
                        tasks.append(asyncio.create_task(analyze_tank_stats_WG(workers, db, u, tankQ, args.prune)))
                        bu.debug('Task ' + str(workers) + ' started: analyze_tank_stats_WG()')
                    workers += 1    # Can do this since only MODE_TANK_STATS is running in parallel                   
                
                bu.debug('Waiting for workers to finish')
                await asyncio.wait(tasks)       
                bu.debug('Cancelling workers')
                for task in tasks:
                    task.cancel()
                bu.debug('Waiting for workers to cancel')
                if len(tasks) > 0:
                    await asyncio.gather(*tasks, return_exceptions=True)
                
                bu.finish_progress_bar()
                print_stats_analyze(args.mode)
        
        elif args.check:
            for u in updates:                
                if MODE_PLAYER_ACHIEVEMENTS in args.mode:
                    if not await check_player_achievements(db, u, args.sample):
                        bu.error('Error in checking Player Achievement duplicates.')
                        raise KeyboardInterrupt()
                if MODE_TANK_STATS in args.mode:
                    if not await check_tank_stats(db, u, args.sample):
                        bu.error('Error in checking Tank Stats duplicates.') 
                        raise KeyboardInterrupt()
        
        elif args.prune:
            # do the actual pruning and DELETE DATA
            bu.verbose_std('Starting to prune in 3 seconds. Press CTRL + C to CANCEL')
            bu.wait(3)
            await prune_stats(db, args)
        
        elif args.snapshot:
            bu.verbose_std('Starting to snapshot stats in 3 seconds. Press CTRL 0 C to CANCEL')
            bu.wait(3)
            if MODE_PLAYER_ACHIEVEMENTS in args.mode:
                await snapshot_player_achivements(db, args)
            if MODE_TANK_STATS in args.mode:
                await snapshot_tank_stats(db, args)

        elif args.archive:
            bu.verbose_std('Starting to archive stats in 3 seconds. Press CTRL 0 C to CANCEL')
            bu.wait(3)
            if MODE_PLAYER_ACHIEVEMENTS in args.mode:
                await archive_player_achivements(db, args)
            if MODE_TANK_STATS in args.mode:
                await archive_tank_stats(db, args)
             
    except KeyboardInterrupt:
        bu.finish_progress_bar()
        bu.verbose_std('\nExiting..')
    except asyncio.CancelledError as err:
        bu.error('Queue gets cancelled while still working.')
    except Exception as err:
        bu.error('Unexpected Exception', exception=err)

    return None


def mk_update_entry(update: str, start: int, end: int):
    """Make update entry to the update list to process"""
    if (end == None) or (end == 0):
        end = bu.NOW()
    return {'update': update, 'start': start, 'end': end}


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
        bu.verbose_std('Selecting the latest update')
        return [ await get_latest_update(db) ]
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


def print_stats_analyze(stat_types : list = list()):
    for stat_type in stat_types:
        if DUPS_FOUND[stat_type] == 0:
            bu.verbose_std(stat_type + ': No duplicates found')
        else:    
            bu.verbose_std(stat_type + ': ' + str(DUPS_FOUND[stat_type]) + ' new duplicates found')
        DUPS_FOUND[stat_type] = 0
    

def print_stats_prune(stats_pruned : dict):
    """Print end statistics of the pruning operation"""
    try:
        for stat_type in stats_pruned:
            bu.verbose_std(stat_type + ': ' + str(stats_pruned[stat_type]) + ' duplicates removed')
            stats_pruned[stat_type] = 0
    except Exception as err:
        bu.error(exception=err)
    return None    


async def analyze_tank_stats_WG(workerID: int, db: motor.motor_asyncio.AsyncIOMotorDatabase, update: dict, tankQ: asyncio.Queue, prune : bool = False):
    """Async Worker to fetch player tank stats"""
    dbc = db[DB_C_TANK_STATS]
    stat_type = MODE_TANK_STATS

    try:
        start   = update['start']
        end     = update['end']
        update  = update['update']
    except Exception as err:
        bu.error(exception=err, id=workerID)
        return None    

    while not tankQ.empty():
        try:
            tank_id = await tankQ.get()
            bu.debug(str(tankQ.qsize())  + ' tanks to process', id=workerID)
                
            pipeline = [    {'$match': { '$and': [  {'tank_id': tank_id }, {'last_battle_time': {'$lte': end}}, {'last_battle_time': {'$gt': start}} ] }},
                            { '$project' : { 'account_id' : 1, 'tank_id' : 1, 'last_battle_time' : 1}},
                            { '$sort': {'account_id': 1, 'last_battle_time': -1} }
                        ]
            cursor = dbc.aggregate(pipeline, allowDiskUse=True)

            account_id_prev = -1
            entry_prev = mk_log_entry(stat_type, { 'account_id': -1})        
            dups_counter = 0
            async for doc in cursor:
                bu.print_progress()
                account_id = doc['account_id']
                if bu.is_debug():
                    entry = mk_log_entry(stat_type, { 'account_id' : account_id, 'last_battle_time' : doc['last_battle_time'], 'tank_id' : doc['tank_id']})
                if account_id == account_id_prev:
                    # Older doc found!
                    if bu.is_debug():
                        bu.debug('Duplicate found: --------------------------------', id=workerID)
                        bu.debug(entry + ' : Old (to be deleted)', id=workerID)
                        bu.debug(entry_prev + ' : Newer', id=workerID)
                    await add_stat2del(workerID, db, stat_type, doc['_id'], prune)
                    dups_counter += 1
                account_id_prev = account_id 
                if bu.is_debug():
                    entry_prev = entry

        except Exception as err:
            bu.error(exception=err, id=workerID)
        finally:
            bu.debug('Tank_id=' + str(tank_id) + ' processed: ' + str(dups_counter) + ' duplicates found', id = workerID)
            tankQ.task_done()

    return None


async def analyze_player_achievements_WG(workerID: int, db: motor.motor_asyncio.AsyncIOMotorDatabase, update: dict, prune : bool = False):
    """Async Worker to fetch player achievement stats"""
    dbc = db[DB_C_PLAYER_ACHIVEMENTS]
    stat_type = MODE_PLAYER_ACHIEVEMENTS

    try:
        start   = update['start']
        end     = update['end']
        update  = update['update']

        bu.debug('Update: ' + update + ' Start: ' + str(start) + ' End: ' + str(end))
        
        pipeline = [    {'$match': { '$and': [  {'updated': {'$lte': end}}, {'updated': {'$gt': start}} ] }},
                        { '$project' : { 'account_id' : 1, 'updated' : 1}},
                        { '$sort': {'account_id': 1, 'updated': -1} } 
                    ]
        cursor = dbc.aggregate(pipeline, allowDiskUse=True)

        account_id_prev = -1 
        entry_prev = mk_log_entry(stat_type, { 'account_id': -1}) 
        dups_counter = 0       
        async for doc in cursor:    
            if bu.is_normal():
                bu.print_progress()
            account_id = doc['account_id']
            if bu.is_debug():
                entry = mk_log_entry(stat_type, { 'account_id' : account_id, 'updated' : doc['updated']})
            if account_id == account_id_prev:
                # Older doc found!
                if bu.is_debug():
                    bu.debug('Duplicate found: --------------------------------', id=workerID)
                    bu.debug(entry + ' : Old (to be deleted)', id=workerID)
                    bu.debug(entry_prev + ' : Newer', id=workerID)
                await add_stat2del(workerID, db, stat_type, doc['_id'], prune)
                dups_counter += 1                
            account_id_prev = account_id
            if bu.is_debug():
                entry_prev = entry 

    except Exception as err:
        bu.error('Unexpected Exception', exception=err, id=workerID)
    finally:
        bu.debug( stat_type + ': ' + str(dups_counter) + ' duplicates found for update ' + update, id = workerID)          

    return None


async def analyze_player_stats_WG(workerID: int, db: motor.motor_asyncio.AsyncIOMotorDatabase, update: dict, prune : bool = False):
    bu.error('NOT IMPLEMENTED YET')
    pass


async def check_player_achievements_serial(db: motor.motor_asyncio.AsyncIOMotorDatabase, update_record: dict, sample: int = DEFAULT_SAMPLE) -> bool:
    """Check analyzed player achievement duplicates"""
    try:
        dbc_dups    = db[DB_C_STATS_2_DEL]
        dbc         = db[DB_C_PLAYER_ACHIVEMENTS]
        stat_type   = MODE_PLAYER_ACHIEVEMENTS
        start   = update_record['start']
        end     = update_record['end']
        update  = update_record['update']
        
        bu.verbose_std('Checking Player Achievements duplicates for update ' + update)
        bu.verbose_std('Counting duplicates ... ', eol=False)
        N_dups = await dbc_dups.count_documents({'type': stat_type})
        bu.verbose_std(str(N_dups) + ' found')
        pipeline = [ {'$match': { 'type': stat_type}} ]
        if (sample > 0) and (sample < N_dups):
            pipeline.append({'$sample' : {'size': sample }})
            sample_str = str(sample)
        else:
            sample = 0
            sample_str = 'ALL'

        header = 'Taking sample: ' + sample_str + ' : '
        
        if bu.is_normal():
            bu.set_counter(header)
            bu.set_progress_step(100)
        else:
            bu.verbose_std(header)

        cursor = dbc_dups.aggregate(pipeline, allowDiskUse=False)
        id = None
        dups_ok      = 0
        dups_nok     = 0
        dups_skipped = 0
       
        async for dup in cursor:
            try:                
                id = dup['id']
                if bu.is_normal():
                    bu.print_progress()
                dup_stat    = await dbc.find_one({'_id': id})
                updated     = dup_stat['updated']
                account_id  = dup_stat['account_id']
                if updated > end or updated <= start:
                    bu.verbose('Sampled an duplicate not in the defined update. Skipping')
                    dups_skipped += 1
                    continue
                
                bu.verbose(str_dups_player_achievements(update, account_id, updated, is_dup=True))
                cursor_stats = dbc.find({ '$and': [ {'account_id': account_id}, {'updated': { '$gt': updated }}, 
                                                { 'updated': { '$lt': end }}] }).sort('updated', pymongo.ASCENDING )
                dup_count = 0
                async for stat in cursor_stats:
                    updated     = stat['updated']
                    bu.verbose(str_dups_player_achievements(update, account_id, updated))
                    dup_count += 1
                if dup_count == 0:
                    dups_nok += 1
                    bu.verbose("NO DUPLICATE FOUND FOR: account_id=" + str(account_id) + " updated=" + str(updated) + " _id=" + id) 
                else:
                    dups_ok += 1    
                
            except Exception as err:
                bu.error('Error checking duplicates. Mode=' + stat_type + ' _id=' + id, err)
        if bu.is_normal():
            bu.finish_progress_bar()
        print_dups_stats(stat_type, N_dups, sample, dups_ok, dups_nok, dups_skipped)
        return dups_nok == 0
    except Exception as err:
        bu.error(exception=err)
        return False


async def check_player_achievements(db: motor.motor_asyncio.AsyncIOMotorDatabase, update: dict, sample: int = DEFAULT_SAMPLE):
    """Parallel check for the analyzed player achievement duplicates"""
    try:
        dups_ok      = 0
        dups_nok     = 0
        dups_skipped = 0
        dups_total   = 0
        
        dbc_dups    = db[DB_C_STATS_2_DEL]
        stat_type   = MODE_PLAYER_ACHIEVEMENTS
        
        bu.verbose_std('Checking Player Achievement duplicates for update ' + update['update'])
        bu.verbose_std('Counting duplicates ... ', eol=False)
        N_dups = await dbc_dups.count_documents({'type': stat_type})
        bu.verbose_std(str(N_dups) + ' found')
        
        if (sample > 0) and (sample < N_dups):            
            header = 'Checking sample of duplicates: ' 
        else:
            sample = N_dups
            header = 'Checking ALL duplicates: '
        
        if bu.is_normal():
            bu.set_progress_bar(header, sample, 100, slow=True)            
        else:
            bu.verbose_std(header)
                
        worker_tasks = list()
        if sample < N_dups:
            for sub_sample in split_int(sample, N_WORKERS):
                worker_tasks.append(asyncio.create_task(check_player_achievement_worker(db, update, sub_sample )))
        else:
            worker_tasks.append(asyncio.create_task(check_player_achievement_worker(db, update)))

        if len(worker_tasks) > 0:
            for res in await asyncio.gather(*worker_tasks):
                dups_ok      += res['ok']
                dups_nok     += res['nok']
                dups_skipped += res['skipped']
                dups_total   += res['total']

        if bu.is_normal():
            bu.finish_progress_bar()
        print_dups_stats(stat_type, N_dups, sample, dups_ok, dups_nok, dups_skipped)
        return dups_nok == 0
    except Exception as err:
        bu.error(exception=err)
        return False


async def check_player_achievement_worker(db: motor.motor_asyncio.AsyncIOMotorDatabase, update_record: dict, sample: int = 0) -> dict:
    """Worker to check Player Achievement duplicates. Returns results in a dict"""
    try:
        id = None
        dups_ok      = 0
        dups_nok     = 0
        dups_skipped = 0
        
        dbc         = db[DB_C_PLAYER_ACHIVEMENTS]
        dbc_dups    = db[DB_C_STATS_2_DEL]
        stat_type   = MODE_PLAYER_ACHIEVEMENTS
        
        start   = update_record['start']
        end     = update_record['end']
        update  = update_record['update']

        pipeline = [ {'$match': { 'type': stat_type}} ]
        if sample > 0:
            pipeline.append({'$sample' : {'size': sample }})
        cursor = dbc_dups.aggregate(pipeline, allowDiskUse=False)
       
        async for dup in cursor:
            try:                
                id = dup['id']
                if bu.is_normal():   ## since the --verbose causes far more logging 
                    bu.print_progress()
                dup_stat        = await dbc.find_one({'_id': id})
                updated         = dup_stat['updated']
                account_id      = dup_stat['account_id']
                if updated > end or updated <= start:
                    bu.verbose('Sampled an duplicate not in the defined update. Skipping')
                    dups_skipped += 1
                    continue
                
                bu.verbose(str_dups_player_achievements(update, account_id, updated, is_dup=True))
                cursor_stats = dbc.find({ '$and': [ {'account_id': account_id}, {'updated': { '$gt': updated }}, 
                                                    { 'updated': { '$lt': end }}] }).sort('updated', pymongo.ASCENDING )
                dup_count = 0
                async for stat in cursor_stats:
                    updated     = stat['updated']
                    bu.verbose(str_dups_player_achievements(update, account_id, updated))
                    dup_count = 1  # on purpose! 
                    
                if dup_count == 0:
                    dups_nok += 1
                    bu.verbose("NO DUPLICATE FOUND FOR: account_id=" + str(account_id) +  " updated=" + str(updated) + " _id=" + id) 
                else:
                    dups_ok += 1                        
                                    
            except Exception as err:
                bu.error('Error checking duplicates. Mode=' + stat_type + ' _id=' + str(id), err)
                return None

    except Exception as err:
        bu.error('Mode=' + stat_type + ' _id=' + str(id), err)
        return None

    if sample == 0:
        sample = dups_ok + dups_nok + dups_skipped
    return {'total': sample, 'ok': dups_ok, 'nok': dups_nok, 'skipped': dups_skipped, 'stat_type': stat_type }


async def check_tank_stats_serial(db: motor.motor_asyncio.AsyncIOMotorDatabase, update_record: dict, sample: int = DEFAULT_SAMPLE):
    """Check analyzed tank stat duplicates"""
    try:
        dbc_dups    = db[DB_C_STATS_2_DEL]
        dbc         = db[DB_C_TANK_STATS]
        stat_type   = MODE_TANK_STATS
        start   = update_record['start']
        end     = update_record['end']
        update  = update_record['update']
        
        bu.verbose_std('Checking Tank Stats duplicates for update ' + update)
        bu.verbose_std('Counting duplicates ... ', eol=False)
        N_dups = await dbc_dups.count_documents({'type': stat_type})
        bu.verbose_std(str(N_dups) + ' found')
        pipeline = [ {'$match': { 'type': stat_type}} ]
        if (sample > 0) and (sample < N_dups):
            pipeline.append({'$sample' : {'size': sample }})
            sample_str = str(sample)
        else:
            sample = 0
            sample_str = 'ALL'
        
        header = 'Taking sample: ' + sample_str + ' : '
        if bu.is_normal():
            bu.set_counter(header)
            bu.set_progress_step(100)
        else:
            bu.verbose_std(header)

        ## Async TBD
        # for worker
        #  check_worker(update, sample / N_worker)
        # join results
        cursor = dbc_dups.aggregate(pipeline, allowDiskUse=False)
        id = None
        dups_ok      = 0
        dups_nok     = 0
        dups_skipped = 0
       
        async for dup in cursor:
            try:                
                id = dup['id']
                if bu.is_normal():
                    bu.print_progress()
                dup_stat    = await dbc.find_one({'_id': id})
                last_battle_time= dup_stat['last_battle_time']
                account_id      = dup_stat['account_id']
                tank_id         = dup_stat['tank_id']
                if last_battle_time > end or last_battle_time <= start:
                    bu.verbose('Sampled an duplicate not in the defined update. Skipping')
                    dups_skipped += 1
                    continue
                
                bu.verbose(str_dups_tank_stats(update, account_id, tank_id, last_battle_time, is_dup=True))
                cursor_stats = dbc.find({ '$and': [ {'account_id': account_id}, {'tank_id': tank_id}, 
                                                    {'last_battle_time': { '$gt': last_battle_time }}, { 'last_battle_time': { '$lt': end }}] }
                                                    ).sort('last_battle_time', pymongo.ASCENDING )
                dup_count = 0
                async for stat in cursor_stats:
                    last_battle_time     = stat['last_battle_time']
                    bu.verbose(str_dups_player_achievements(update, account_id, last_battle_time))
                    dup_count += 1
                if dup_count == 0:
                    dups_nok += 1
                    bu.verbose("NO DUPLICATE FOUND FOR: account_id=" + str(account_id) +  ' tank_id=' + str(tank_id) + " last_battle_time=" + str(last_battle_time) + " _id=" + id) 
                else:
                    dups_ok += 1                        
                                    
            except Exception as err:
                bu.error('Error checking duplicates. Mode=' + stat_type + ' _id=' + id, err)
        if bu.is_normal():
            bu.finish_progress_bar()
        print_dups_stats(stat_type, N_dups, sample, dups_ok, dups_nok, dups_skipped)
        return dups_nok == 0
    except Exception as err:
        bu.error(exception=err)
        return False


async def check_tank_stats(db: motor.motor_asyncio.AsyncIOMotorDatabase, update: dict, sample: int = DEFAULT_SAMPLE):
    """Parallel check for the analyzed tank stat duplicates"""
    try:
        dups_ok      = 0
        dups_nok     = 0
        dups_skipped = 0
        dups_total   = 0
        
        dbc_dups    = db[DB_C_STATS_2_DEL]
        stat_type   = MODE_TANK_STATS
        
        bu.verbose_std('Checking Tank Stats duplicates for update ' + update['update'])
        bu.verbose_std('Counting duplicates ... ', eol=False)
        N_dups = await dbc_dups.count_documents({'type': stat_type})
        bu.verbose_std(str(N_dups) + ' found')
        
        if (sample > 0) and (sample < N_dups):            
            header = 'Checking sample of duplicates: ' 
        else:
            sample = N_dups
            header = 'Checking ALL duplicates: '
        
        if bu.is_normal():
            bu.set_progress_bar(header, sample, 100, slow=True)            
        else:
            bu.verbose_std(header)
                
        worker_tasks = list()
        if sample < N_dups:
            for sub_sample in split_int(sample, N_WORKERS):
                worker_tasks.append(asyncio.create_task(check_tank_stat_worker(db, update, sub_sample )))
        else:
            worker_tasks.append(asyncio.create_task(check_tank_stat_worker(db, update)))
 
        if len(worker_tasks) > 0:
            for res in await asyncio.gather(*worker_tasks):
                dups_ok      += res['ok']
                dups_nok     += res['nok']
                dups_skipped += res['skipped']
                dups_total   += res['total']

        if bu.is_normal():
            bu.finish_progress_bar()
        print_dups_stats(stat_type, N_dups, sample, dups_ok, dups_nok, dups_skipped)
        return dups_nok == 0
    except Exception as err:
        bu.error(exception=err)
        return False


async def check_tank_stat_worker(db: motor.motor_asyncio.AsyncIOMotorDatabase, update_record: dict, sample: int = 0) -> dict:
    """Worker to check Tank Stats duplicates. Returns results in a dict"""
    try:
        id = None
        dups_ok      = 0
        dups_nok     = 0
        dups_skipped = 0
        
        dbc         = db[DB_C_TANK_STATS]
        dbc_dups    = db[DB_C_STATS_2_DEL]
        stat_type   = MODE_TANK_STATS
        
        start   = update_record['start']
        end     = update_record['end']
        update  = update_record['update']

        pipeline = [ {'$match': { 'type': stat_type}} ]
        if sample > 0:
            pipeline.append({'$sample' : {'size': sample }})
        cursor = dbc_dups.aggregate(pipeline, allowDiskUse=False)
       
        async for dup in cursor:
            try:                
                id = dup['id']
                if bu.is_normal():   ## since the --verbose causes far more logging 
                    bu.print_progress()
                dup_stat    = await dbc.find_one({'_id': id})
                last_battle_time= dup_stat['last_battle_time']
                account_id      = dup_stat['account_id']
                tank_id         = dup_stat['tank_id']
                if last_battle_time > end or last_battle_time <= start:
                    bu.verbose('Sampled an duplicate not in the defined update. Skipping')
                    dups_skipped += 1
                    continue
                
                bu.verbose(str_dups_tank_stats(update, account_id, tank_id, last_battle_time, is_dup=True))
                cursor_stats = dbc.find({ '$and': [ {'tank_id': tank_id}, {'account_id': account_id},
                                                    {'last_battle_time': { '$gt': last_battle_time }}, { 'last_battle_time': { '$lt': end }}] }
                                                    ).sort('last_battle_time', pymongo.ASCENDING )
                dup_count = 0
                async for stat in cursor_stats:
                    last_battle_time     = stat['last_battle_time']
                    bu.verbose(str_dups_tank_stats(update, account_id, tank_id, last_battle_time))
                    dup_count = 1  # =1 on purpose
                    
                if dup_count == 0:
                    dups_nok += 1
                    bu.verbose("NO DUPLICATE FOUND FOR: account_id=" + str(account_id) +  ' tank_id=' + str(tank_id) + " last_battle_time=" + str(last_battle_time) + " _id=" + id) 
                else:
                    dups_ok += 1                        
                                    
            except Exception as err:
                bu.error('Error checking duplicates. Mode=' + stat_type + ' _id=' + str(id), err)
                return None

    except Exception as err:
        bu.error('Mode=' + stat_type + ' _id=' + str(id), err)
        return None

    if sample == 0:
        sample = dups_ok + dups_nok + dups_skipped
    return {'total': sample, 'ok': dups_ok, 'nok': dups_nok, 'skipped': dups_skipped, 'stat_type': stat_type }
            

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


def print_dups_stats(stat_type: str, dups_total: int, sample: int, dups_ok: int = 0, dups_nok: int = 0, dups_skipped: int= 0):
    try:
        sample_str = (str(sample)  +  " (" + '{:.2f}'.format(sample/dups_total*100) + "%)") if sample > 0 else "all"        
        bu.verbose_std('Total ' + str(dups_total) + ' ' + stat_type +' duplicates. Checked ' + sample_str + " duplicates, skipped " + str(dups_skipped))
        bu.verbose_std("OK: " + str(dups_ok) + " Errors: " + str(dups_nok))
        return dups_nok == 0
    except Exception as err:
        bu.error(exception=err)


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


def str_dups_player_achievements(update : str, account_id: int,  updated: int, is_dup: bool = False):
    try:
        return ("           " if not is_dup else "DUPLICATE: ") + "Update: " + update + ' account_id=' + str(account_id) + ' updated: ' + str(updated)
    except Exception as err:
        bu.error(exception=err)
        return "ERROR"


def str_dups_tank_stats(update : str, account_id: int, tank_id: int, last_battle_time: int, is_dup: bool = False):
    try:
        return  ("           " if not is_dup else "DUPLICATE: ") + "Update: " + update + ' account_id=' + str(account_id) + ' tank_id=' + str(tank_id) + ' last_battle_time: ' + str(last_battle_time)
    except Exception as err:
        bu.error(exception=err)
        return "ERROR"


async def add_stat2del(workerID: int, db: motor.motor_asyncio.AsyncIOMotorDatabase, stat_type: str, id: str, prune : bool = False):
    """Adds _id of the stat record to be deleted in into DB_C_STATS_2_DEL"""
    global DUPS_FOUND, STATS_PRUNED

    dbc = db[DB_C_STATS_2_DEL]
    dbc2prune = db[DB_C[stat_type]]
    try:
        if prune:
            res = await dbc2prune.delete_one( { '_id': id } )
            STATS_PRUNED[stat_type] += res.deleted_count
        else:
            await dbc.insert_one({'type': stat_type, 'id': id})
            DUPS_FOUND[stat_type] += 1
    except Exception as err:
        bu.error(exception=err, id=workerID)
    return None


async def prune_stats_serial(db: motor.motor_asyncio.AsyncIOMotorDatabase, args : argparse.Namespace):
    """Execute DB pruning and DELETING DATA. Does NOT verify whether there are newer stats"""
    global STATS_PRUNED
    try:
        batch_size = 200
        dbc_prunelist = db[DB_C_STATS_2_DEL]
        for stat_type in args.mode:
            dbc_2_prune = db[DB_C[stat_type]]
            #DB_FILTER = {'type' : stat_type}
            stats2prune = await dbc_prunelist.count_documents({'type' : stat_type})
            bu.debug('Pruning ' + str(stats2prune) + ' ' + stat_type)
            bu.set_progress_bar(stat_type + ' pruned: ', stats2prune, slow=True)
            time.sleep(2)
            cursor = dbc_prunelist.find({'type' : stat_type}).batch_size(batch_size)
            docs = await cursor.to_list(batch_size)
            while docs:
                ids = set()
                for doc in docs:
                    ids.add(doc['id'])
                    bu.print_progress()
                if len(ids) > 0:
                    try:
                        res = await dbc_2_prune.delete_many( { '_id': { '$in': list(ids) } } )
                        STATS_PRUNED[stat_type] += res.deleted_count
                    except Exception as err:
                        bu.error('Failure in deleting ' + stat_type, exception=err)
                    try:
                        await dbc_prunelist.delete_many({ 'type': stat_type, 'id': { '$in': list(ids) } })
                    except Exception as err:
                        bu.error('Failure in clearing stats-to-be-pruned table')
                docs = await cursor.to_list(batch_size)
            bu.finish_progress_bar()

    except Exception as err:
        bu.error(exception=err)
    return None


async def prune_stats(db: motor.motor_asyncio.AsyncIOMotorDatabase, args : argparse.Namespace):
    """Parellen DB pruning, DELETES DATA. Does NOT verify whether there are newer stats"""
    #global STATS_PRUNED
    try:
        dbc_prunelist = db[DB_C_STATS_2_DEL]
        batch_size = 100
        rl = RecordLogger('Prune stats')
        Q = asyncio.Queue(5*batch_size)
        workers = list()
        for workerID in range(N_WORKERS):
            workers.append(asyncio.create_task(prune_stats_worker(db, Q, workerID)))                    
        
        for stat_type in args.mode:
            N_stats2prune = await dbc_prunelist.count_documents({'type' : stat_type})
            
            bu.debug('Pruning ' + str(N_stats2prune) + ' ' + stat_type)            
            bu.set_progress_bar(stat_type + ' pruned: ', N_stats2prune, step = 1000, slow=True)
            cursor = dbc_prunelist.find({'type' : stat_type}).batch_size(batch_size)
            docs = await cursor.to_list(batch_size)
            while docs:
                ids = set()
                for doc in docs:
                    ids.add(doc['id'])                    
                if len(ids) > 0:
                    stats2prune                 = dict()
                    stats2prune['stat_type']    = stat_type
                    stats2prune['ids']          = list(ids)
                    await Q.put(stats2prune)
                    bu.debug('added ' + str(len(ids)) + ' stats to be pruned to the queue')
                    rl.log(stat_type + ' to be pruned', len(ids))
                docs = await cursor.to_list(batch_size)
            
            # waiting for the Queue to finish 
            await Q.join()
            bu.finish_progress_bar()

        if len(workers) > 0:
            for worker in workers:
                worker.cancel()
            for res_rl in await asyncio.gather(*workers):
                rl.merge(res_rl)
        rl.print()

    except Exception as err:
        bu.error(exception=err)
    return None


async def prune_stats_worker(db: motor.motor_asyncio.AsyncIOMotorDatabase, Q: asyncio.Queue, ID: int = 1) -> dict:
    """Paraller Worker for pruning stats"""
    
    bu.debug('Started', id=ID)
    rl = RecordLogger('Prune stats')
    try:
        dbc_prunelist = db[DB_C_STATS_2_DEL]        
        
        
        while True:
            prune_task  = await Q.get()
            try:                 
                stat_type   = prune_task['stat_type']
                ids         = prune_task['ids']
                dbc_2_prune = db[DB_C[stat_type]]

                for _id in ids:
                    try:
                        res = await dbc_2_prune.delete_one( { '_id': _id } )
                        if res.deleted_count == 1:                        
                            rl.log(stat_type + ' pruned')
                            bu.print_progress()
                        else:
                            bu.error('Could not find ' + stat_type + ' _id=' + _id)
                            rl.log('Error: Not found ' + stat_type)
                        await dbc_prunelist.delete_one({ '$and': [ {'type': stat_type}, {'id': _id }]})
                    except Exception as err:
                        rl.log('Error pruning ' + stat_type)
                        bu.error(exception=err, id=ID)
            except Exception as err:
                bu.error(exception=err, id=ID)
            Q.task_done()        
    except (asyncio.CancelledError):
        bu.debug('Prune queue is empty', id=ID)
    except Exception as err:
        bu.error(exception=err, id=ID)
    return rl


async def get_tanks_DB(db: motor.motor_asyncio.AsyncIOMotorDatabase, archive=False):
    """Get tank_ids of tanks in the DB"""
    if archive: 
        dbc = db[DB_C_TANK_STATS + DB_C_ARCHIVE]
    else:
        dbc = db[DB_C_TANK_STATS]
    return await dbc.distinct('tank_id').sort()


async def get_tank_name(db: motor.motor_asyncio.AsyncIOMotorDatabase, tank_id: int) -> str:
    """Get tank name from DB's Tankopedia"""
    try:
        dbc = db[DB_C_TANKS]
        res = await dbc.find_one( { 'tank_id': tank_id}, { '_id': 0, 'name': 1} )
        return res['name']
    except Exception as err:
        bu.debug(exception=err)
    return None


async def get_tanks_opt(db: motor.motor_asyncio.AsyncIOMotorDatabase, option: list = None, archive=False):
    """read option and return tank_ids"""
    try:
        TANK_ID_MAX = 10e7
        tank_id_start = TANK_ID_MAX
        tank_ids = set()
        p = re.compile(r'^(\d+)(\+)?$')
        for tank in option:
            try:
                m = p.match(tank).groups()
                if m[1] != None:
                    tank_id_start = min(m[0], tank_id_start)
                else:
                    tank_ids.add(m[0])
            except:
                bu.error('Invalid tank_id give: ' + tank)
        
        if tank_id_start < TANK_ID_MAX:
            tank_ids_start = [ tank_id for tank_id in sorted(await get_tanks_DB(db, archive=False)) if tank_id >= tank_id_start ]
            tank_ids.update(tank_ids_start)
        return list(tank_ids)
    except Exception as err:
        bu.error('Returning empty list', exception=err)
    return list()


async def archive_player_achivements(db: motor.motor_asyncio.AsyncIOMotorDatabase, args: argparse.Namespace = None):
    pass


async def archive_tank_stats(db: motor.motor_asyncio.AsyncIOMotorDatabase, args: argparse.Namespace = None):
    try:
        dbc                 = db[DB_C[MODE_TANK_STATS]]
        archive_collection  = DB_C_ARCHIVE[MODE_TANK_STATS]
                
        bu.verbose_std('Archiving tank stats')
        rl = RecordLogger('Archive tank stats')
        ## bu.set_progress_bar(info_str, n_tank_stats, step = 1000, slow=True )  ## After MongoDB fixes $merge cursor: https://jira.mongodb.org/browse/DRIVERS-671
        pipeline = [ {'$match': { FIELD_UPDATED : True } },
                    { '$unset': FIELD_UPDATED },                                  
                    { '$merge': { 'into': archive_collection, 'on': '_id', 'whenMatched': 'keepExisting' }} ]
        cursor = dbc.aggregate(pipeline, allowDiskUse=True)
        s = 0
        async for _ in cursor:      ## This one does not work yet until MongoDB fixes $merge cursor: https://jira.mongodb.org/browse/DRIVERS-671
            pass
            # bu.print_progress()
            # s +=1
        rl.log('Tank stats archived', s)        
        
    except Exception as err:
        bu.error(exception=err)
    finally:
        bu.finish_progress_bar()        
        bu.log(rl.print(do_print=False))
        rl.print()
    return None


async def snapshot_player_achivements(db: motor.motor_asyncio.AsyncIOMotorDatabase, args: argparse.Namespace = None):
    pass


async def snapshot_tank_stats(db: motor.motor_asyncio.AsyncIOMotorDatabase, args: argparse.Namespace = None):
    try:
        # dbc         = db[DB_C[MODE_TANK_STATS]]
        # dbc_archive = db[DB_C_ARCHIVE[MODE_TANK_STATS]]
        dbc_archive       = db[DB_C[MODE_TANK_STATS]]
        target_collection = DB_C_TANK_STATS + '_latest'
        #dbc               = db[target_collection]

        if args.opt_tanks != None:
            tank_ids = await get_tanks_opt(db, args.opt_tanks)
        else:
            tank_ids = await get_tanks_DB(db)
        
        bu.verbose_std('Creating a snapshot of the latest tank stats')
        
        rl = RecordLogger('Snapshot tank stats')
        l = len(tank_ids)
        i = 0
        id_max      = int(31e8)
        id_step     = int(1e6)
        for tank_id in tank_ids:
            tank_name = None
            try:
                tank_name = await get_tank_name(db, tank_id)
            except Exception as err:
                bu.error('tank_id=' + str(tank_id) + ' not found', exception=err)
            finally:
                if tank_name == None:
                    tank_name = 'Tank name not found'
            i += 1
            info_str = 'Processing tank (' + str(i) + '/' + str(l) + '): ' + tank_name + ' (' +  str(tank_id) + '):'
            bu.log(info_str)
            # n_tank_stats = dbc_archive.count_documents({ 'tank_id': tank_id})
            #bu.set_counter(info_str, rate=True)
            #bu.set_progress_step(1000)
            bu.set_progress_bar(info_str, 31e8/id_step, step = 1, slow=True )
            ## bu.set_progress_bar(info_str, n_tank_stats, step = 1000, slow=True )  ## After MongoDB fixes $merge cursor: https://jira.mongodb.org/browse/DRIVERS-671
            for account_id in range(0, id_max, id_step):
                bu.print_progress()
                try:
                    pipeline = [ {'$match': { '$and': [ {'tank_id': tank_id }, {'account_id': {'$gte': account_id}}, {'account_id': {'$lt': account_id + id_step}} ] }},
                                {'$sort': {'last_battle_time': -1}},
                                {'$group': { '_id': '$account_id',
                                            'doc': {'$first': '$$ROOT'}}},
                                {'$replaceRoot': {'newRoot': '$doc'}}, 
                                { '$merge': { 'into': target_collection, 'on': '_id', 'whenMatched': 'keepExisting' }} ]
                    cursor = dbc_archive.aggregate(pipeline, allowDiskUse=True)
                    s = 0
                    async for _ in cursor:      ## This one does not work yet until MongoDB fixes $merge cursor: https://jira.mongodb.org/browse/DRIVERS-671
                        pass
                        # bu.print_progress()
                        # s +=1
                    rl.log('Tank stats snapshotted', s)
                except Exception as err:
                    bu.error(exception=err)
            bu.finish_progress_bar()
            rl.log('Tanks processed')
        bu.log(rl.print(do_print=False))
        rl.print()
    except Exception as err:
        bu.error(exception=err)
    return None


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
