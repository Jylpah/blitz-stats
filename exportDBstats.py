#!/usr/bin/python3.7

# Script Analyze WoT Blitz replays

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

DB_C_ACCOUNTS = 'WG_Accounts'
DB_C_WG_TANK_STATS = 'WG_TankStats'
DB_C_BS_TANK_STATS = 'BS_PlayerTankStats'
DB_C_BS_PLAYER_STATS = 'BS_PlayerStats'
DB_C_TANKS = 'Tankopedia'

CACHE_VALID = 24*3600*7   # 7 days

bs = None

TODAY = datetime.datetime.utcnow().date()
DEFAULT_DAYS_DELTA = datetime.timedelta(days=90)
DATE_DELTA = datetime.timedelta(days=7)
STATS_EXPORTED = 0

# main() -------------------------------------------------------------


async def main(argv):
    parser = argparse.ArgumentParser(description='Retrieve player stats from the DB')
    parser.add_argument('-f', '--filename', type=str, default=None, help='Filename to write stats into')
    parser.add_argument('--mode', default='help', choices=['players', 'tankstats'], help='Select action')
    parser.add_argument('--tier', type=int, default=None, help='Fiter tanks based on tier')
    parser.add_argument('--date_delta', type=int, default=DATE_DELTA, help='Date delta from the date')
    arggroup = parser.add_mutually_exclusive_group()
    arggroup.add_argument( '-d', '--debug', 		action='store_true', default=False, help='Debug mode')
    arggroup.add_argument( '-v', '--verbose', 	action='store_true', default=False, help='Verbose mode')
    arggroup.add_argument( '-s', '--silent', 	action='store_true', default=False, help='Silent mode')
    parser.add_argument('dates', metavar='DATE1 DATE2 [DATE3 ...]', type=valid_date, default=TODAY, nargs='+', help='Stats cut-off date(s) - format YYYY-MM-DD')

    args = parser.parse_args()
    bu.setSilent(args.silent)
    bu.setVerbose(args.verbose)
    bu.setDebug(args.debug)

    try:

        dates = sorted(args.dates)
        if len(dates) == 1:
            start_date = dates[0] - DEFAULT_DAYS_DELTA
            dates.insert(0, start_date)
        bu.debug(str(dates))


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
        DB_USER     = configDB.get('db_user', 'mongouser')
        DB_PASSWD   = configDB.get('db_password', "PASSWORD")

		#### Connect to MongoDB
        client = motor.motor_asyncio.AsyncIOMotorClient(DB_SERVER,DB_PORT, authSource=DB_AUTH, username=DB_USER, password=DB_PASSWD, ssl=DB_SSL, ssl_cert_reqs=DB_CERT_REQ)

        db = client[DB_NAME]
        bu.debug(str(type(db)))
        tasks = []

        periodQ = asyncio.Queue()
        for i in range(1, len(dates)):
            await periodQ.put([dates[i-1]+args.date_delta, dates[i]+args.date_delta])
        
        if args.mode == 'players':
            filename = 'playerstats' if args.filename == None else args.filename
            for i in range(N_WORKERS):
                tasks.append(asyncio.create_task(qBSplayerStats(i, db, periodQ, filename)))
        elif args.mode == 'tankstats':
            filename = 'tankstats' if args.filename == None else args.filename                
            for i in range(N_WORKERS):
                tasks.append(asyncio.create_task(qWGtankStats(i, db, periodQ, filename, args.tier)))
                bu.debug('Task ' + str(i) + ' started')

        bu.debug('Waiting for statsworkers to finish')
        await periodQ.join()
		
        bu.debug('Cancelling workers')
        for task in tasks:
            task.cancel()
        bu.debug('Waiting for workers to cancel')
        if len(tasks) > 0:
            await asyncio.gather(*tasks, return_exceptions=True)
        printStats(args.mode)
    except asyncio.CancelledError as err:
        bu.error('Queue gets cancelled while still working.')
    except Exception as err:
        bu.error('Unexpected Exception: ' + str(type(err)) + ' : ' + str(err))

    return None

def printStats(mode = ""):
    bu.verbose_std(str(STATS_EXPORTED) + ' stats exported (' + mode + ')')

def valid_date(s):
    try:
        return date.fromisoformat(s)
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def NOW() -> int:
    return int(time.time())


async def qBStankStats(workerID: int, db: motor.motor_asyncio.AsyncIOMotorDatabase, periodQ: asyncio.Queue, filename: str, tier=None):
    """Async Worker to fetch tank stats"""
    
    global STATS_EXPORTED
    dbc = db[DB_C_BS_TANK_STATS]

    while True:
        item = await periodQ.get()
        bu.debug( str(periodQ.qsize())  + ' periods to process', workerID)
        try:
            
            dayA = item[0]
            dayB = item[1]
            timeA = int(time.mktime(dayA.timetuple()))
            timeB = int(time.mktime(dayB.timetuple()))
            datestr = dayB.isoformat()
            fn = filename + '_' + datestr + (('_tier_' + str(tier)) if tier != None else '') + '.jsonl'

            bu.debug('Start: ' + str(timeA) + ' End: ' + str(timeB), workerID)

            async with aiofiles.open(fn, 'w', encoding="utf8") as fp:
                tanks = await getDBtanksTier(db, tier)
                for tank_id in tanks:
                    bu.debug('Exporting stats for tier ' + str(tier) + ' tanks: ' + ', '.join(list(map(str, tanks))), workerID)
                    findQ = {'$and': [{'last_battle_time': {'$lte': timeB}}, { 'last_battle_time': {'$gt': timeA}}, {'tank_id': tank_id}]}
                    cursor = dbc.find(findQ, {'_id': 0})
                    i = 0
                    async for doc in cursor:
                        i = (i+1) % 10000
                        if i == 0:
                            bu.printWaiter()
                        await fp.write(json.dumps(doc, ensure_ascii=False) + '\n')
                        STATS_EXPORTED += 1

        except Exception as err:
            bu.error('Unexpected Exception: ' + str(type(err)) + ' : ' + str(err), workerID)
        finally:
            bu.debug('File write complete', workerID)
            periodQ.task_done()
            
    return None


async def qWGtankStats(workerID: int, db: motor.motor_asyncio.AsyncIOMotorDatabase, periodQ: asyncio.Queue, filename: str, tier=None):
    """Async Worker to fetch player stats"""
    global STATS_EXPORTED
    dbc = db[DB_C_WG_TANK_STATS]
    
    tanks = await getDBtanksTier(db, tier)
    bu.debug('[' + str(workerID) + '] ' + str(len(tanks))  + ' tanks in DB')

    while True:
        item = await periodQ.get()
        bu.debug('[' + str(workerID) + '] ' + str(periodQ.qsize())  + ' periods to process')
        
        try:
            dayA = item[0]
            dayB = item[1]
            timeA = int(time.mktime(dayA.timetuple()))
            timeB = int(time.mktime(dayB.timetuple()))
            datestr = dayB.isoformat()
            fn = filename + '_' + datestr + '.jsonl'

            bu.debug('[' + str(workerID) + '] Start: ' + str(timeA) + ' End: ' + str(timeB))

            async with aiofiles.open(fn, 'w', encoding="utf8") as fp:
                for tank_id in tanks:
                    pipeline = [ {'$match': { '$and': [{'last_battle_time': {'$lte': timeB}}, {'last_battle_time': {'$gt': timeA}}, {'tank_id': tank_id } ] }},
                                {'$sort': {'last_battle_time': -1}},
                                {'$group': { '_id': '$account_id',
                                            'doc': {'$first': '$$ROOT'}}},
                                {'$replaceRoot': {'newRoot': '$doc'}}, 
                                {'$project': {'_id': False}} ]
                    cursor = dbc.aggregate(pipeline, allowDiskUse=True)
                    #cursor = dbc.aggregate(pipeline)
                    i = 0
                    async for doc in cursor:
                        i = (i+1) % 10000
                        if i == 0:
                            bu.printWaiter()
                        await fp.write(json.dumps(doc, ensure_ascii=False) + '\n')
                        STATS_EXPORTED += 1
                        
        except Exception as err:
            bu.error('[' + str(workerID) + '] Unexpected Exception: ' + str(type(err)) + ' : ' + str(err))
        finally:
            bu.verbose_std('\n[' + str(workerID) + '] File write complete: ' + fn)
            periodQ.task_done()

    return None


async def qBSplayerStats(workerID: int, db: motor.motor_asyncio.AsyncIOMotorDatabase, periodQ: asyncio.Queue, filename: str):
    """Async Worker to fetch player stats"""
    global STATS_EXPORTED
    dbc = db[DB_C_BS_PLAYER_STATS]

    while True:
        item = await periodQ.get()
        bu.debug('[' + str(workerID) + '] ' + str(periodQ.qsize())  + ' periods to process')
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
                    pipeline = [{'$match': {
                        '$and': [{'last_battle_time': {'$lte': timeB}}, {'last_battle_time': {'$gt': timeA}},
                                {'account_id': {'$lte': id + id_step}}, {'account_id': {'$gt': id}}]}},
                                {'$sort': {'last_battle_time': -1}},
                                {'$group': {'_id': '$account_id',
                                            'doc': {'$first': '$$ROOT'}}},
                                {'$replaceRoot': {'newRoot': '$doc'}},
                                {'$project': {'achievements': False, 'clan': False}}]

                    cursor = dbc.aggregate(pipeline, allowDiskUse=True)
                    i = 0
                    async for doc in cursor:
                        i = (i+1) % 1000
                        if i == 0:
                            bu.printWaiter()
                        await fp.write(json.dumps(doc, ensure_ascii=False) + '\n')
                        STATS_EXPORTED += 1

                    bu.debug('[' + str(workerID) + '] write iteration complete')
                bu.debug('[' + str(workerID) + '] File write complete')

        except Exception as err:
            bu.error('[' + str(workerID) + '] Unexpected Exception: ' + str(type(err)) + ' : ' + str(err))
        finally:
            bu.debug('[' + str(workerID) + '] File write complete')
            periodQ.task_done()

    return None


async def getDBtanks(db: motor.motor_asyncio.AsyncIOMotorDatabase):
    """Get tank_ids of tanks in the DB"""
    dbc = db[DB_C_WG_TANK_STATS]
    return await dbc.distinct('tank_id')
    

async def getDBtanksTier(db: motor.motor_asyncio.AsyncIOMotorDatabase, tier: int):
    """Get tank_ids of tanks in a particular tier"""
    dbc = db[DB_C_TANKS]
    tanks = list()
    
    if (tier == None):
        tanks = await getDBtanks(db)
        
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
