from argparse import ArgumentParser, Namespace, SUPPRESS
from configparser import ConfigParser
from datetime import datetime
from typing import Optional, Any, List, Dict
from asyncio import create_task, gather, sleep, wait, Queue, CancelledError, Task
import asyncio
from sortedcollections import NearestDict  # type: ignore
from tqdm import tqdm
import copy
from os import getpid

import os.path
from math import ceil

# from asyncstdlib import enumerate
# from alive_progress import alive_bar  # type: ignore

# from icecream import ic  # type: ignore
# from yappi import profile 	# type: ignore

# multiprocessing
from multiprocessing import Manager, cpu_count
from multiprocessing.pool import Pool, AsyncResult
from multiprocessing.managers import DictProxy
import queue

# export data
import pandas as pd  # type: ignore
import pyarrow as pa  # type: ignore
import pyarrow.dataset as ds  # type: ignore

# from pandas.io.json import json_normalize	# type: ignore

from eventcounter import EventCounter
from queutils import IterableQueue, QueueDone, AsyncQueue

# from multilevellogger import MultiLevelLogger, getMultiLevelLogger
import logging

from pydantic_exportables import JSONExportable, export
from blitzmodels import (
    WGApi,
    Region,
    TankStat,
    add_args_wg,
)

from .backend import (
    Backend,
    OptAccountsInactive,
    BSTableType,
    ACCOUNTS_Q_MAX,
    get_sub_type,
)
from .models import BSAccount, BSBlitzRelease, StatsTypes, BSTank
from .accounts import (
    create_accountQ,
    read_args_accounts,
    create_accountQ_active,
    accounts_parse_args,
)
from .releases import get_releases, release_mapper

from .arrow import (
    dataset_writer,
    EXPORT_DATA_FORMATS,
    DEFAULT_EXPORT_DATA_FORMAT,
)
from .utils import tqdm_monitorQ

logger = logging.getLogger(__name__)
error = logger.error
message = logger.warning
verbose = logger.info
debug = logger.debug

logger.setLevel(logging.getLogger().level)

# Constants

WORKERS_WGAPI: int = 75
WORKERS_IMPORTERS: int = 5
WORKERS_PRUNE: int = 10
WORKERS_EDIT: int = 10
TANK_STATS_Q_MAX: int = 1000
TANK_STATS_BATCH: int = 50000

# Globals

# export_total_rows : int = 0
db: Backend
mp_wg: WGApi
readQ: AsyncQueue[List[Any] | None]
progressQ: AsyncQueue[int | None]
workQ_t: AsyncQueue[int | None]
workQ_a: AsyncQueue[BSAccount | None]
tank_statQ: AsyncQueue[List[TankStat]]
counterQas: AsyncQueue[int]
writeQ: AsyncQueue[pd.DataFrame]
in_model: type[JSONExportable]
mp_options: Dict[str, Any] = dict()
mp_args: Namespace

########################################################
#
# add_args_ functions
#
########################################################


def add_args(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
    try:
        debug("starting")
        tank_stats_parsers = parser.add_subparsers(
            dest="tank_stats_cmd",
            title="tank-stats commands",
            description="valid commands",
            metavar="fetch | prune | edit | import | export",
        )
        tank_stats_parsers.required = True

        fetch_parser = tank_stats_parsers.add_parser(
            "fetch", aliases=["get"], help="tank-stats fetch help"
        )
        if not add_args_fetch(fetch_parser, config=config):
            raise Exception("Failed to define argument parser for: tank-stats fetch")

        edit_parser = tank_stats_parsers.add_parser("edit", help="tank-stats edit help")
        if not add_args_edit(edit_parser, config=config):
            raise Exception("Failed to define argument parser for: tank-stats edit")

        prune_parser = tank_stats_parsers.add_parser(
            "prune", help="tank-stats prune help"
        )
        if not add_args_prune(prune_parser, config=config):
            raise Exception("Failed to define argument parser for: tank-stats prune")

        import_parser = tank_stats_parsers.add_parser(
            "import", help="tank-stats import help"
        )
        if not add_args_import(import_parser, config=config):
            raise Exception("Failed to define argument parser for: tank-stats import")

        export_parser = tank_stats_parsers.add_parser(
            "export", help="tank-stats export help"
        )
        if not add_args_export(export_parser, config=config):
            raise Exception("Failed to define argument parser for: tank-stats export")

        export_data_parser = tank_stats_parsers.add_parser(
            "export-data", help="tank-stats export-data help"
        )
        if not add_args_export_data(export_data_parser, config=config):
            raise Exception(
                "Failed to define argument parser for: tank-stats export-data"
            )

        debug("Finished")
        return True
    except Exception as err:
        error(f"{err}")
    return False


def add_args_fetch(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    try:
        debug("starting")
        if not add_args_wg(parser, config):
            return False

        parser.add_argument(
            "--regions",
            "--region",
            type=str,
            nargs="*",
            choices=[r.value for r in Region.API_regions()],
            default=[r.value for r in Region.API_regions()],
            help="Filter by region (default: eu + com + asia)",
        )
        parser.add_argument(
            "--inactive",
            type=str,
            choices=[o.value for o in OptAccountsInactive],
            default=OptAccountsInactive.both.value,
            help="Include inactive accounts",
        )
        parser.add_argument(
            "--active-since",
            type=str,
            default=None,
            metavar="RELEASE/DAYS",
            help="Fetch stats for accounts that have been active since RELEASE/DAYS",
        )
        parser.add_argument(
            "--inactive-since",
            type=str,
            default=None,
            metavar="RELEASE/DAYS",
            help="Fetch stats for accounts that have been inactive since RELEASE/DAYS",
        )
        parser.add_argument(
            "--cache-valid",
            type=float,
            default=1,
            metavar="DAYS",
            help="Fetch stats only for accounts with stats older than DAYS",
        )
        parser.add_argument(
            "--distributed",
            "--dist",
            type=str,
            dest="distributed",
            metavar="I:N",
            default=None,
            help="Distributed fetching for accounts: id %% N == I",
        )
        parser.add_argument(
            "--check-disabled",
            dest="disabled",
            action="store_true",
            default=False,
            help="Check disabled accounts",
        )
        parser.add_argument(
            "--accounts",
            type=str,
            default=[],
            nargs="*",
            metavar="ACCOUNT_ID [ACCOUNT_ID1 ...]",
            help="Fetch stats for the listed ACCOUNT_ID(s). \
									ACCOUNT_ID format 'account_id:region' or 'account_id'",
        )
        # parser.add_argument('--mp', action='store_true', default=False,
        # 					help='Multiprocess fetch')
        parser.add_argument(
            "--force",
            action="store_true",
            default=False,
            help="Fetch stats for all accounts",
        )
        parser.add_argument(
            "--sample",
            type=float,
            default=0,
            metavar="SAMPLE",
            help="Fetch tank stats for SAMPLE of accounts. If 0 < SAMPLE < 1, SAMPLE defines a %% of users",
        )
        parser.add_argument(
            "--file",
            type=str,
            metavar="FILENAME",
            default=None,
            help="Read account_ids from FILENAME one account_id per line",
        )
        parser.add_argument("--last", action="store_true", default=False, help=SUPPRESS)

        return True
    except Exception as err:
        error(f"{err}")
    return False


def add_args_edit(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    debug("starting")
    try:
        edit_parsers = parser.add_subparsers(
            dest="tank_stats_edit_cmd",
            title="tank-stats edit commands",
            description="valid commands",
            metavar="remap-release",
        )
        edit_parsers.required = True

        remap_parser = edit_parsers.add_parser(
            "remap-release", help="tank-stats edit remap-release help"
        )
        if not add_args_edit_remap_release(remap_parser, config=config):
            raise Exception(
                "Failed to define argument parser for: tank-stats edit remap-release"
            )

        ################################################################################
        #
        ## DEPRECIATED: This function is not needed anymore. Comment out, but keep.
        #
        ################################################################################
        #
        # missing_parser = edit_parsers.add_parser('fix-missing', help="tank-stats edit fix-missing help")
        # if not add_args_edit_fix_missing(missing_parser, config=config):
        # 	raise Exception("Failed to define argument parser for: tank-stats edit fix-missing")

        return True
    except Exception as err:
        error(f"{err}")
    return False


def add_args_edit_common(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    """Adds common arguments to edit subparser. Required for meaningful helps and usability"""
    debug("starting")
    parser.add_argument(
        "--commit",
        action="store_true",
        default=False,
        help="Do changes instead of just showing what would be changed",
    )
    parser.add_argument(
        "--sample",
        type=float,
        default=0,
        metavar="SAMPLE",
        help="Sample size. 0 < SAMPLE < 1 : %% of stats, 1<=SAMPLE : Absolute number",
    )
    # filters
    parser.add_argument(
        "--regions",
        "--region",
        type=str,
        nargs="*",
        choices=[r.value for r in Region.API_regions()],
        default=[r.value for r in Region.API_regions()],
        help=f"Filter by region (default is API = {' + '.join([r.value for r in Region.API_regions()])})",
    )
    parser.add_argument(
        "--release",
        type=str,
        metavar="RELEASE",
        default=None,
        help="Apply edits to a RELEASE",
    )
    parser.add_argument(
        "--since",
        type=str,
        metavar="DATE",
        default=None,
        nargs="?",
        help="Apply edits to releases launched after DATE (default is all releases)",
    )
    parser.add_argument(
        "--accounts",
        type=str,
        default=[],
        nargs="*",
        metavar="ACCOUNT_ID [ACCOUNT_ID1 ...]",
        help="Edit tank stats for the listed ACCOUNT_ID(s) ('account_id:region' or 'account_id')",
    )
    parser.add_argument(
        "--tank_ids",
        type=int,
        default=None,
        nargs="*",
        metavar="TANK_ID [TANK_ID1 ...]",
        help="Edit tank stats for the listed TANK_ID(S).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        dest="workers",
        default=WORKERS_EDIT,
        help="Set number of asynchronous workers",
    )
    return True


def add_args_edit_remap_release(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    debug("starting")
    return add_args_edit_common(parser, config)


################################################################################
#
## DEPRECIATED: This function is not needed anymore. Comment out, but keep.
#
################################################################################

# def add_args_edit_fix_missing(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
# 	debug('starting')
# 	if not add_args_edit_common(parser, config):
# 		return False
# 	parser.add_argument('field', type=str, metavar='FIELD',
# 						help='Fix stats with missing FIELD')
# 	return True


def add_args_prune(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    debug("starting")
    parser.add_argument(
        "release", type=str, metavar="RELEASE", help="prune tank stats for a RELEASE"
    )
    parser.add_argument(
        "--regions",
        "--region",
        type=str,
        nargs="*",
        choices=[r.value for r in Region.API_regions()],
        default=[r.value for r in Region.API_regions()],
        help="filter by region (default: eu + com + asia + ru)",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        default=False,
        help="execute pruning stats instead of listing duplicates",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=WORKERS_PRUNE,
        help=f"set number of worker processes (default={WORKERS_PRUNE})",
    )
    parser.add_argument(
        "--sample", type=int, default=0, metavar="SAMPLE", help="sample size"
    )
    return True


def add_args_import(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    """Add argument parser for tank-stats import"""
    try:
        debug("starting")

        import_parsers = parser.add_subparsers(
            dest="import_backend",
            title="tank-stats import backend",
            description="valid backends",
            metavar=", ".join(Backend.list_available()),
        )
        import_parsers.required = True

        for backend in Backend.get_registered():
            import_parser = import_parsers.add_parser(
                backend.driver, help=f"tank-stats import {backend.driver} help"
            )
            if not backend.add_args_import(import_parser, config=config):
                raise Exception(
                    f"Failed to define argument parser for: tank-stats import {backend.driver}"
                )
        parser.add_argument(
            "--workers",
            type=int,
            default=0,
            help="set number of worker processes (default=0 i.e. auto)",
        )
        parser.add_argument(
            "--import-model",
            metavar="IMPORT-TYPE",
            type=str,
            required=True,
            choices=["TankStat"],
            help="data format to import. Default is blitz-stats native format.",
        )
        parser.add_argument(
            "--sample",
            type=float,
            default=0,
            help="Sample size. 0 < SAMPLE < 1 : %% of stats, 1<=SAMPLE : Absolute number",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            default=False,
            help="overwrite existing stats when importing",
        )
        # parser.add_argument('--mp', action='store_true', default=False,
        # 					help='Use multi-processing')
        parser.add_argument(
            "--no-release-map",
            action="store_true",
            default=False,
            help="do not map releases when importing",
        )
        # parser.add_argument('--last', action='store_true', default=False, help=SUPPRESS)

        return True
    except Exception as err:
        error(f"{err}")
    return False


def add_args_export(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    try:
        debug("starting")
        EXPORT_FORMAT = "json"
        EXPORT_FILE = "tank_stats"
        EXPORT_SUPPORTED_FORMATS: List[str] = ["json"]  # , 'csv'

        if config is not None and "TANK_STATS" in config.sections():
            configTS = config["TANK_STATS"]
            EXPORT_FORMAT = configTS.get("export_format", EXPORT_FORMAT)
            EXPORT_FILE = configTS.get("export_file", EXPORT_FILE)

        parser.add_argument(
            "format",
            type=str,
            nargs="?",
            choices=EXPORT_SUPPORTED_FORMATS,
            default=EXPORT_FORMAT,
            help="export file format",
        )
        parser.add_argument(
            "filename",
            metavar="FILE",
            type=str,
            nargs="?",
            default=EXPORT_FILE,
            help="file to export tank-stats to. Use '-' for STDIN",
        )
        parser.add_argument(
            "--basedir",
            metavar="FILE",
            type=str,
            nargs="?",
            default=".",
            help="base dir to export data",
        )
        parser.add_argument(
            "--append", action="store_true", default=False, help="Append to file(s)"
        )
        parser.add_argument(
            "--force",
            action="store_true",
            default=False,
            help="overwrite existing file(s) when exporting",
        )
        # parser.add_argument('--disabled', action='store_true', default=False, help='Disabled accounts')
        # parser.add_argument('--inactive', type=str, choices=[ o.value for o in OptAccountsInactive ],
        # default=OptAccountsInactive.no.value, help='Include inactive accounts')
        parser.add_argument(
            "--regions",
            "--region",
            type=str,
            nargs="*",
            choices=[r.value for r in Region.API_regions()],
            default=[r.value for r in Region.API_regions()],
            help="filter by region (default is API = eu + com + asia)",
        )
        parser.add_argument(
            "--by-region",
            action="store_true",
            default=False,
            help="Export tank-stats by region",
        )
        parser.add_argument(
            "--accounts",
            type=str,
            default=[],
            nargs="*",
            metavar="ACCOUNT_ID [ACCOUNT_ID1 ...]",
            help="exports tank stats for the listed ACCOUNT_ID(s). \
									ACCOUNT_ID format 'account_id:region' or 'account_id'",
        )
        parser.add_argument(
            "--tanks",
            type=int,
            default=[],
            nargs="*",
            metavar="TANK_ID [TANK_ID1 ...]",
            help="export tank stats for the listed TANK_ID(s)",
        )
        parser.add_argument(
            "--release",
            type=str,
            metavar="RELEASE",
            default=None,
            help="export stats for a RELEASE",
        )
        parser.add_argument(
            "--sample",
            type=float,
            default=0,
            help="sample size. 0 < SAMPLE < 1 : %% of stats, 1<=SAMPLE : Absolute number",
        )
        return True
    except Exception as err:
        error(f"{err}")
    return False


def add_args_export_data(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    try:
        debug("starting")
        EXPORT_FORMAT: str = DEFAULT_EXPORT_DATA_FORMAT
        EXPORT_DIR: str = "export"

        if config is not None and "EXPORT" in config.sections():
            configEXP = config["EXPORT"]
            EXPORT_FORMAT = configEXP.get("data_format", DEFAULT_EXPORT_DATA_FORMAT)
            EXPORT_DIR = configEXP.get("dir", EXPORT_DIR)

        parser.add_argument(
            "EXPORT_TYPE",
            type=str,
            choices=["update", "career"],
            help="export latest stats or stats for the update",
        )
        parser.add_argument("RELEASE", type=str, help="export stats for a RELEASE")
        parser.add_argument(
            "--after",
            action="store_true",
            default=False,
            help="exports stats at the end of release (default=False)",
        )
        parser.add_argument(
            "--format",
            type=str,
            nargs="?",
            choices=EXPORT_DATA_FORMATS,
            default=EXPORT_FORMAT,
            help="export file format",
        )
        # parser.add_argument('--filename', metavar='FILE', type=str, nargs='?', default=None,
        # 					help='file to export tank-stats to')
        parser.add_argument(
            "--dir",
            dest="basedir",
            metavar="FILE",
            type=str,
            nargs="?",
            default=EXPORT_DIR,
            help=f"directory to export data (default: {EXPORT_DIR})",
        )
        parser.add_argument(
            "--regions",
            "--region",
            type=str,
            nargs="*",
            choices=[r.value for r in Region.API_regions()],
            default=[r.value for r in Region.API_regions()],
            help="filter by region (default: " + " + ".join(Region.API_regions()) + ")",
        )
        parser.add_argument(
            "--workers",
            type=int,
            default=0,
            help="set number of worker processes (default=0 i.e. auto)",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            default=False,
            help="overwrite existing file(s) when exporting",
        )

        return True
    except Exception as err:
        error(f"{err}")
    return False


###########################################
#
# cmd_ functions
#
###########################################


async def cmd(db: Backend, args: Namespace) -> bool:
    try:
        debug("starting")
        if args.tank_stats_cmd == "fetch":
            if len(args.accounts) > 0 or args.file is not None:
                return await cmd_fetch(db, args)
            else:
                return await cmd_fetchMP(db, args)

        elif args.tank_stats_cmd == "edit":
            return await cmd_edit(db, args)

        elif args.tank_stats_cmd == "export":
            return await cmd_export_text(db, args)

        elif args.tank_stats_cmd == "export-data":
            if args.EXPORT_TYPE == "update":
                return await cmd_export_update(db, args)
            elif args.EXPORT_TYPE == "career":
                return await cmd_export_career(db, args)
            else:
                raise NotImplementedError(
                    f"export type not implemented: {args.EXPORT_TYPE}"
                )
        elif args.tank_stats_cmd == "import":
            return await cmd_importMP(db, args)

        elif args.tank_stats_cmd == "prune":
            return await cmd_prune(db, args)

    except Exception as err:
        error(f"{err}")
    return False


########################################################
#
# cmd_fetchMP()
#
########################################################


async def cmd_fetchMP(db: Backend, args: Namespace) -> bool:
    """fetch tank stats"""
    debug("starting")

    try:
        stats: EventCounter = EventCounter("tank-stats fetch")
        regions: set[Region] = {Region(r) for r in args.regions}
        # worker: Task
        WORKERS: int = len(regions)
        with Manager() as manager:
            totals: DictProxy[Region, int] = manager.dict()
            progress: DictProxy[Region, int] = manager.dict()
            bars: Dict[Region, tqdm] = dict()
            initialized: dict[Region, bool] = dict()
            message("Fetching tank stats")

            with Pool(
                processes=WORKERS,
                initializer=fetch_mp_init,
                initargs=[db.config],
            ) as pool:
                debug(f"starting {WORKERS} workers")
                position: int = 0
                for region in sorted(regions):
                    totals[region] = 0
                    progress[region] = 0
                    initialized[region] = False
                    bars[region] = tqdm(
                        total=0,
                        position=position,
                        desc=f"{region}",
                        bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed} ETA {remaining} {rate_fmt}]",
                        unit="",
                        leave=True,
                    )
                    position += 1

                results: AsyncResult = pool.starmap_async(
                    fetch_mp_worker_start,
                    [(args, r, progress, totals) for r in regions],
                )
                pool.close()

                while not results.ready():
                    await sleep(1)
                    for r in regions:
                        if not initialized[r] and totals[r] > 0:
                            bars[r].total = totals[r]
                            bars[r].refresh()
                            initialized[r] = True
                        else:
                            if totals[r] > bars[r].total:
                                bars[r].total = totals[r]
                                bars[r].refresh()
                            bars[r].update(progress[r] - bars[r].n)
                for r in regions:
                    bars[r].close()

                for res in results.get():
                    stats.merge_child(res)
                pool.join()

        if (msg := stats.print(do_print=False, clean=True)) is not None:
            # tqdm.write(msg)
            message(msg)
        return True
    except Exception as err:
        error(f"{err}")

    return False


def fetch_mp_init(
    backend_config: Dict[str, Any],
):
    """Initialize static/global backend into a forked process"""
    global db
    debug(f"starting (PID={getpid()})")

    if (tmp_db := Backend.create(delayed_init=False, **backend_config)) is None:
        raise ValueError("could not create backend")
    db = tmp_db
    debug("finished")


def fetch_mp_worker_start(
    args: Namespace,
    region: Region,
    progress: DictProxy,  # [Region, int]
    totals: DictProxy,  # [Region, int]
) -> EventCounter:
    """Forkable tank stats fetch worker for tank stats"""
    debug(f"starting fetch worker {region}")
    return asyncio.run(fetch_mp_worker(args, region, progress, totals), debug=False)


async def fetch_mp_worker(
    args: Namespace,
    region: Region,
    progress: DictProxy,  # [Region, int]
    totals: DictProxy,  # [Region, int]
) -> EventCounter:
    """Forkable tank stats fetch worker for latest (career) stats"""
    global db

    if db is None:
        raise ValueError("db is not initialized")
    debug(f"fetch worker starting: {region}")
    stats: EventCounter = EventCounter(f"fetch {region}")
    accountQ: IterableQueue[BSAccount] = IterableQueue(maxsize=20)
    failedQ: IterableQueue[BSAccount] | None = None
    retryQ: IterableQueue[BSAccount] = IterableQueue(maxsize=5)
    statsQ: Queue[List[TankStat]] = Queue(TANK_STATS_Q_MAX)
    # args: Namespace = mp_args
    THREADS: int = args.wg_workers
    wg: WGApi = WGApi(
        app_id=args.wg_app_id,
        rate_limit=args.wg_rate_limit,
    )
    try:
        args.regions = {region}
        if not args.disabled:
            failedQ = IterableQueue()  # must not use maxsize

        workers: List[Task] = list()
        workers.append(create_task(fetch_backend_worker(db, statsQ, force=args.force)))

        for _ in range(THREADS):
            workers.append(
                create_task(
                    fetch_api_worker(
                        db,
                        wg_api=wg,
                        accountQ=accountQ,
                        statsQ=statsQ,
                        retryQ=failedQ,
                        disabled=args.disabled,
                    )
                )
            )
        # BATCH: int = 100
        # i: int = 0
        await accountQ.add_producer()
        if (accounts_args := await accounts_parse_args(db, args)) is not None:
            totals[region] = await db.accounts_count(
                StatsTypes.tank_stats, **accounts_args
            )
            async for account in db.accounts_get(
                stats_type=StatsTypes.tank_stats, **accounts_args
            ):
                await accountQ.put(account)
                stats.log("read")
                progress[region] += 1
                # i += 1
                # if i == BATCH:
                #     await counterQas.put(BATCH)
                #     i = 0
            # await counterQas.put(i)
        else:
            raise ValueError(f"could not parse account args: {args}")
        await accountQ.finish_producer()

        debug(f"waiting for account queue to finish: {region}")
        await accountQ.join()

        # Process retryQ
        if failedQ is not None and not failedQ.empty():
            retry_accounts: int = failedQ.qsize()
            totals[region] += retry_accounts
            debug(f"retryQ: size={retry_accounts} is_filled={retryQ.is_filled}")
            for _ in range(THREADS):
                workers.append(
                    create_task(
                        fetch_api_worker(db, wg_api=wg, accountQ=retryQ, statsQ=statsQ)
                    )
                )
            await retryQ.add_producer()
            async for account in failedQ:
                await retryQ.put(account)
                stats.log("retry")
                progress[region] += 1
            await retryQ.finish_producer()
            await retryQ.join()

        await statsQ.join()
        await stats.gather_stats(workers)

    except Exception as err:
        error(f"{err}")
    finally:
        wg.print()
        await wg.close()

    return stats


########################################################
#
# cmd_fetch()
#
########################################################


async def cmd_fetch(db: Backend, args: Namespace) -> bool:
    """fetch tank stats"""
    debug("starting")
    wg: WGApi = WGApi(
        app_id=args.wg_app_id,
        rate_limit=args.wg_rate_limit,
    )

    try:
        stats: EventCounter = EventCounter("tank-stats fetch")
        regions: set[Region] = {Region(r) for r in args.regions}
        accountQs: Dict[Region, IterableQueue[BSAccount]] = dict()
        retryQ: IterableQueue[BSAccount] | None = None
        statsQ: Queue[List[TankStat]] = Queue(maxsize=TANK_STATS_Q_MAX)
        accounts_args: Dict[str, Any] | None

        if not args.disabled:
            retryQ = IterableQueue()  # must not use maxsize

        workers: List[Task] = list()
        workers.append(create_task(fetch_backend_worker(db, statsQ, force=args.force)))

        # Process accountQ
        accounts: int = -1
        if len(args.accounts) > 0:
            accounts = len(args.accounts)
        elif args.file is not None:
            accounts = await BSAccount.count_file(args.file)
        else:
            if (accounts_args := await accounts_parse_args(db, args)) is None:
                raise ValueError(f"could not parse account args: {args}")
            accounts = await db.accounts_count(StatsTypes.tank_stats, **accounts_args)

        WORKERS: int = max([int(args.wg_workers), 1])
        WORKERS = min([WORKERS, ceil(accounts / 4)])
        tasks_bar: list[Task] = list()
        bar_position: int = 0
        for r in regions:
            accountQs[r] = IterableQueue(maxsize=ACCOUNTS_Q_MAX)
            r_args = copy.copy(args)
            r_args.regions = {r}

            if len(args.accounts) == 0 and args.file is None:
                if (accounts_args := await accounts_parse_args(db, args)) is None:
                    raise ValueError(f"could not parse account args: {args}")
                accounts = await db.accounts_count(
                    StatsTypes.tank_stats, **accounts_args | {"regions": {r}}
                )

            workers.append(
                create_task(
                    create_accountQ(
                        db, r_args, accountQs[r], stats_type=StatsTypes.tank_stats
                    )
                )
            )
            for _ in range(WORKERS):
                workers.append(
                    create_task(
                        fetch_api_worker(
                            db,
                            wg_api=wg,
                            accountQ=accountQs[r],
                            statsQ=statsQ,
                            retryQ=retryQ,
                            disabled=r_args.disabled,
                        )
                    )
                )
            tasks_bar.append(
                create_task(
                    tqdm_monitorQ(
                        accountQs[r],
                        tqdm(
                            total=accounts,
                            desc=f"Fetching tank stats [{r}]",
                            position=bar_position,
                        ),
                    )
                )
            )
            bar_position += 1

        for r, Q in accountQs.items():
            debug(f"waiting for account queue to finish: {r}")
            await accountQs[r].join()

        # Process retryQ
        if retryQ is not None and not retryQ.empty():
            retry_accounts: int = retryQ.qsize()
            debug(f"retryQ: size={retry_accounts} is_filled={retryQ.is_filled}")
            task_bar = create_task(
                tqdm_monitorQ(
                    retryQ, tqdm(total=retry_accounts, desc="Retrying failed accounts")
                )
            )
            for _ in range(min([args.wg_workers, ceil(retry_accounts / len(regions))])):
                workers.append(
                    create_task(
                        fetch_api_worker(db, wg_api=wg, accountQ=retryQ, statsQ=statsQ)
                    )
                )
            await retryQ.join()
            task_bar.cancel()

        await statsQ.join()

        for task in workers:
            task.cancel()

        for ec in await gather(*workers, return_exceptions=True):
            if isinstance(ec, EventCounter):
                stats.merge_child(ec)
        if (msg := stats.print(do_print=False, clean=True)) is not None:
            message(msg)
        return True
    except Exception as err:
        error(f"{err}")
    finally:
        wg.print()
        await wg.close()
    return False


async def fetch_api_worker(
    db: Backend,
    wg_api: WGApi,
    accountQ: IterableQueue[BSAccount],
    statsQ: Queue[List[TankStat]],
    retryQ: IterableQueue[BSAccount] | None = None,
    disabled: bool = False,
) -> EventCounter:
    """Async worker to fetch tank stats from WG API"""
    debug("starting")
    stats: EventCounter
    tank_stats: List[TankStat] | None
    if retryQ is None:
        stats = EventCounter("re-try")
    else:
        stats = EventCounter("fetch")
        await retryQ.add_producer()

    try:
        while True:
            account: BSAccount = await accountQ.get()
            # if retryQ is None:
            # 	print(f'retryQ: account_id={account.id}')
            try:
                debug(f"account_id: {account.id}")
                stats.log("accounts total")

                if account.region is None:
                    raise ValueError(
                        f"account_id={account.id} does not have region set"
                    )

                if (
                    tank_stats := await wg_api.get_tank_stats(
                        account.id, account.region
                    )
                ) is None:
                    debug(f"Could not fetch account: {account.id}")
                    if retryQ is not None:
                        stats.log("accounts to re-try")
                        await retryQ.put(account)
                    else:
                        stats.log("accounts w/o stats")
                        account.disabled = True
                        await db.account_update(account=account, fields=["disabled"])
                        stats.log("accounts disabled")
                else:
                    await statsQ.put(tank_stats)
                    stats.log("tank stats fetched", len(tank_stats))
                    stats.log("accounts /w stats")
                    if disabled:
                        account.disabled = False
                        await db.account_update(account=account, fields=["disabled"])
                        stats.log("accounts enabled")

            except Exception as err:
                stats.log("errors")
                error(f"{err}")
            finally:
                accountQ.task_done()
    except QueueDone:
        debug("accountQ has been processed")
    except CancelledError:
        debug("Cancelled")
    except Exception as err:
        error(f"{err}")
    finally:
        if retryQ is not None:
            await retryQ.finish_producer()

    stats.log(
        "accounts total", -stats.get_value("accounts to re-try")
    )  # remove re-tried accounts from total
    return stats


async def fetch_backend_worker(
    db: Backend, statsQ: Queue[List[TankStat]], force: bool = False
) -> EventCounter:
    """Async worker to add tank stats to backend. Assumes batch is for the same account"""
    debug("starting")
    stats: EventCounter = EventCounter(f"db: {db.driver}")
    added: int
    not_added: int
    account: BSAccount | None
    account_id: int
    last_battle_time: int
    rel: BSBlitzRelease | None

    try:
        releases: NearestDict[int, BSBlitzRelease] = await release_mapper(db)
        while True:
            added = 0
            not_added = 0
            last_battle_time = -1
            account = None
            tank_stats: List[TankStat] = await statsQ.get()

            try:
                if len(tank_stats) > 0:
                    debug(f"Read {len(tank_stats)} from queue")

                    last_battle_time = max([ts.last_battle_time for ts in tank_stats])
                    for ts in tank_stats:
                        if (rel := releases[(ts.last_battle_time)]) is not None:
                            ts.release = rel.release
                        else:
                            error(
                                f"could not map release last_battle_time={ts.last_battle_time}"
                            )
                            stats.log("release mapping errors")

                    added, not_added = await db.tank_stats_insert(
                        tank_stats, force=force
                    )
                    account_id = tank_stats[0].account_id
                    if (account := await db.account_get(account_id=account_id)) is None:
                        account = BSAccount(id=account_id)
                    account.last_battle_time = last_battle_time
                    account.stats_updated(StatsTypes.tank_stats)
                    if added > 0:
                        stats.log("accounts /w new stats")
                        if account.inactive:
                            stats.log("accounts marked active")
                        account.inactive = False
                    else:
                        stats.log("accounts w/o new stats")

                    await db.account_insert(account=account, force=True)
            except Exception as err:
                error(f"{err}")
            finally:
                stats.log("tank stats added", added)
                stats.log("old tank stats found", not_added)
                debug(f"{added} tank stats added, {not_added} old tank stats found")
                statsQ.task_done()
    except CancelledError:
        debug("Cancelled")
    except Exception as err:
        error(f"{err}")
    return stats


########################################################
#
# cmd_edit()
#
########################################################


async def cmd_edit(db: Backend, args: Namespace) -> bool:
    try:
        debug("starting")
        release: BSBlitzRelease | None = None
        if args.release is not None:
            # release = BSBlitzRelease(release=args.release)
            release = await db.release_get(release=args.release)
        stats: EventCounter = EventCounter("tank-stats edit")
        regions: set[Region] = {Region(r) for r in args.regions}
        since: int = 0
        if args.since is not None:
            since = int(datetime.fromisoformat(args.since).timestamp())
        accounts: List[BSAccount] | None = read_args_accounts(args.accounts)
        sample: float = args.sample
        commit: bool = args.commit
        tank_statQ: IterableQueue[TankStat] = IterableQueue(maxsize=TANK_STATS_Q_MAX)
        edit_tasks: List[Task] = list()
        message("Counting tank-stats to scan for edits")

        if args.tank_stats_edit_cmd == "remap-release":
            N: int = await db.tank_stats_count(
                release=release,
                regions=regions,
                accounts=accounts,
                since=since,
                sample=sample,
            )
            edit_tasks.append(
                create_task(cmd_edit_rel_remap(db, tank_statQ, commit, N))
            )

            stats.merge_child(
                await db.tank_stats_get_worker(
                    tank_statQ,
                    release=release,
                    regions=regions,
                    accounts=accounts,
                    since=since,
                    sample=sample,
                )
            )
            await tank_statQ.join()

        ################################################################################
        #
        ## DEPRECIATED: This function is not needed anymore. Comment out, but keep.
        #
        ################################################################################

        # elif args.tank_stats_edit_cmd == 'fix-missing':
        # 	BATCH: int = 100
        # 	message(f'Fixing {args.field} for release {release}')
        # 	writeQ : CounterQueue[List[TankStat]]  = CounterQueue(maxsize=TANK_STATS_Q_MAX,
        # 					   										batch=BATCH)
        # 	for _ in range(args.workers):
        # 		edit_tasks.append(create_task(cmd_edit_fix_missing(db, tank_statQ,
        # 															writeQ=writeQ,
        # 															commit=commit,
        # 															batch = BATCH)))
        # 	if commit:
        # 		edit_tasks.append(create_task(db.tank_stats_insert_worker(writeQ, force=True)))
        # 		edit_tasks.append(create_task(alive_bar_monitor([writeQ],
        # 				   									title=f'Fixing {args.field}',
        # 													total=None)))
        # 	else:
        # 		edit_tasks.append(create_task(alive_bar_monitor([writeQ],
        # 				   									title=f'Analyzing {args.field}',
        # 													total=None)))
        # 	for r in regions:
        # 		edit_tasks.append(create_task(db.tank_stats_get_worker(tank_statQ,
        # 																release=release,
        # 																regions={r},
        # 																accounts=accounts,
        # 																missing=args.field,
        # 																since=since,
        # 																sample=sample)))
        # 	await tank_statQ.join()
        # 	await writeQ.join()
        else:
            raise NotImplementedError

        await stats.gather_stats(edit_tasks, cancel=False)

        if (msg := stats.print(do_print=False, clean=True)) is not None:
            message(msg)

    except Exception as err:
        error(f"{err}")
    return False


################################################################################
#
## DEPRECIATED: This function is not needed anymore. Comment out, but keep.
#
################################################################################

# async def cmd_edit_fix_missing(db: Backend,
# 								tank_statQ : IterableQueue[TankStat],
# 								writeQ: Queue[List[TankStat]],
# 								commit: bool = False,
# 								batch : int = 1000
# 								) -> EventCounter:
# 	"""Fix: move all.shots -> all.spots & missing all.spots fields by estimating
# 		those from later stats"""
# 	debug('starting')
# 	stats : EventCounter = EventCounter('fix missing')
# 	tank_stats : List[TankStat] = list()
# 	try:
# 		while True:
# 			ts : TankStat = await tank_statQ.get()
# 			try:
# 				fixed : bool = False
# 				tsa: WGTankStatAll = ts.all
# 				if (region := ts.region) is None:
# 					stats.log('missing region')
# 					continue
# 				later_found : bool = False
# 				async for later in db.tank_stats_get(accounts=[BSAccount(id=ts.account_id, region=ts.region)],
# 													regions={region},
# 													tanks=[BSTank(tank_id=ts.tank_id)],
# 													since=ts.last_battle_time + 1):
# 					later_found = True
# 					try:
# 						ltsa = later.all
# 						if ltsa.battles <= 0:
# 							stats.log('bad stats: 0 battles')
# 							continue

# 						# Fix shots
# 						if tsa.spotted >= tsa.hits:
# 							tsa.shots = tsa.spotted
# 							stats.log('all.shots = all.spots')
# 						else:
# 							tsa.shots = int(ltsa.shots * tsa.battles / ltsa.battles)
# 							stats.log('all.shots estimated')

# 						# Fix spotted
# 						tsa.spotted = int(ltsa.spotted * tsa.battles / ltsa.battles)
# 						stats.log('all.spotted estimated')

# 						ts.all = tsa
# 						debug(f'updating {ts}: all.spotted, all.shots')
# 						if commit:
# 							tank_stats.append(ts)
# 							if len(tank_stats) == batch:
# 								await writeQ.put(tank_stats)
# 								stats.log('fixed', batch)
# 								tank_stats = list()
# 						else:
# 							message(f"Would fix: account={ts.account_id} tank={ts.tank_id} lbt={ts.last_battle_time}: spotted={ts.all.spotted}, shots={ts.all.shots} (btls={tsa.battles}) [{ltsa.spotted}, {ltsa.shots} (btls={ltsa.battles})]")
# 							stats.log('would fix')
# 						fixed = True
# 						break

# 					except Exception as err:
# 						verbose(f'Failed to update {ts}: {err}')
# 						stats.log('errors')
# 				if not fixed:
# 					stats.log('could not fix')
# 					if not later_found:
# 						stats.log('no later stats found')
# 					verbose(f'could not fix: {ts}')
# 			except Exception as err:
# 				error(f'failed to fix {ts}')
# 			finally:
# 				tank_statQ.task_done()

# 	except QueueDone:
# 		debug('Queue done')
# 		if len(tank_stats) > 0:
# 			await writeQ.put(tank_stats)
# 			stats.log('fixed', len(tank_stats))
# 	except CancelledError:
# 		debug('cancelled')
# 	except Exception as err:
# 		error(f'{err}')
# 	return stats


async def cmd_edit_rel_remap(
    db: Backend,
    tank_statQ: IterableQueue[TankStat],
    commit: bool = False,
    total: int | None = None,
) -> EventCounter:
    """Remap tank stat's releases"""
    debug("starting")
    stats: EventCounter = EventCounter("remap releases")
    try:
        release: BSBlitzRelease | None
        releases: NearestDict[int, BSBlitzRelease] = await release_mapper(db)
        if commit:
            stats.log("updated", 0)
        else:
            stats.log("would update", 0)
        bar: tqdm = tqdm(
            total=total,
            desc="Remapping tank stats' releases",
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed} ETA {remaining} {rate_fmt}]",
            unit="",
            leave=True,
            position=0,
        )

        async for ts in tank_statQ:
            try:
                if (release := releases[ts.last_battle_time]) is None:
                    error(f"Could not map: {ts}")
                    stats.log("errors")
                    continue
                if ts.release != release.release:
                    if commit:
                        ts.release = release.release
                        debug(f"Remapping {ts.release} to {release.release}: {ts}")
                        if await db.tank_stat_update(ts, fields=["release"]):
                            # debug(f"remapped release for {ts}")
                            stats.log("updated")
                        else:
                            # debug(f"failed to remap release for {ts}")
                            stats.log("failed to update")
                    else:
                        stats.log("would update")
                        verbose(
                            f"would update release {ts.release:<7} to {release.release:<7} for {ts}"
                        )
                else:
                    # debug(f"No need to remap: {ts}")
                    stats.log("no need")
            except Exception as err:
                error(f"could not remap {ts}: {err}")
            except (CancelledError, KeyboardInterrupt):
                message("Cancelled")
                break
            finally:
                tank_statQ.task_done()
                bar.update(1)

        # with alive_bar(
        #     total,
        #     title="Remapping tank stats' releases ",
        #     refresh_secs=1,
        #     enrich_print=False,
        # ) as bar:
        #     while True:
        #         ts: TankStat = await tank_statQ.get()
        #         try:
        #             if (release := releases[ts.last_battle_time]) is None:
        #                 error(f"Could not map: {ts}")
        #                 stats.log("errors")
        #                 continue
        #             if ts.release != release.release:
        #                 if commit:
        #                     ts.release = release.release
        #                     debug(f"Remapping {ts.release} to {release.release}: {ts}")
        #                     if await db.tank_stat_update(ts, fields=["release"]):
        #                         debug(f"remapped release for {ts}")
        #                         stats.log("updated")
        #                     else:
        #                         debug(f"failed to remap release for {ts}")
        #                         stats.log("failed to update")
        #                 else:
        #                     stats.log("would update")
        #                     verbose(
        #                         f"would update release {ts.release:<7} to {release.release:<7} for {ts}"
        #                     )
        #             else:
        #                 debug(f"No need to remap: {ts}")
        #                 stats.log("no need")
        #         except Exception as err:
        #             error(f"could not remap {ts}: {err}")
        #         finally:
        #             tank_statQ.task_done()
        #             bar()
    except QueueDone:
        debug("tank stat queue processed")
    except CancelledError:
        debug("Cancelled")
    except Exception as err:
        error(f"{err}")
    return stats


########################################################
#
# cmd_prune()
#
########################################################


async def cmd_prune(db: Backend, args: Namespace) -> bool:
    """prune tank stats"""
    # performance can be increased 5-10x with multiprocessing
    debug("starting")
    try:
        regions: set[Region] = {Region(r) for r in args.regions}
        sample: int = args.sample
        release: BSBlitzRelease = BSBlitzRelease(release=args.release)
        stats: EventCounter = EventCounter(f"tank-stats prune {release}")
        commit: bool = args.commit
        tankQ: IterableQueue[BSTank] = IterableQueue()
        workers: List[Task] = list()
        WORKERS: int = args.workers
        if sample > 0:
            WORKERS = 1
        progress_str: str = "Finding duplicates "
        if commit:
            message(
                f"Pruning tank stats of release {release} from {db.table_uri(BSTableType.TankStats)}"
            )
            message("Press CTRL+C to cancel in 3 secs...")
            await sleep(3)
            progress_str = "Pruning duplicates "
        await tankQ.add_producer()
        async for tank_id in db.tank_stats_unique(
            "tank_id", int, release=release, regions=regions
        ):
            await tankQ.put(BSTank(tank_id=tank_id))
        await tankQ.finish_producer()
        # N : int = await db.tankopedia_count()
        N: int = tankQ.qsize()

        for _ in range(WORKERS):
            workers.append(
                create_task(
                    prune_worker(
                        db,
                        tankQ=tankQ,
                        release=release,
                        regions=regions,
                        commit=commit,
                        sample=sample,
                    )
                )
            )
        # prev: int = 0
        # done: int
        # left: int
        bar: tqdm = tqdm(
            total=N,
            desc=progress_str,
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed} ETA {remaining} {rate_fmt}]",
            unit="",
            leave=True,
            position=0,
        )
        await tqdm_monitorQ(tankQ, bar)

        # with alive_bar(N, title=progress_str, refresh_secs=1) as bar:
        #     while (left := tankQ.qsize()) > 0:
        #         done = N - left
        #         if (done - prev) > 0:
        #             bar(done - prev)
        #         prev = done
        #         await sleep(1)

        await tankQ.join()
        await stats.gather(workers, cancel=False)
        stats.print()
        return True
    except Exception as err:
        error(f"{err}")
    return False


async def prune_worker(
    db: Backend,
    tankQ: IterableQueue[BSTank],
    release: BSBlitzRelease,
    regions: set[Region],
    commit: bool = False,
    sample: int = 0,
) -> EventCounter:
    """Worker to delete duplicates"""
    debug("starting")
    stats: EventCounter = EventCounter("duplicates")
    try:
        async for tank in tankQ:
            async for dup in db.tank_stats_duplicates(tank, release, regions):
                stats.log("found")
                if commit:
                    if await db.tank_stat_delete(
                        account_id=dup.account_id,
                        last_battle_time=dup.last_battle_time,
                        tank_id=tank.tank_id,
                    ):
                        verbose(f"deleted duplicate: {dup}")
                        stats.log("deleted")
                    else:
                        error(f"failed to delete duplicate: {dup}")
                        stats.log("deletion errors")
                else:
                    async for newer in db.tank_stats_get(
                        releases={release},
                        regions=regions,
                        accounts=[BSAccount(id=dup.account_id)],
                        tanks=[tank],
                        since=dup.last_battle_time + 1,
                    ):
                        message(f"duplicate:  {dup}")
                        message(f"newer stat: {newer}")
                if sample == 1:
                    raise CancelledError
    except CancelledError:
        debug("cancelled")
    except Exception as err:
        error(f"{err}")
    return stats


########################################################
#
# cmd_export_text()
#
########################################################


async def cmd_export_text(db: Backend, args: Namespace) -> bool:
    try:
        debug("starting")
        assert type(args.sample) in [int, float], 'param "sample" has to be a number'

        stats: EventCounter = EventCounter("tank-stats export")
        regions: set[Region] = {Region(r) for r in args.regions}
        filename: str = args.filename
        force: bool = args.force
        export_stdout: bool = filename == "-"
        sample: float = args.sample
        accounts: List[BSAccount] | None = read_args_accounts(args.accounts)
        tanks: List[BSTank] | None = read_args_tanks(args.tanks)
        release: BSBlitzRelease | None = None
        if args.release is not None:
            release = (await get_releases(db, [args.release]))[0]

        tank_statQs: Dict[str, IterableQueue[TankStat]] = dict()
        backend_worker: Task
        export_workers: List[Task] = list()

        total: int = await db.tank_stats_count(
            regions=regions,
            sample=sample,
            accounts=accounts,
            tanks=tanks,
            release=release,
        )

        tank_statQs["all"] = IterableQueue(
            maxsize=ACCOUNTS_Q_MAX, count_items=not args.by_region
        )
        # fetch tank_stats for all the regios
        backend_worker = create_task(
            db.tank_stats_get_worker(
                tank_statQs["all"],
                regions=regions,
                sample=sample,
                accounts=accounts,
                tanks=tanks,
                release=release,
            )
        )
        if args.by_region:
            for region in regions:
                tank_statQs[region.name] = IterableQueue(maxsize=ACCOUNTS_Q_MAX)
                export_workers.append(
                    create_task(
                        export(
                            iterable=tank_statQs[region.name],
                            format=args.format,
                            filename=f"{filename}.{region.name}",
                            force=force,
                            append=args.append,
                        )
                    )
                )
            # split by region
            export_workers.append(
                create_task(
                    split_tank_statQ_by_region(
                        Q_all=tank_statQs["all"], regionQs=tank_statQs
                    )
                )
            )
        else:
            if filename != "-":
                filename += ".all"
            export_workers.append(
                create_task(
                    export(
                        iterable=tank_statQs["all"],
                        format=args.format,
                        filename=filename,
                        force=force,
                        append=args.append,
                    )
                )
            )
        monitors: list[Task] = list()
        if not export_stdout:
            bar: tqdm = tqdm(total=total, desc="Exporting tank_stats")
            for Q in tank_statQs.values():
                monitors.append(create_task(tqdm_monitorQ(Q, bar)))

        await wait([backend_worker])
        for queue in tank_statQs.values():
            await queue.join()

        await stats.gather_stats([backend_worker], cancel=False)
        await stats.gather_stats(export_workers, cancel=False)
        if not export_stdout:
            if (msg := stats.print(do_print=False, clean=True)) is not None:
                message(msg)

    except Exception as err:
        error(f"{err}")
    return False


########################################################
#
# cmd_export_career()
#
########################################################


async def cmd_export_career(db: Backend, args: Namespace) -> bool:
    """Export career stats for accounts who played during release"""
    debug("starting")
    # global export_total_rows
    assert args.format in EXPORT_DATA_FORMATS, "--format has to be 'arrow' or 'parquet'"
    WORKERS: int = args.workers
    stats: EventCounter = EventCounter("tank-stats export")

    try:
        regions: set[Region] = {Region(r) for r in args.regions}
        release: BSBlitzRelease = BSBlitzRelease(release=args.RELEASE)
        force: bool = args.force
        format: str = args.format
        basedir: str = os.path.join(
            args.basedir,
            release.release,
            "career_" + ("after" if args.after else "before"),
        )
        options: Dict[str, Any] = dict()
        options["release"] = release
        options["after"] = args.after

        message(f"Exporting career stats for release {release}")

        if WORKERS > 0:
            WORKERS = min([cpu_count() - 1, WORKERS])
        else:
            WORKERS = cpu_count() - 1

        with Manager() as manager:
            dataQ: queue.Queue[pd.DataFrame] = manager.Queue(TANK_STATS_Q_MAX)
            workQ: queue.Queue[BSAccount | None] = manager.Queue(100)
            adataQ: AsyncQueue[pd.DataFrame] = AsyncQueue(dataQ)
            aworkQ: AsyncQueue[BSAccount | None] = AsyncQueue(workQ)
            partioning: ds.Partioning = ds.partitioning(
                pa.schema([("region", pa.string())])
            )
            schema: pa.Schema = TankStat.arrow_schema()
            writer: Task = create_task(
                dataset_writer(
                    basedir,
                    adataQ,
                    format,
                    partioning=partioning,
                    schema=schema,
                    force=force,
                )
            )

            with Pool(
                processes=WORKERS,
                initializer=export_career_mp_init,
                initargs=[db.config, workQ, dataQ, options],
            ) as pool:
                message("Counting accounts played during release...")
                N: int = await db.tank_stats_unique_count(
                    "account_id", release=release, regions=regions
                )
                Qcreator: Task = create_task(
                    create_accountQ_active(db, aworkQ, release, regions, randomize=True)
                )
                debug(f"starting {WORKERS} workers")
                results: AsyncResult = pool.map_async(
                    export_career_stats_mp_worker_start, range(WORKERS)
                )
                pool.close()

                prev: int = 0
                done: int = 0
                delta: int = 0
                with tqdm(
                    total=N,
                    desc="Exporting tank stats",
                    bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed} ETA {remaining} {rate_fmt}]",
                    unit="",
                    leave=True,
                    position=0,
                ) as bar:
                    while not Qcreator.done():
                        done = aworkQ.items
                        delta = done - prev
                        if delta > 0:
                            bar.update(delta)
                        prev = done
                        await sleep(1)
                # with alive_bar(
                #     N, title="Exporting tank stats ", enrich_print=False, refresh_secs=1
                # ) as bar:
                #     while not Qcreator.done():
                #         done = aworkQ.items
                #         delta = done - prev
                #         if delta > 0:
                #             bar(delta)
                #         prev = done
                #         await sleep(1)

                for _ in range(WORKERS):
                    await aworkQ.put(None)
                for res in results.get():
                    stats.merge_child(res)
                pool.join()

            await adataQ.join()
            await stats.gather_stats([writer, Qcreator])

    except Exception as err:
        error(f"{err}")
    stats.print()
    return False


def export_career_mp_init(
    backend_config: Dict[str, Any],
    accountQ: queue.Queue[BSAccount | None],
    dataQ: queue.Queue[pd.DataFrame],
    options: Dict[str, Any],
):
    """Initialize static/global backend into a forked process"""
    global db, workQ_a, writeQ, mp_options
    debug(f"starting (PID={getpid()})")

    if (tmp_db := Backend.create(**backend_config)) is None:
        raise ValueError("could not create backend")
    db = tmp_db
    workQ_a = AsyncQueue(accountQ)
    writeQ = AsyncQueue(dataQ)
    mp_options = options
    debug("finished")


def export_career_stats_mp_worker_start(worker: int = 0) -> EventCounter:
    """Forkable tank stats import worker cor career stats"""
    debug(f"starting import worker #{worker}")
    return asyncio.run(export_career_stats_mp_worker(worker), debug=False)


async def export_career_stats_mp_worker(worker: int = 0) -> EventCounter:
    """Forkable tank stats import worker for latest (career) stats"""
    global db, workQ_a, writeQ, mp_options

    debug(f"#{worker}: starting")
    stats: EventCounter = EventCounter("importer")
    THREADS: int = 4

    try:
        release: BSBlitzRelease = mp_options["release"]
        after: bool = mp_options["after"]
        workers: List[Task] = list()
        if not after:
            if (rel := await db.release_get_previous(release)) is None:
                raise ValueError(f"could not find previous release: {release}")
            else:
                release = rel

        for _ in range(THREADS):
            workers.append(
                create_task(export_career_fetcher(db, workQ_a, writeQ, release))
            )

        for w in workers:
            stats.merge_child(await w)
        debug(f"#{worker}: async workers done")
    except CancelledError:
        pass
    except Exception as err:
        error(f"{err}")
    return stats


async def export_career_fetcher(
    db: Backend,
    accountQ: AsyncQueue[BSAccount | None],
    dataQ: AsyncQueue[pd.DataFrame],
    release: BSBlitzRelease,
) -> EventCounter:
    """Fetch tanks stats data from backend and convert to Pandas data frames"""
    debug("starting")
    stats: EventCounter = EventCounter(f"fetch {db.driver}")
    datas: List[Dict[str, Any]] = list()
    tank_stats: List[TankStat]
    try:
        while (account := await accountQ.get()) is not None:
            try:
                async for tank_stats in db.tank_stats_export_career(
                    account=account, release=release
                ):
                    datas.extend([ts.obj_src() for ts in tank_stats])
                    if len(datas) >= TANK_STATS_BATCH:
                        # error(f'putting DF to dataQ: {len(tank_stats)} rows')
                        await dataQ.put(pd.json_normalize(datas))
                        stats.log("tank stats read", len(datas))
                        datas = list()
            except Exception as err:
                error(f"{err}")
            finally:
                stats.log("accounts")
                accountQ.task_done()

        if len(datas) > 0:
            await dataQ.put(pd.json_normalize(datas))
            stats.log("tank stats read", len(datas))

    except CancelledError:
        debug("cancelled")
    except Exception as err:
        error(f"{err}")

    accountQ.task_done()
    await accountQ.put(None)

    return stats


########################################################
#
# cmd_export_update()
#
########################################################


async def cmd_export_update(db: Backend, args: Namespace) -> bool:
    debug("starting")
    assert args.format in EXPORT_DATA_FORMATS, "--format has to be 'arrow' or 'parquet'"
    MAX_WORKERS: int = 16
    stats: EventCounter = EventCounter("tank-stats export")
    try:
        regions: set[Region] = {Region(r) for r in args.regions}
        release: BSBlitzRelease = BSBlitzRelease(release=args.RELEASE)
        force: bool = args.force
        export_format: str = args.format
        basedir: str = os.path.join(args.basedir, release.release, "update_total")
        # filename 		: str			= args.filename
        options: Dict[str, Any] = dict()
        options["regions"] = regions
        options["release"] = release.release
        tank_id: int
        WORKERS: int = min([cpu_count() - 1, MAX_WORKERS])
        message(f"Exporting update stats for release {release}")

        with Manager() as manager:
            dataQ: queue.Queue[pd.DataFrame] = manager.Queue(TANK_STATS_Q_MAX)
            tankQ: queue.Queue[int | None] = manager.Queue()
            adataQ: AsyncQueue[pd.DataFrame] = AsyncQueue(dataQ)
            atankQ: AsyncQueue[int | None] = AsyncQueue(tankQ)
            partioning: ds.Partioning = ds.partitioning(
                pa.schema([("region", pa.string())])
            )
            schema: pa.Schema = TankStat.arrow_schema()
            worker: Task = create_task(
                dataset_writer(
                    basedir,
                    adataQ,
                    export_format,
                    partioning=partioning,
                    schema=schema,
                    force=force,
                )
            )

            with Pool(
                processes=WORKERS,
                initializer=export_update_mp_init,
                initargs=[db.config, tankQ, dataQ, options],
            ) as pool:
                async for tank_id in db.tank_stats_unique(
                    "tank_id", int, regions=regions, release=release
                ):
                    await atankQ.put(tank_id)

                debug(f"starting {WORKERS} workers, {atankQ.qsize()} tanks")
                results: AsyncResult = pool.map_async(
                    export_data_update_mp_worker_start, range(WORKERS)
                )
                pool.close()

                N: int = atankQ.qsize()
                left: int = N
                prev: int = 0
                done: int

                with tqdm(
                    total=N,
                    desc="Exporting tank stats",
                    bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed} ETA {remaining} {rate_fmt}]",
                    unit="",
                    leave=True,
                    position=0,
                ) as bar:
                    while True:
                        left = atankQ.qsize()
                        done = N - left
                        if (done - prev) > 0:
                            bar.update(done - prev)
                        prev = done
                        if left == 0:
                            break
                        await sleep(1)

                # with alive_bar(
                #     N, title="Exporting tank stats ", enrich_print=False, refresh_secs=1
                # ) as bar:
                #     while True:
                #         left = atankQ.qsize()
                #         done = N - left
                #         bar(done - prev)
                #         prev = done
                #         if left == 0:
                #             break
                #         await sleep(1)

                await atankQ.join()
                for _ in range(WORKERS):
                    await atankQ.put(None)
                for res in results.get():
                    stats.merge_child(res)
                pool.join()

            await adataQ.join()
            await stats.gather_stats([worker])

    except Exception as err:
        error(f"{err}")
    stats.print()
    return False


def export_update_mp_init(
    backend_config: Dict[str, Any],
    tankQ: queue.Queue[int | None],
    dataQ: queue.Queue[pd.DataFrame],
    options: Dict[str, Any],
):
    """Initialize static/global backend into a forked process"""
    global db, workQ_t, writeQ, mp_options
    debug(f"starting (PID={getpid()})")

    if (tmp_db := Backend.create(**backend_config)) is None:
        raise ValueError("could not create backend")
    db = tmp_db
    workQ_t = AsyncQueue(tankQ)
    writeQ = AsyncQueue(dataQ)
    mp_options = options
    debug("finished")


def export_data_update_mp_worker_start(worker: int = 0) -> EventCounter:
    """Forkable tank stats import worker"""
    debug(f"starting import worker #{worker}")
    return asyncio.run(export_data_update_mp_worker(worker), debug=False)


async def export_data_update_mp_worker(worker: int = 0) -> EventCounter:
    """Forkable tank stats import worker"""
    global db, workQ_t, writeQ, mp_options

    debug(f"#{worker}: starting")
    stats: EventCounter = EventCounter("importer")
    workers: List[Task] = list()
    THREADS: int = 4

    try:
        regions: set[Region] = mp_options["regions"]
        release: BSBlitzRelease = BSBlitzRelease(release=mp_options["release"])

        for _ in range(THREADS):
            workers.append(
                create_task(
                    export_update_fetcher(db, release, regions, workQ_t, writeQ)
                )
            )

        for w in workers:
            stats.merge_child(await w)
        debug(f"#{worker}: async workers done")
    except CancelledError:
        pass
    except Exception as err:
        error(f"{err}")
    return stats


async def export_update_fetcher(
    db: Backend,
    release: BSBlitzRelease,
    regions: set[Region],
    tankQ: AsyncQueue[int | None],
    dataQ: AsyncQueue[pd.DataFrame],
) -> EventCounter:
    """Fetch tanks stats data from backend and convert to Pandas data frames"""
    debug("starting")
    stats: EventCounter = EventCounter(f"fetch {db.driver}")
    tank_stats: List[Dict[str, Any]] = list()

    try:
        while (tank_id := await tankQ.get()) is not None:
            async for tank_stat in db.tank_stats_get(
                releases={release} if release is not None else set(),
                regions=regions,
                tanks=[BSTank(tank_id=tank_id)],
            ):
                tank_stats.append(tank_stat.obj_src())
                if len(tank_stats) == TANK_STATS_BATCH:
                    await dataQ.put(pd.json_normalize(tank_stats))
                    stats.log("tank stats read", len(tank_stats))
                    tank_stats = list()

            stats.log("tanks processed")
            tankQ.task_done()

        if len(tank_stats) > 0:
            await dataQ.put(pd.json_normalize(tank_stats))
            stats.log("tank stats read", len(tank_stats))

    except CancelledError:
        debug("cancelled")
    except Exception as err:
        error(f"{err}")

    tankQ.task_done()
    await tankQ.put(None)

    return stats


async def cmd_importMP(db: Backend, args: Namespace) -> bool:
    """Import tank stats from other backend"""
    try:
        debug("starting")
        stats: EventCounter = EventCounter("tank-stats import")
        import_db: Backend | None = None
        import_backend: str = args.import_backend
        import_model: type[JSONExportable] | None = None
        WORKERS: int = args.workers

        if WORKERS == 0:
            WORKERS = max([cpu_count() - 1, 1])

        if (import_model := get_sub_type(args.import_model, JSONExportable)) is None:
            raise ValueError("--import-model has to be subclass of JSONExportable")

        if (
            import_db := Backend.create_import_backend(
                driver=import_backend,
                args=args,
                import_type=BSTableType.TankStats,
                copy_from=db,
                config_file=args.import_config,
            )
        ) is None:
            raise ValueError(f"Could not init {import_backend} to import releases from")

        with Manager() as manager:
            readQ: queue.Queue[List[Any] | None] = manager.Queue(TANK_STATS_Q_MAX)
            options: Dict[str, Any] = dict()
            options["force"] = args.force
            options["map_releases"] = not args.no_release_map

            with Pool(
                processes=WORKERS,
                initializer=import_mp_init,
                initargs=[db.config, readQ, import_model, options],
            ) as pool:
                debug(f"starting {WORKERS} workers")
                results: AsyncResult = pool.map_async(
                    import_mp_worker_start, range(WORKERS)
                )
                pool.close()

                message("Counting tank stats to import ...")
                N: int = await import_db.tank_stats_count(sample=args.sample)

                # with alive_bar(
                #     N, title="Importing tank stats ", enrich_print=False, refresh_secs=1
                # ) as bar:
                with tqdm(
                    total=N,
                    desc=f"Importing {import_model.__name__} from {import_db.table_uri(BSTableType.TankStats)}",
                    bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed} ETA {remaining} {rate_fmt}]",
                    unit="",
                    leave=True,
                    position=0,
                ) as bar:
                    async for objs in import_db.objs_export(
                        table_type=BSTableType.TankStats, sample=args.sample
                    ):
                        read: int = len(objs)
                        # debug(f'read {read} tank_stat objects')
                        readQ.put(objs)
                        stats.log(f"{db.driver}:stats read", read)
                        bar.update(read)

                debug(
                    f"Finished exporting {import_model} from {import_db.table_uri(BSTableType.TankStats)}"
                )
                for _ in range(WORKERS):
                    readQ.put(None)  # add sentinel

                for res in results.get():
                    stats.merge_child(res)
                pool.join()

        if (msg := stats.print(do_print=False, clean=True)) is not None:
            message(msg)
        return True
    except Exception as err:
        error(f"{err}")
    return False


def import_mp_init(
    backend_config: Dict[str, Any],
    inputQ: queue.Queue,
    import_model: type[JSONExportable],
    options: Dict[str, Any],
):
    """Initialize static/global backend into a forked process"""
    global db, readQ, in_model, mp_options
    debug(f"starting (PID={getpid()})")

    if (tmp_db := Backend.create(**backend_config)) is None:
        raise ValueError("could not create backend")
    db = tmp_db
    readQ = AsyncQueue(inputQ)
    in_model = import_model
    mp_options = options
    debug("finished")


def import_mp_worker_start(id: int = 0) -> EventCounter:
    """Forkable tank stats import worker"""
    debug(f"starting import worker #{id}")
    return asyncio.run(import_mp_worker(id), debug=False)


async def import_mp_worker(id: int = 0) -> EventCounter:
    """Forkable tank stats import worker"""
    debug(f"#{id}: starting")
    stats: EventCounter = EventCounter("importer")
    workers: List[Task] = list()
    try:
        global db, readQ, in_model, mp_options
        THREADS: int = 4
        import_model: type[JSONExportable] = in_model
        releases: NearestDict[int, BSBlitzRelease] | None = None
        tank_statsQ: Queue[List[TankStat]] = Queue(100)
        force: bool = mp_options["force"]
        rel_map: bool = mp_options["map_releases"]
        tank_stats: List[TankStat]

        if rel_map:
            debug("mapping releases")
            releases = await release_mapper(db)

        for _ in range(THREADS):
            workers.append(
                create_task(
                    db.tank_stats_insert_worker(tank_statsQ=tank_statsQ, force=force)
                )
            )
        errors: int = 0
        mapped: int = 0
        while (objs := await readQ.get()) is not None:
            debug(f"#{id}: read {len(objs)} objects")
            try:
                read: int = len(objs)
                debug(f"read {read} documents")
                stats.log("stats read", read)
                tank_stats = TankStat.from_objs(objs=objs, in_type=import_model)
                errors = len(objs) - len(tank_stats)
                stats.log("tank stats read", len(tank_stats))
                stats.log("format errors", errors)
                if releases is not None:
                    tank_stats, mapped, errors = map_releases(tank_stats, releases)
                    stats.log("release mapped", mapped)
                    stats.log("not release mapped", read - mapped)
                    stats.log("release map errors", errors)
                await tank_statsQ.put(tank_stats)
            except Exception as err:
                error(f"{err}")
            finally:
                readQ.task_done()
        debug(f"#{id}: finished reading objects")
        readQ.task_done()
        await tank_statsQ.join()  # add sentinel for other workers
        await stats.gather_stats(workers)
    except CancelledError:
        pass
    except Exception as err:
        error(f"{err}")
    return stats


def map_releases(
    tank_stats: List[TankStat], releases: NearestDict[int, BSBlitzRelease]
) -> tuple[List[TankStat], int, int]:
    debug("starting")
    mapped: int = 0
    errors: int = 0
    res: List[TankStat] = list()
    for tank_stat in tank_stats:
        try:
            if (release := releases[tank_stat.last_battle_time]) is not None:
                tank_stat.release = release.release
                mapped += 1
        except Exception as err:
            error(f"{err}")
            errors += 1
        res.append(tank_stat)
    return res, mapped, errors


async def map_releases_worker(
    releases: NearestDict[int, BSBlitzRelease] | None,
    inputQ: Queue[List[TankStat]],
    outputQ: Queue[List[TankStat]],
) -> EventCounter:
    """Map tank stats to releases and pack those to List[TankStat] queue.
    map_all is None means no release mapping is done"""
    stats: EventCounter = EventCounter("Release mapper")
    release: BSBlitzRelease | None
    try:
        debug("starting")
        while True:
            tank_stats = await inputQ.get()
            stats.log("read", len(tank_stats))
            try:
                if releases is not None:
                    for tank_stat in tank_stats:
                        if (
                            release := releases[(tank_stat.last_battle_time)]
                        ) is not None:
                            tank_stat.release = release.release
                            stats.log("mapped")
                        else:
                            stats.log("could not map")
                else:
                    stats.log("not mapped", len(tank_stats))

                await outputQ.put(tank_stats)
            except Exception as err:
                error(f"Processing input queue: {err}")
            finally:
                inputQ.task_done()
    except CancelledError:
        debug("Cancelled")
    except Exception as err:
        error(f"{err}")
    return stats


async def split_tank_statQ_by_region(
    Q_all: IterableQueue[TankStat],
    regionQs: Dict[str, IterableQueue[TankStat]],
    progress: bool = False,
    bar_title: str = "Splitting tank stats queue",
) -> EventCounter:
    debug("starting")
    stats: EventCounter = EventCounter("tank stats")
    try:
        for Q in regionQs.values():
            await Q.add_producer()

        with tqdm(
            total=Q_all.qsize(),
            desc=bar_title,
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed} ETA {remaining} {rate_fmt}]",
            unit="",
            leave=True,
            position=0,
            disable=not progress,
        ) as bar:
            # with alive_bar(
            #     Q_all.qsize(),
            #     title=bar_title,
            #     manual=True,
            #     refresh_secs=1,
            #     enrich_print=False,
            #     disable=not progress,
            # ) as bar:
            async for tank_stat in Q_all:
                try:
                    if tank_stat.region is None:
                        raise ValueError(
                            f"account ({tank_stat.id}) does not have region defined"
                        )
                    if tank_stat.region.name in regionQs:
                        await regionQs[tank_stat.region.name].put(tank_stat)
                        stats.log(tank_stat.region.name)
                    else:
                        stats.log(f"excluded region: {tank_stat.region.name}")
                except CancelledError:
                    raise CancelledError from None
                except Exception as err:
                    stats.log("errors")
                    error(f"{err}")
                finally:
                    stats.log("total")
                    bar.update()

    except CancelledError:
        debug("Cancelled")
    except Exception as err:
        error(f"{err}")
    finally:
        for Q in regionQs.values():
            await Q.finish_producer()
    return stats


def read_args_tanks(tank_ids: List[int]) -> List[BSTank] | None:
    tanks: List[BSTank] = list()
    for id in tank_ids:
        tanks.append(BSTank(tank_id=id))
    if len(tanks) == 0:
        return None
    return tanks
