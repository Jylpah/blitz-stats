from argparse import ArgumentParser, Namespace
from configparser import ConfigParser
from typing import Optional, Any, Sequence, List, Dict
from datetime import datetime, timedelta
import logging
from asyncio import (
    create_task,
    gather,
    wait,
    timeout,
    Queue,
    CancelledError,
    Task,
    sleep,
)
from aiofiles import open
from asyncstdlib import enumerate
from alive_progress import alive_bar  # type: ignore
from pydantic import ValidationError
from tqdm import tqdm

# from icecream import ic  # type: ignore

from eventcounter import EventCounter
from queutils import IterableQueue, QueueDone
from pydantic_exportables.exportable import export
from pyutils.utils import chunker

from blitzmodels import (
    Region,
    Account,  # noqa
    AccountInfo,
    WGApi,
    add_args_wg,
)

from blitzmodels.wotinspector.wi_apiv2 import Replay, ReplaySummary, WoTinspector

from .backend import (
    Backend,
    # OptAccountsInactive,
    OptDistributed,
    batch_gen,
    BSTableType,
    ACCOUNTS_Q_MAX,
)
from .models import BSAccount, StatsTypes, BSBlitzRelease
from .utils import tqdm_monitorQ, tqdm_opts

logger = logging.getLogger(__name__)
error = logger.error
message = logger.warning
verbose = logger.info
debug = logger.debug

# wotinspector.com
WI_MAX_PAGES: int = 100
WI_MAX_OLD_REPLAYS: int = 30
WI_RATE_LIMIT: Optional[float] = None
WI_AUTH_TOKEN: Optional[str] = None


EXPORT_SUPPORTED_FORMATS: List[str] = ["json", "txt", "csv"]

ACCOUNT_INFO_CACHE_VALID: int = 7  # days

###########################################
#
# add_args_accouts functions
#
###########################################


def add_args(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
    try:
        debug("starting")
        accounts_parsers = parser.add_subparsers(
            dest="accounts_cmd",
            title="accounts commands",
            description="valid commands",
            metavar="fetch | update | export | remove",
        )
        accounts_parsers.required = True

        fetch_parser = accounts_parsers.add_parser(
            "fetch", aliases=["get"], help="accounts fetch help"
        )
        if not add_args_fetch(fetch_parser, config=config):
            raise Exception("Failed to define argument parser for: accounts fetch")

        update_parser = accounts_parsers.add_parser(
            "update", help="accounts update help"
        )
        if not add_args_update(update_parser, config=config):
            raise Exception("Failed to define argument parser for: accounts update")

        export_parser = accounts_parsers.add_parser(
            "export", help="accounts export help"
        )
        if not add_args_export(export_parser, config=config):
            raise Exception("Failed to define argument parser for: accounts export")

        remove_parser = accounts_parsers.add_parser(
            "remove", aliases=["rm"], help="accounts remove help"
        )
        if not add_args_remove(remove_parser, config=config):
            raise Exception("Failed to define argument parser for: accounts remove")

        import_parser = accounts_parsers.add_parser(
            "import", help="accounts import help"
        )
        if not add_args_import(import_parser, config=config):
            raise Exception("Failed to define argument parser for: accounts import")

        return True
    except Exception as err:
        error(f"{err}")
    return False


def add_args_update(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    try:
        debug("starting")
        update_parsers = parser.add_subparsers(
            dest="accounts_update_source",
            title="accounts update source",
            description="valid sources",
            metavar="wg | files",
        )
        update_parsers.required = True

        update_wg_parser = update_parsers.add_parser(
            "wg", help="accounts update wg help"
        )
        if not add_args_update_wg(update_wg_parser, config=config):
            raise Exception("Failed to define argument parser for: accounts update wg")

        update_files_parser = update_parsers.add_parser(
            "files", help="accounts update files help"
        )
        if not add_args_update_files(update_files_parser, config=config):
            raise Exception(
                "Failed to define argument parser for: accounts update files"
            )

        parser.add_argument(
            "--force",
            action="store_true",
            default=False,
            help="add accounts not found in the backend",
        )
        return True
    except Exception as err:
        error(f"{err}")
    return False


def add_args_update_files(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    """Update accounts from file(s)"""
    debug("add_args_update_files(): not implemented")
    return True


def add_args_update_wg(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    """Update existing accounts from WG API"""
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
            help="filter by region (default: " + " + ".join(Region.API_regions()) + ")",
        )
        parser.add_argument(
            "--disabled",
            action="store_true",
            default=False,
            help="Check existing disabled accounts",
        )
        parser.add_argument(
            "--active-since",
            type=str,
            default=None,
            metavar="RELEASE/DAYS",
            help="update account info for accounts that have been active since RELEASE/DAYS",
        )
        parser.add_argument(
            "--inactive-since",
            type=str,
            default=None,
            metavar="RELEASE/DAYS",
            help="update account info for accounts that have been inactive since RELEASE/DAYS",
        )
        # parser.add_argument(
        #     "--inactive",
        #     type=str,
        #     choices=[o.value for o in OptAccountsInactive],
        #     default=OptAccountsInactive.both.value,
        #     help="Include inactive accounts",
        # )
        parser.add_argument(
            "--accounts",
            type=str,
            default=[],
            nargs="*",
            metavar="ACCOUNT_ID [ACCOUNT_ID1 ...]",
            help="update accounts for the listed ACCOUNT_ID(s). \
                                    ACCOUNT_ID format 'account_id:region' or 'account_id'",
        )
        # parser.add_argument('--start', dest='wg_start_id',
        # 					metavar='ACCOUNT_ID', type=int, default=0,
        # 					help='start fetching account_ids from ACCOUNT_ID (default = 0 \
        # 						start from highest ACCOUNT_ID in backend)')
        parser.add_argument(
            "--distributed",
            "--dist",
            type=str,
            dest="distributed",
            metavar="I:N",
            default=None,
            help="Distributed stats fetching for accounts: id %% N == I",
        )
        parser.add_argument(
            "--cache-valid",
            type=float,
            default=ACCOUNT_INFO_CACHE_VALID,
            metavar="DAYS",
            help="update only accounts updated more than DAYS ago",
        )
        parser.add_argument(
            "--sample",
            type=float,
            default=0,
            metavar="SAMPLE",
            help="update SAMPLE of accounts. If 0 < SAMPLE < 1, SAMPLE defines a %% of users",
        )
        parser.add_argument(
            "--file",
            metavar="FILE",
            type=str,
            default=None,
            help="file to read accounts to update from",
        )
        parser.add_argument(
            "--format",
            type=str,
            choices=["json", "txt", "csv", "auto"],
            default="json",
            help="accounts list format",
        )

        return True
    except Exception as err:
        error(f"{err}")
    return False


def add_args_fetch(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    try:
        debug("starting")
        fetch_parsers = parser.add_subparsers(
            dest="accounts_fetch_source",
            title="accounts fetch source",
            description="valid sources",
            metavar="wg | wi | files",
        )
        fetch_parsers.required = True
        fetch_wg_parser = fetch_parsers.add_parser("wg", help="accounts fetch wg help")
        if not add_args_fetch_wg(fetch_wg_parser, config=config):
            raise Exception("Failed to define argument parser for: accounts fetch wg")

        fetch_wi_parser = fetch_parsers.add_parser("wi", help="accounts fetch wi help")
        if not add_args_fetch_wi(fetch_wi_parser, config=config):
            raise Exception("Failed to define argument parser for: accounts fetch wi")

        fetch_files_parser = fetch_parsers.add_parser(
            "files", help="accounts fetch files help"
        )
        if not add_args_fetch_files(fetch_files_parser, config=config):
            raise Exception(
                "Failed to define argument parser for: accounts fetch files"
            )

        parser.add_argument(
            "--force",
            action="store_true",
            default=False,
            help="Ignore existing accounts exporting",
        )

        return True
    except Exception as err:
        error(f"{err}")
    return False


def add_args_fetch_wg(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    try:
        debug("starting")
        NULL_RESPONSES: int = 2000
        if not add_args_wg(parser, config):
            return False
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
            "--start-id",
            dest="wg_start_id",
            metavar="ACCOUNT_ID",
            type=int,
            default=-1,
            help="start fetching account_ids from ACCOUNT_ID \
                    default = -1 start from highest ACCOUNT_ID in backend\
                    0 starts from the region(s) first ID",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            default=False,
            help="fetch accounts starting from --start ACCOUNT_ID",
        )
        parser.add_argument(
            "--max",
            dest="max_accounts",
            type=int,
            default=0,
            metavar="ACCOUNT_IDS",
            help="maximum number of accounts to try",
        )
        parser.add_argument(
            "--end",
            dest="null_responses",
            type=int,
            default=NULL_RESPONSES,
            metavar="N",
            help="end fetching accounts after N consequtive empty responses",
        )
        parser.add_argument(
            "--file",
            type=str,
            metavar="FILENAME",
            default=None,
            help="Read account_ids from FILENAME one account_id per line",
        )

        return True
    except Exception as err:
        error(f"{err}")
    return False


def add_args_fetch_wi(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    try:
        debug("starting")
        global WI_MAX_OLD_REPLAYS

        WI_RATE_LIMIT: float = 20 / 3600
        WI_MAX_PAGES: int = 100
        WI_AUTH_TOKEN: Optional[str] = None
        WI_WORKERS: int = 2

        if config is not None and "WOTINSPECTOR" in config.sections():
            configWI = config["WOTINSPECTOR"]
            WI_RATE_LIMIT = configWI.getfloat("rate_limit", WI_RATE_LIMIT)
            WI_MAX_PAGES = configWI.getint("max_pages", WI_MAX_PAGES)
            WI_WORKERS = configWI.getint("workers", WI_WORKERS)
            WI_AUTH_TOKEN = configWI.get("auth_token", WI_AUTH_TOKEN)
        parser.add_argument(
            "--max",
            "--max-pages",
            dest="wi_max_pages",
            type=int,
            default=WI_MAX_PAGES,
            metavar="MAX_PAGES",
            help="Maximum number of pages to spider",
        )
        parser.add_argument(
            "--start",
            "--start_page",
            dest="wi_start_page",
            metavar="START_PAGE",
            type=int,
            default=1,
            help="Start page to start spidering of WoTinspector.com",
        )
        parser.add_argument(
            "--workers",
            dest="wi_workers",
            type=int,
            default=WI_WORKERS,
            metavar="WORKERS",
            help="Number of async workers to spider wotinspector.com",
        )
        parser.add_argument(
            "--old-replay-limit",
            dest="wi_max_old_replays",
            type=int,
            default=WI_MAX_OLD_REPLAYS,
            metavar="OLD-REPLAYS",
            help="Cancel spidering after number of old replays found",
        )
        parser.add_argument(
            "--wi-auth-token",
            dest="wi_auth_token",
            type=str,
            default=WI_AUTH_TOKEN,
            metavar="AUTH_TOKEN",
            help="Start page to start spidering of WoTinspector.com",
        )
        parser.add_argument(
            "--wi-rate-limit",
            type=float,
            default=WI_RATE_LIMIT,
            metavar="RATE_LIMIT",
            help="Rate limit for WoTinspector.com",
        )

        return True
    except Exception as err:
        error(f"{err}")
    return False


def add_args_fetch_files(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    try:
        debug("starting")
        IMPORT_FORMAT = "txt"

        if config is not None and "ACCOUNTS" in config.sections():
            configAccs = config["ACCOUNTS"]
            IMPORT_FORMAT = configAccs.get("import_format", IMPORT_FORMAT)
        parser.add_argument(
            "--format",
            type=str,
            choices=["json", "txt", "csv", "auto"],
            default=IMPORT_FORMAT,
            help="Accounts list file format",
        )
        parser.add_argument(
            "files",
            metavar="FILE1 [FILE2 ...]",
            type=str,
            nargs="*",
            default="-",
            help="Files to read. Use '-' for STDIN",
        )
        return True
    except Exception as err:
        error(f"{err}")
    return False


def add_args_export(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    try:
        debug("starting")
        EXPORT_FORMAT = "txt"
        EXPORT_FILE = "accounts"

        if config is not None and "ACCOUNTS" in config.sections():
            configAccs = config["ACCOUNTS"]
            EXPORT_FORMAT = configAccs.get("export_format", EXPORT_FORMAT)
            EXPORT_FILE = configAccs.get("export_file", EXPORT_FILE)

        parser.add_argument(
            "format",
            type=str,
            nargs="?",
            choices=EXPORT_SUPPORTED_FORMATS,
            default=EXPORT_FORMAT,
            help="Accounts list file format",
        )
        parser.add_argument(
            "filename",
            metavar="FILE",
            type=str,
            nargs="?",
            default=EXPORT_FILE,
            help="File to export accounts to. Use '-' for STDIN",
        )
        parser.add_argument(
            "--append", action="store_true", default=False, help="Append to file(s)"
        )
        parser.add_argument(
            "--force",
            action="store_true",
            default=False,
            help="Overwrite existing file(s) when exporting",
        )
        parser.add_argument(
            "--accounts",
            type=str,
            default=[],
            nargs="*",
            metavar="ACCOUNT_ID [ACCOUNT_ID1 ...]",
            help="exports accounts for the listed ACCOUNT_ID(s). \
                                    ACCOUNT_ID format 'account_id:region' or 'account_id'",
        )
        parser.add_argument(
            "--disabled", action="store_true", default=False, help="Disabled accounts"
        )
        # parser.add_argument(
        #     "--inactive",
        #     type=str,
        #     choices=[o.value for o in OptAccountsInactive],
        #     default=OptAccountsInactive.no.value,
        #     help="Include inactive accounts",
        # )
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
            "--regions",
            "--region",
            type=str,
            nargs="*",
            choices=[r.value for r in Region.API_regions()],
            default=[r.value for r in Region.API_regions()],
            help="Filter by region (default is API = eu + com + asia)",
        )
        parser.add_argument(
            "--by-region",
            action="store_true",
            default=False,
            help="Export accounts by region",
        )
        parser.add_argument(
            "--distributed",
            "--dist",
            type=str,
            dest="distributed",
            metavar="I:N",
            default=None,
            help="Distributed stats fetching for accounts: id %% N == I",
        )
        parser.add_argument("--sample", type=float, default=0, help="Sample accounts")

        return True
    except Exception as err:
        error(f"{err}")
    return False


def add_args_import(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    """Add argument parser for accounts import"""
    try:
        debug("starting")

        import_parsers = parser.add_subparsers(
            dest="import_backend",
            title="accounts import backend",
            description="valid backends",
            metavar=" | ".join(Backend.list_available()),
        )
        import_parsers.required = True

        for backend in Backend.get_registered():
            import_parser = import_parsers.add_parser(
                backend.driver, help=f"accounts import {backend.driver} help"
            )
            if not backend.add_args_import(import_parser, config=config):
                raise Exception(
                    f"Failed to define argument parser for: accounts import {backend.driver}"
                )

        parser.add_argument(
            "--import-model",
            metavar="IMPORT-TYPE",
            type=str,
            required=True,
            choices=["BSAccount", "WG_Account"],
            help="Data format to import. Default is blitz-stats native format.",
        )
        parser.add_argument(
            "--regions",
            "--region",
            type=str,
            nargs="*",
            choices=[r.value for r in Region.API_regions()],
            default=[r.value for r in Region.API_regions()],
            help="Filter by region (default is API = eu + com + asia)",
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
            help="Overwrite existing file(s) when exporting",
        )

        return True
    except Exception as err:
        error(f"{err}")
    return False


def add_args_remove(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    try:
        debug("starting")
        IMPORT_FORMAT = "txt"

        if config is not None:
            if "ACCOUNTS" in config.sections():
                configAccs = config["ACCOUNTS"]
                IMPORT_FORMAT = configAccs.get("import_format", IMPORT_FORMAT)

        parser.add_argument(
            "--format",
            type=str,
            choices=["json", "txt", "csv"],
            default=IMPORT_FORMAT,
            help="Accounts list file format",
        )
        account_src_parser = parser.add_mutually_exclusive_group()
        account_src_parser.add_argument(
            "--file",
            metavar="FILE",
            type=str,
            default=None,
            help="File to export accounts to. Use '-' for STDIN",
        )
        account_src_parser.add_argument(
            "--accounts",
            metavar="ACCOUNT_ID [ACCOUNT_ID ...]",
            type=int,
            nargs="+",
            help="remove listed ACCOUNT_ID(s). \
					ACCOUNT_ID format 'account_id:region' or 'account_id'",
        )

        return True
    except Exception as err:
        error(f"{err}")
    return False


###########################################
#
# cmd_accouts functions
#
###########################################


async def cmd(db: Backend, args: Namespace) -> bool:
    try:
        debug("starting")
        if args.accounts_cmd == "fetch":
            return await cmd_fetch(db, args)

        elif args.accounts_cmd == "update":
            return await cmd_update(db, args)

        elif args.accounts_cmd == "export":
            return await cmd_export(db, args)

        elif args.accounts_cmd == "import":
            return await cmd_import(db, args)

        elif args.accounts_cmd == "remove":
            return await cmd_remove(db, args)

    except Exception as err:
        error(f"{err}")
    return False


async def cmd_update(db: Backend, args: Namespace) -> bool:
    try:
        debug("starting")

        stats = EventCounter("accounts update")
        updateQ: IterableQueue[BSAccount] = IterableQueue(maxsize=10000)
        db_worker = create_task(
            db.accounts_insert_worker(updateQ, force=True)
        )  # without force=True update fails

        try:
            if args.accounts_update_source == "wg":
                debug("wg")
                stats.merge_child(await cmd_update_wg(db, args, updateQ))
            else:
                raise ValueError(
                    f"unknown accounts update source: {args.accounts_update_source}"
                )

        except Exception as err:
            error(f"{err}")
        await updateQ.join()
        await stats.gather([db_worker], merge_child=True, cancel=True)
        stats.print()

    except Exception as err:
        error(f"{err}")
    return False


###########################################
#
# cmd_update_wg()
#
###########################################

# TODO: use multiprocessing to speed this up
#   - Pattern from tanks.cmd_fecthMP()
#   - Separate async threads to count items and update totals
#   - Each worker processes all the regions, identify region from the first account.region
#   - Use args.dist to divide the work between multiprocessing workers
#   - divide rate limit by number of workers
#   - Current performance is ~500 accounts / second. There is room for upti 6000/sec
#   - Use Yappi for profiling first


async def cmd_update_wg(
    db: Backend, args: Namespace, updateQ: IterableQueue[BSAccount]
) -> EventCounter:
    """Update accounts from WG API"""
    debug("starting")
    stats: EventCounter = EventCounter("WG API")
    BATCH: int = 100  # max number of accounts to fetch in one request
    try:
        regions: set[Region] = {Region(r) for r in args.regions}
        wg: WGApi = WGApi(
            app_id=args.wg_app_id,
            rate_limit=args.wg_rate_limit,
        )
        WORKERS: int = max([args.wg_workers, 1])
        accountQ: IterableQueue[BSAccount] = IterableQueue(maxsize=ACCOUNTS_Q_MAX)
        workQ_creators: List[Task] = list()
        api_workers: List[Task] = list()
        workQs: Dict[Region, IterableQueue[List[BSAccount]]] = dict()

        # for region in regions:
        #     workQs[region] = IterableQueue(maxsize=3)
        #     workQ_creators.append(
        #         create_task(
        #             create_accountQ_batch(
        #                 db,
        #                 args,
        #                 region,
        #                 accountQ=workQs[region],
        #                 stats_type=StatsTypes.account_info,
        #             )
        #         )
        #     )

        for region in regions:
            workQs[region] = IterableQueue(maxsize=50)
            for _ in range(WORKERS):
                api_workers.append(
                    create_task(
                        update_account_info_worker(
                            wg, region, workQ=workQs[region], updateQ=updateQ
                        )
                    )
                )

        workQ_creators.append(
            create_task(
                create_accountQ(
                    db, args, accountQ=accountQ, stats_type=StatsTypes.account_info
                )
            )
        )
        workQ_creators.append(
            create_task(split_accountQ_batch(accountQ, workQs, batch=BATCH))
        )

        # for region in regions:
        #     workQs[region] = IterableQueue(maxsize=50)
        #     workQ_creators.append(
        #         create_task(
        #             create_accountQ_batch(
        #                 db,
        #                 args,
        #                 region,
        #                 accountQ=workQs[region],
        #                 stats_type=StatsTypes.account_info,
        #             )
        #         )
        #     )

        message(
            "Updating accounts from WG API:"
            + (
                f" active since={args.active_since}"
                if args.active_since is not None
                else ""
            )
            + (
                f" inactive since={args.inactive_since}"
                if args.inactive_since is not None
                else ""
            )
            + (f" sample={int(args.sample)}" if args.sample > 0 else "")
        )
        # cancel: bool = False
        with tqdm(
            desc=f"{', '.join(regions)}",
            total=int(args.sample),
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed} ETA {remaining} {rate_fmt}]",
            unit="",
            leave=True,
        ) as bar:
            try:
                prev: int = 0
                done: int
                while not updateQ.is_done:
                    done = updateQ.count
                    if done - prev > 0:
                        bar.update(done - prev)

                    # if args.sample >= 1:
                    #     if not cancel and (
                    #         accountQ.qsize() + accountQ.wip + accountQ.count
                    #         >= args.sample
                    #     ):
                    #         for workQ in workQs.values():
                    #             await workQ.finish_producer(all=True)
                    #         cancel = True
                    #     if updateQ.count + updateQ.qsize() + updateQ.wip >= args.sample:
                    #         await updateQ.finish_producer(all=True)
                    #     if updateQ.count >= args.sample:
                    #         raise CancelledError("Sample size reached")

                    prev = done
                    await sleep(0.05)

            except KeyboardInterrupt:
                message("cancelled")
            #    cancel = True
            # except CancelledError as err:
            #     debug(f"{err}")

        await stats.gather(workQ_creators, merge_child=False)
        for region, workQ in workQs.items():
            debug(f"waiting for workQ for {region} to complete")
            await workQ.join()
        await stats.gather(api_workers, merge_child=True)
        wg.print()
        async with timeout(3):
            await wg.close()
    except Exception as err:
        error(f"{err}")
    return stats


async def update_account_info_worker(
    wg: WGApi,
    region: Region,
    workQ: IterableQueue[List[BSAccount]],
    updateQ: IterableQueue[BSAccount],
) -> EventCounter:
    """Update accounts with data from WG API accounts/info"""
    debug("starting")
    stats: EventCounter = EventCounter(f"{region}")
    infos: List[AccountInfo] | None
    account: BSAccount

    try:
        await updateQ.add_producer()
        async for account_batch in workQ:
            accounts: Dict[int, BSAccount] = dict()
            for account in account_batch:
                accounts[account.id] = account
            N: int = len(accounts)
            try:
                stats.log("account_ids", N)
                if N == 0 or N > 100:
                    raise ValueError(f"Incorrect number of account_ids give {N}")

                ids: List[int] = list(accounts.keys())
                ids_stats: List[int] = list()
                if (infos := await wg.get_account_info(ids, region)) is not None:
                    stats.log("stats found", len(infos))

                    # accounts with stats
                    for info in infos:
                        try:
                            account = accounts[info.account_id]
                            ids_stats.append(info.account_id)
                            # error(f'updating account_id={a.id}: {info}')
                            if account.update_info(info):
                                debug(
                                    "account_id=%d region=%s: updated",
                                    account.id,
                                    account.region,
                                )
                                stats.log("updated")
                            else:
                                debug(
                                    "account_id=%d region=%s: not updated",
                                    account.id,
                                    account.region,
                                )
                                stats.log("not updated")
                            account.disabled = False
                            await updateQ.put(
                                account
                            )  # to updated account_info_updated
                        except KeyError as err:
                            error(f"{err}")
                        except QueueDone:
                            debug("updateQ is done")
                            break

                    # accounts w/o stats
                    no_stats: set[int] = set(ids) - set(ids_stats)
                    if len(no_stats) > 0:
                        # accounts without stats will bve disabled when fetching tank stats
                        stats.log("no stats", len(no_stats))
                else:
                    stats.log("query errors")

            except ValueError:
                stats.log("errors", N)
            except Exception as err:
                stats.log("errors")
                error(f"{err}")

    except KeyboardInterrupt:
        debug("cancelled")
    except Exception as err:
        error(f"{err}")
    finally:
        await updateQ.finish_producer()
    return stats


async def cmd_fetch(db: Backend, args: Namespace) -> bool:
    try:
        debug("starting")

        stats = EventCounter("accounts fetch")
        accountQ: IterableQueue[BSAccount] = IterableQueue(maxsize=ACCOUNTS_Q_MAX)
        db_worker = create_task(db.accounts_insert_worker(accountQ))

        try:
            if args.accounts_fetch_source == "wg":
                debug("wg")
                stats.merge_child(await cmd_fetch_wg(db, args, accountQ))

            if args.accounts_fetch_source == "wi":
                debug("wi")
                stats.merge_child(await cmd_fetch_wi(db, args, accountQ))

            elif args.accounts_fetch_source == "files":
                debug("files")
                stats.merge_child(await cmd_fetch_files(db, args, accountQ))
        except Exception as err:
            error(f"{err}")

        await accountQ.join()
        await stats.gather_stats([db_worker])
        stats.print()

    except Exception as err:
        error(f"{err}")
    return False


# async def add_worker(db: Backend, accountQ: Queue[List[BSAccount]]) -> EventCounter:
# 	"""worker to read accounts from queue and add those to backend"""
# 	## REFACTOR: use Queue[List[BSAccount]] instead
# 	debug('starting')
# 	stats 		: EventCounter = EventCounter(f'{db.driver}')
# 	added 		: int
# 	not_added 	: int
# 	try:
# 		while True:
# 			accounts : List[BSAccount] = await accountQ.get()
# 			try:
# 				debug(f'Read {len(accounts)} from queue')
# 				stats.log('accounts total', len(accounts))
# 				try:
# 					added, not_added= await db.accounts_insert(accounts)
# 					stats.log('accounts added', added)
# 					stats.log('old accounts found', not_added)
# 				except Exception as err:
# 					stats.log('errors')
# 					error(f'Cound not add accounts do {db.backend}: {err}')
# 			except Exception as err:
# 				error(f'{err}')
# 			finally:
# 				accountQ.task_done()
# 	except QueueDone:
# 		debug('account queue is done')
# 	except CancelledError as err:
# 		debug(f'Cancelled')
# 	except Exception as err:
# 		error(f'{err}')
# 	return stats


async def cmd_fetch_files(
    db: Backend, args: Namespace, accountQ: Queue[BSAccount]
) -> EventCounter:
    debug("starting")
    raise NotImplementedError


###########################################
#
# cmd_fetch_wg()
#
###########################################

# DONE: --start -1 to start automatic
# DONE: --start 0 to start from the region's beginning


async def cmd_fetch_wg(
    db: Backend, args: Namespace, accountQ: IterableQueue[BSAccount]
) -> EventCounter:
    """Fetch account_ids from WG API /account/info"""
    debug("starting")
    stats: EventCounter = EventCounter("WG API")
    wg: WGApi = WGApi(
        app_id=args.wg_app_id,
        rate_limit=args.wg_rate_limit,
    )
    id_creators: List[Task] = list()
    api_workers: List[Task] = list()
    latest: Dict[Region, BSAccount] = dict()
    try:
        regions: set[Region] = {Region(r) for r in args.regions}
        start: int = args.wg_start_id
        if start > 0 and len(regions) > 1:
            raise ValueError("if --start >= 0, only one region can be chosen")
        if args.file is not None and len(regions) > 1:
            raise ValueError("if --file set, only one region can be chosen")
        force: bool = args.force
        null_responses: int = args.null_responses
        max_accounts: int = args.max_accounts

        WORKERS: int = max([int(args.wg_workers), 1])
        # ic(start, args.regions, regions)

        idQs: Dict[Region, IterableQueue[Sequence[int]]] = dict()

        if start == -1 and not force and args.file is None:
            message("finding latest accounts by region")
            latest = await db.accounts_latest(regions)
        for region in regions:
            try:
                idQs[region] = IterableQueue(maxsize=100)
                for _ in range(WORKERS):
                    api_workers.append(
                        create_task(
                            fetch_account_info_worker(
                                wg,
                                region,
                                idQs[region],
                                accountQ,
                                null_responses=null_responses,
                            )
                        )
                    )
                if args.file is None:
                    id_range: range = region.id_range
                    if start == -1 and not force:
                        id_range = range(latest[region].id + 1, id_range.stop)
                    elif start == 0:
                        id_range = region.id_range
                    else:
                        id_range = range(start, id_range.stop)
                    if max_accounts > 0:
                        id_range = range(
                            id_range.start,
                            min([id_range.start + max_accounts, id_range.stop]),
                        )

                    message(
                        f"fetching accounts for {region}: start={id_range.start}, stop={id_range.stop}"
                    )
                    id_creators.append(
                        create_task(
                            account_idQ_maker(
                                idQs[region], id_range.start, id_range.stop
                            )
                        )
                    )
                else:
                    ids: List[int] = list()
                    await idQs[region].add_producer()
                    async for account in BSAccount.import_file(args.file):
                        if account.region != region:
                            continue
                        ids.append(account.id)
                        if len(ids) == 100:
                            await idQs[region].put(ids)
                            ids = list()
                    if len(ids) > 0:
                        await idQs[region].put(ids)
                    await idQs[region].finish_producer()

            except Exception as err:
                error(f"could not create account queue for '{region}': {err}")
                raise Exception()

        with alive_bar(None, title="Getting accounts from WG API ") as bar:
            try:
                prev: int = 0
                done: int
                while not accountQ.is_filled:
                    done = accountQ.count
                    if done - prev > 0:
                        bar(done - prev)
                    prev = done
                    await sleep(1)

            except KeyboardInterrupt:
                message("cancelled")
                for idQ in idQs.values():
                    await idQ.finish_producer(all=True)
        await stats.gather_stats(id_creators)
        for region in idQs.keys():
            debug(f"waiting for idQ for {region} to complete")
            await idQs[region].join()

    except Exception as err:
        error(f"{err}")
    finally:
        await stats.gather_stats(api_workers)
        wg.print()
        await wg.close()
    return stats


async def account_idQ_maker(
    idQ: IterableQueue[Sequence[int]], start: int, end: int, batch: int = 100
) -> EventCounter:
    """Create account_id queue"""
    debug("starting")
    stats: EventCounter = EventCounter("account_ids")
    last: int = start
    await idQ.add_producer()
    try:
        for i in range(start, end, batch):
            await idQ.put(range(i, i + batch))
            last = i + batch
    except QueueDone:
        debug("queue done")
    except (KeyboardInterrupt, CancelledError):
        debug("Cancelled")
        raise
    except Exception as err:
        error(f"{err}")
    finally:
        stats.log("queued", last - start)
        debug("closing idQ")
        await idQ.finish_producer()
    return stats


async def fetch_account_info_worker(
    wg: WGApi,
    region: Region,
    idQ: IterableQueue[Sequence[int]],
    accountQ: IterableQueue[BSAccount],
    force: bool = False,
    null_responses: int = 100,
) -> EventCounter:
    """Fetch account info from WG API accounts/info"""
    debug("starting")
    stats: EventCounter = EventCounter(f"{region}")
    left: int = null_responses
    infos: List[AccountInfo] | None
    ids: Sequence[int]

    try:
        await accountQ.add_producer()
        async for ids in idQ:
            valid_stats: bool = False
            N_ids: int = len(ids)
            try:
                stats.log("account_ids", N_ids)
                if N_ids == 0 or N_ids > 100:
                    raise ValueError(f"Incorrect number of account_ids give {N_ids}")

                if (infos := await wg.get_account_info(ids, region)) is not None:
                    stats.log("stats found", len(infos))

                    for info in infos:
                        if (acc := BSAccount.transform(info)) is not None:
                            await accountQ.put(acc)
                            # stats.log('stats valid')
                            valid_stats = True
                        else:
                            stats.log("format errors")
                else:
                    stats.log("query errors")

            except ValueError:
                stats.log("errors", N_ids)
            except Exception as err:
                stats.log("errors")
                error(f"{err}")
            finally:
                if not force:
                    left = null_responses if valid_stats else left - 1
                    if left <= 0:  # too many NULL responses, stop
                        break

    except QueueDone:
        debug("account_id queue is done")
    except KeyboardInterrupt:
        debug("cancelled")
    except Exception as err:
        error(f"{err}")
    finally:
        debug(f"closing accountQ: {region}")
        await accountQ.finish_producer()
    debug(f"closing idQ: {region}")
    await idQ.finish_producer(all=True)
    # empty idQ
    async for _ in idQ:
        pass
    return stats


async def cmd_fetch_wi(
    db: Backend, args: Namespace, accountQ: IterableQueue[BSAccount] | None
) -> EventCounter:
    """Fetch account_ids from replays.wotinspector.com replays"""
    debug("starting")
    stats: EventCounter = EventCounter("WoTinspector")
    workersN: int = args.wi_workers
    workers: List[Task[EventCounter | BaseException]] = list()
    max_pages: int = args.wi_max_pages
    start_page: int = args.wi_start_page
    rate_limit: float = args.wi_rate_limit
    token: str = args.wi_auth_token  # temp fix...
    replay_idQ: IterableQueue[str] = IterableQueue(maxsize=10)

    wi: WoTinspector = WoTinspector(rate_limit=rate_limit, auth_token=token)

    pbar_pages = tqdm(total=max_pages, desc="Pages", position=2)
    pbar_replays = tqdm(total=max_pages * 20, desc="Replays", position=0)
    pbar_errors = tqdm(total=max_pages * 20, desc="Errors", position=1)

    if accountQ is not None:
        await accountQ.add_producer()
    try:
        step: int = 1
        if max_pages < 0:
            step = -1
        elif max_pages == 0:
            step = -1
            max_pages = -start_page

        pages: range = range(start_page, (start_page + max_pages), step)

        workers.append(
            create_task(
                fetch_wi_get_replay_ids(
                    db, wi, args, replay_idQ, pages, pbar=pbar_pages
                )
            )
        )
        for _ in range(workersN):
            workers.append(
                create_task(
                    fetch_wi_fetch_replays(
                        db,
                        wi,
                        replay_idQ,
                        accountQ,
                        pbar=pbar_replays,
                        pbar_errors=pbar_errors,
                    )
                )
            )

        await replay_idQ.join()
        await stats.gather_stats(workers, merge_child=True)

    except KeyboardInterrupt:
        debug("CTRL+C pressed, stopping...")
        pbar_pages.close()
        await replay_idQ.finish_producer(all=True)
        async for _ in replay_idQ:
            pass  # empty replay_idQ
    except Exception as err:
        error(f"{err}")
    finally:
        pbar_errors.close()
        pbar_replays.close()
        if accountQ is not None:
            await accountQ.finish_producer()
        await wi.close()
    return stats


async def fetch_wi_get_replay_ids(
    db: Backend,
    wi: WoTinspector,
    args: Namespace,
    replay_idQ: IterableQueue[str],
    pages: range,
    pbar: tqdm,
    # disable_bar: bool = False,
) -> EventCounter:
    """Spider replays.WoTinspector.com and feed found replay IDs into replayQ. Return stats"""
    debug("starting")
    stats: EventCounter = EventCounter("Crawler")
    max_old_replays: int = args.wi_max_old_replays
    force: bool = args.force
    old_replays: int = 0

    try:
        debug(f"Starting ({len(pages)} pages)")
        await replay_idQ.add_producer()

        for page in pages:
            pbar.update(1)
            try:
                if old_replays > max_old_replays:
                    raise CancelledError
                    #  break
                debug(f"spidering page {page}")
                replay_summaries: List[ReplaySummary] | None
                replay_summary: ReplaySummary
                if (replay_summaries := await wi.get_replay_list(page)) is not None:
                    for replay_summary in replay_summaries:
                        res: Replay | None = await db.replay_get(
                            replay_id=replay_summary.id
                        )
                        if res is not None:
                            debug(
                                f"Replay already in the {db.backend}: {replay_summary.id}"
                            )
                            stats.log("old replays found")
                            if not force:
                                old_replays += 1
                                continue
                            else:
                                stats.log("old replays to re-fetch")
                        else:
                            stats.log("new replays")
                        await replay_idQ.put(replay_summary.id)
                else:
                    debug(f"No replays found for page {page}")
            except KeyboardInterrupt:
                raise
            except Exception as err:
                error(f"{err}")
    except KeyboardInterrupt:
        debug("CTRL+C pressed, stopping...")
        await replay_idQ.finish_producer(all=True)
        async for _ in replay_idQ:
            pass  # empty replay_idQ
    except CancelledError:
        message(f"{old_replays} found. Stopping spidering for more")
    except Exception as err:
        error(f"{err}")
    finally:
        await replay_idQ.finish_producer()
        pbar.close()
    return stats


async def fetch_wi_fetch_replays(
    db: Backend,
    wi: WoTinspector,
    replay_idQ: IterableQueue[str],
    accountQ: Queue[BSAccount] | None,
    pbar: tqdm,
    pbar_errors: tqdm,
) -> EventCounter:
    debug("starting")
    stats: EventCounter = EventCounter("Fetch replays")
    try:
        async for replay_id in replay_idQ:
            try:
                replay: Replay | None
                if (replay := await wi.get_replay(replay_id)) is None:
                    verbose(f"Could not fetch replay id: {replay_id}")
                    stats.log("errors")
                    pbar_errors.update(1)
                    continue
                if accountQ is not None:
                    account_ids: List[int] = replay.allies + replay.enemies
                    stats.log("players found", len(account_ids))
                    for account_id in account_ids:
                        await accountQ.put(BSAccount(id=account_id))
                if await db.replay_insert(replay):
                    stats.log("replays added")
                    pbar.update(1)
                else:
                    stats.log("replays not added")
            except Exception as err:
                error(f"error fetching replay_id={replay_id}")
                debug(f"{err}")

    except Exception as err:
        error(f"{err}")
    return stats


async def cmd_import(db: Backend, args: Namespace) -> bool:
    """Import accounts from other backend"""
    try:
        stats: EventCounter = EventCounter("accounts import")
        accountQ: IterableQueue[BSAccount] = IterableQueue(maxsize=ACCOUNTS_Q_MAX)
        regions: set[Region] = {Region(r) for r in args.regions}
        import_db: Backend | None = None
        import_backend: str = args.import_backend
        force: bool = args.force
        if args.force:
            force = True

        write_worker: Task = create_task(
            db.accounts_insert_worker(accountQ=accountQ, force=force)
        )

        if (
            import_db := Backend.create_import_backend(
                driver=import_backend,
                args=args,
                import_type=BSTableType.Accounts,
                copy_from=db,
                config_file=args.import_config,
            )
        ) is None:
            raise ValueError(f"Could not init {import_backend} to import accounts from")

        message("Counting accounts to import ...")
        N: int = await db.accounts_count(
            regions=regions,
            # inactive=OptAccountsInactive.both,
            sample=args.sample,
        )

        with alive_bar(N, title="Importing accounts ", enrich_print=False) as bar:
            async for account in import_db.accounts_export(sample=args.sample):
                await accountQ.put(account)
                bar()
                stats.log("read")

        await accountQ.join()
        write_worker.cancel()
        worker_res: tuple[EventCounter | BaseException] = await gather(
            write_worker, return_exceptions=True
        )
        if type(worker_res[0]) is EventCounter:
            stats.merge_child(worker_res[0])
        elif type(worker_res[0]) is BaseException:
            error(f"account insert worker threw an exception: {worker_res[0]}")
        stats.print()
        return True
    except Exception as err:
        error(f"{err}")
    return False


async def cmd_export(db: Backend, args: Namespace) -> bool:
    try:
        debug("starting")

        ## not implemented...
        # query_args : Dict[str, str | int | float | bool ] = dict()
        stats: EventCounter = EventCounter("accounts export")
        # disabled: bool = args.disabled
        # inactive: OptAccountsInactive = OptAccountsInactive.default()
        regions: set[Region] = {Region(r) for r in args.regions}
        distributed: OptDistributed
        filename: str = args.filename
        force: bool = args.force
        export_stdout: bool = filename == "-"
        # sample: float = args.sample
        accountQ: IterableQueue[BSAccount] = IterableQueue(maxsize=ACCOUNTS_Q_MAX)
        # accountQs: Dict[str, IterableQueue[BSAccount]] = dict()
        regionQs: Dict[Region, IterableQueue[BSAccount]] = dict()

        account_worker: Task
        export_workers: List[Task] = list()
        monitors: List[Task] = list()
        bar: tqdm
        bars: list[tqdm] = list()
        position: int = 0

        accounts_args: Dict[str, Any] | None
        if (accounts_args := await accounts_parse_args(db, args)) is None:
            raise ValueError(f"could not parse args: {args}")

        # try:
        #     inactive = OptAccountsInactive(args.inactive)
        #     if (
        #         inactive == OptAccountsInactive.auto
        #     ):  # auto mode requires specication of stats type
        #         inactive = OptAccountsInactive.no
        # except ValueError:
        #     assert False, f"Incorrect value for argument 'inactive': {args.inactive}"

        total: int = await db.accounts_count(**accounts_args)
        await accountQ.add_producer()
        account_worker = create_task(db.accounts_get_worker(accountQ, **accounts_args))

        if "dist" in accounts_args:
            distributed = accounts_args["dist"]
            i: int = distributed.mod
            export_workers.append(
                create_task(
                    export(
                        iterable=accountQ,
                        format=args.format,
                        filename=f"{filename}.{i}",
                        force=force,
                        append=args.append,
                    )
                )
            )
            if not export_stdout:
                bar = tqdm(
                    total=total, desc=f"Exporting {i}/{distributed.div}", **tqdm_opts
                )
                monitors.append(
                    create_task(
                        tqdm_monitorQ(
                            accountQ,
                            bar=bar,
                            close=False,
                        )
                    )
                )
                bars.append(bar)
        elif args.by_region:
            for region in regions:
                regionQs[region] = IterableQueue(maxsize=ACCOUNTS_Q_MAX)
                if not export_stdout:
                    bar = tqdm(
                        total=total,
                        desc=f"Exporting {region}",
                        position=position,
                        **tqdm_opts,
                    )
                    position += 1
                    monitors.append(
                        create_task(
                            tqdm_monitorQ(
                                regionQs[region],
                                bar=bar,
                                close=False,
                            )
                        )
                    )
                    bars.append(bar)
                await regionQs[region].add_producer()
                export_workers.append(
                    create_task(
                        export(
                            iterable=regionQs[region],
                            format=args.format,
                            filename=f"{filename}.{region}",
                            force=force,
                            append=args.append,
                        )
                    )
                )
            # split by region
            export_workers.append(
                create_task(split_accountQ(inQ=accountQ, regionQs=regionQs))
            )
        else:
            # export all accounts to one file
            if filename != "-":
                filename += ".all"
            if not export_stdout:
                bar = tqdm(total=total, desc="Exporting accounts", **tqdm_opts)
                monitors.append(
                    create_task(
                        tqdm_monitorQ(
                            accountQ,
                            bar=bar,
                            close=False,
                        )
                    )
                )
                bars.append(bar)
            export_workers.append(
                create_task(
                    export(
                        iterable=accountQ,
                        format=args.format,
                        filename=filename,
                        force=force,
                        append=args.append,
                    )
                )
            )

        await wait([account_worker])

        if not export_stdout:
            await gather(*monitors)
            for bar in bars:
                bar.close()

        # for queue in accountQs.values():
        #     await queue.finish_producer()
        #     await queue.join()

        # await stats.gather_stats(account_workers)
        await stats.gather_stats(export_workers, cancel=False)

        if not export_stdout:
            stats.print()

    except Exception as err:
        error(f"{err}")
    return False


async def cmd_remove(db: Backend, args: Namespace) -> bool:
    try:
        debug("starting")
        raise NotImplementedError

    except Exception as err:
        error(f"{err}")
    return False


async def count_accounts(
    db: Backend, args: Namespace, stats_type: StatsTypes | None
) -> int:
    """Helper to count accounts based on CLI args"""
    debug("starting")
    accounts_N: int = 0
    try:
        regions: set[Region] = {Region(r) for r in args.regions}

        if len(args.accounts) > 0:
            return len(args.accounts)
        elif args.file is not None:
            message(f"Reading accounts from {args.file}")
            async with open(args.file, mode="r") as f:
                async for accounts_N, _ in enumerate(f):
                    pass
            accounts_N += 1
            if args.file.endswith(".csv"):
                accounts_N -= 1
        else:
            if args.sample > 1:
                accounts_N = int(args.sample)
            else:
                message("Counting accounts to fetch stats...")
                # inactive: OptAccountsInactive = OptAccountsInactive.default()
                # try:
                #     inactive = OptAccountsInactive(args.inactive)
                # except ValueError:
                #     assert False, (
                #         f"Incorrect value for argument 'inactive': {args.inactive}"
                #     )

                accounts_N = await db.accounts_count(
                    stats_type=stats_type,
                    regions=regions,
                    # inactive=inactive,
                    sample=args.sample,
                    cache_valid=args.cache_valid,
                )
    except Exception as err:
        error(f"{err}")
    return accounts_N


###########################################
#
# create_accountQ()
#
########################################


async def create_accountQ(
    db: Backend,
    args: Namespace,
    accountQ: IterableQueue[BSAccount],
    stats_type: StatsTypes | None = None,
) -> EventCounter:
    """Helper to make accountQ from arguments"""
    stats: EventCounter = EventCounter(f"{db.driver}: accounts")
    debug("starting")
    regions: set[Region] = set(args.regions)
    try:
        accounts: List[BSAccount] | None = None
        try:
            accounts = read_args_accounts(args.accounts)
        except Exception:
            debug("could not read --accounts")

        await accountQ.add_producer()

        if accounts is not None:
            accounts = await accounts_read_from_db(db, accounts)
            for account in accounts:
                if account.region in regions:
                    try:
                        await accountQ.put(account)
                        stats.log("read")
                    except QueueDone as err:
                        error(f"Could not add account ({account.id}) to queue: {err}")
                        stats.log("errors")

        elif args.file is not None:
            async for account in BSAccount.import_file(args.file):
                if account.region in regions:
                    if args.file.lower().endswith(".txt"):
                        if (a := await db.account_get(account.id)) is not None:
                            account = a
                    await accountQ.put(account)
                    debug(f"account put to queue: id={account.id}")
                    stats.log("read")

        else:
            accounts_args: Dict[str, Any] | None
            if (accounts_args := await accounts_parse_args(db, args)) is not None:
                async for account in db.accounts_get(
                    stats_type=stats_type, **accounts_args
                ):
                    try:
                        await accountQ.put(account)
                        stats.log("read")
                    except QueueDone as err:
                        error(f"Could not add account ({account.id}) to queue: {err}")
                        stats.log("errors")
            else:
                error(f"could not parse args: {args}")
    except CancelledError:
        debug("Cancelled")
    except KeyboardInterrupt:
        debug("Keyboard interrupt received")
    except Exception as err:
        error(f"{err}")
    finally:
        await accountQ.finish_producer()
    debug("finished")
    return stats


async def create_accountQ_batch(
    db: Backend,
    args: Namespace,
    region: Region,
    accountQ: IterableQueue[List[BSAccount]],
    stats_type: StatsTypes | None = None,
    batch: int = 100,
) -> EventCounter:
    """Helper to make accountQ from arguments"""
    stats: EventCounter = EventCounter(f"{db.driver}: accounts")
    debug(f"starting: {region}")
    try:
        accounts: List[BSAccount] | None = None
        try:
            accounts = read_args_accounts(args.accounts)
        except Exception:
            debug("could not read --accounts")

        await accountQ.add_producer()

        if accounts is not None:
            accounts = [account for account in accounts if account.region == region]
            accounts = await accounts_read_from_db(db, accounts)
            for account_batch in chunker(accounts, batch):
                try:
                    await accountQ.put(account_batch)
                    stats.log("read", len(account_batch))
                except Exception as err:
                    error(f"{err}")
                    stats.log("errors", len(accounts))

        elif args.file is not None:
            accounts = list()
            async for account in BSAccount.import_file(args.file):
                try:
                    if account.region == region:
                        if args.file.lower().endswith(".txt"):
                            if (a := await db.account_get(account.id)) is not None:
                                account = a
                        accounts.append(account)
                        if len(accounts) == batch:
                            await accountQ.put(accounts)
                            stats.log("read", batch)
                            accounts = list()
                except Exception as err:
                    error(f"Could not add account to the queue: {err}")
                    stats.log("errors")

            if len(accounts) > 0:
                await accountQ.put(accounts)
                stats.log("read", len(accounts))
        else:
            accounts_args: Dict[str, Any] | None
            if (accounts_args := await accounts_parse_args(db, args)) is not None:
                accounts_args["regions"] = {region}
                async for accounts in batch_gen(
                    db.accounts_get(stats_type=stats_type, **accounts_args), batch=batch
                ):
                    try:
                        await accountQ.put(accounts)
                        stats.log("read", len(accounts))
                    except QueueDone:
                        debug("accountQ is finished")
                        break
                    except Exception as err:
                        error(f"Could not add accounts to queue: {err}")
                        stats.log("errors")
            else:
                error(f"could not parse args: {args}")
    except KeyboardInterrupt:
        debug("Cancelled")
    except Exception as err:
        error(f"{err}")
    finally:
        await accountQ.finish_producer()

    return stats


async def create_accountQ_active(
    db: Backend,
    accountQ: Queue[BSAccount],
    release: BSBlitzRelease,
    regions: set[Region],
    randomize: bool = True,
) -> EventCounter:
    """Add accounts active during a release to accountQ"""
    debug("starting")
    stats: EventCounter = EventCounter("accounts")
    try:
        if randomize:
            workers: List[Task] = list()
            for r in regions:
                workers.append(
                    create_task(
                        create_accountQ_active(
                            db, accountQ, release, regions={r}, randomize=False
                        )
                    )
                )
            await stats.gather_stats(workers, merge_child=False, cancel=False)
        else:
            async for account_id in db.tank_stats_unique(
                "account_id", int, release=release, regions=regions
            ):
                try:
                    await accountQ.put(BSAccount(id=account_id))
                    stats.log("added")
                except Exception as err:
                    error(f"{err}")
                    stats.log("errors")
    except Exception as err:
        error(f"{err}")
    return stats


async def split_accountQ(
    inQ: IterableQueue[BSAccount], regionQs: Dict[Region, IterableQueue[BSAccount]]
) -> EventCounter:
    """split accountQ by region"""
    debug("starting")
    stats: EventCounter = EventCounter("accounts")
    try:
        for Q in regionQs.values():
            await Q.add_producer()

        async for account in inQ:
            try:
                if account.region is None:
                    raise ValueError(
                        f"account ({account.id}) does not have region defined"
                    )
                if account.region in regionQs.keys():
                    await regionQs[account.region].put(account)
                    stats.log(account.region)
                else:
                    stats.log(f"excluded region: {account.region}")
            except CancelledError:
                raise CancelledError from None
            except Exception as err:
                stats.log("errors")
                error(f"{err}")
            finally:
                stats.log("total")

    # except QueueDone:
    #     debug("Marking regionQs finished")
    except CancelledError:
        debug("Cancelled")
    except Exception as err:
        error(f"{err}")
    for Q in regionQs.values():
        await Q.finish_producer()
    return stats


async def split_accountQ_batch(
    inQ: IterableQueue[BSAccount],
    regionQs: Dict[Region, IterableQueue[List[BSAccount]]],
    batch: int = 100,
) -> EventCounter:
    """Make accountQ batches by region"""
    stats: EventCounter = EventCounter("batch maker")
    batches: Dict[Region, List[BSAccount]] = dict()
    region: Region
    try:
        for region, Q in regionQs.items():
            batches[region] = list()
            await Q.add_producer()

        async for account in inQ:
            try:
                region = account.region
                if region in regionQs.keys():
                    batches[region].append(account)
                    if len(batches[region]) == batch:
                        await regionQs[region].put(batches[region])
                        stats.log(f"{region} accounts", len(batches[region]))
                        batches[region] = list()
                else:
                    stats.log(f"excluded region: {region}")
            except CancelledError:
                raise CancelledError from None
            except Exception as err:
                stats.log("errors")
                error(f"{err}")
            finally:
                stats.log("total")
                # inQ.task_done()
    except QueueDone:
        debug("inQ done")
        for region in batches.keys():
            if len(batches[region]) > 0:
                await regionQs[region].put(batches[region])
                stats.log(f"{region} accounts", len(batches[region]))
    except CancelledError:
        debug("Cancelled")
    except Exception as err:
        error(f"{err}")
    for Q in regionQs.values():
        await Q.finish_producer()
    return stats


def read_args_accounts(accounts: Sequence[str]) -> List[BSAccount] | None:
    res: List[BSAccount] = list()
    for a in accounts:
        try:
            if (acc := BSAccount(id=a)) is not None:
                res.append(acc)
        except ValidationError as err:
            error(f"{err}")
    if len(res) == 0:
        return None
    return res


async def accounts_read_from_db(
    db: Backend, accounts: Sequence[BSAccount], db_only: bool = False
) -> List[BSAccount]:
    """Read DB versions of "skeleton" accounts from DB"""
    res: List[BSAccount] = list()
    for acc in accounts:
        if (account_db := await db.account_get(account_id=acc.id)) is not None:
            res.append(account_db)
        elif not db_only:
            res.append(acc)
    return res


async def accounts_parse_args(
    db: Backend,
    args: Namespace,
) -> Dict[str, Any] | None:
    """parse accounts args"""
    debug("starting")
    res: Dict[str, Any] = dict()

    try:
        regions: set[Region] = set()
        for region in args.regions:
            try:
                regions.add(Region(region))
            except Exception:
                error(f"could not read --regions={region}")
        res["regions"] = regions

        try:
            res["accounts"] = read_args_accounts(args.accounts)
        except Exception:
            debug("could not read --accounts")

        # try:
        #     res["inactive"] = OptAccountsInactive(args.inactive)
        # except Exception:
        #     debug("could not read --inactive")

        try:
            res["disabled"] = args.disabled
        except Exception:
            debug("could not read --disabled")

        try:
            res["sample"] = args.sample
        except Exception:
            debug("could not read --sample")

        try:
            res["cache_valid"] = args.cache_valid
        except Exception:
            debug("could not read --cache-valid")

        try:
            if (dist := OptDistributed.parse(args.distributed)) is not None:
                res["dist"] = dist
        except Exception:
            debug("could not read --distributed")

        days: int
        today: datetime = datetime.utcnow()
        start: datetime

        if args.inactive_since is not None:
            try:
                if (rel := await db.release_get(args.inactive_since)) is not None:
                    if (prev := await db.release_get_previous(rel)) is not None:
                        res["inactive_since"] = prev.cut_off
                else:
                    days = int(args.inactive_since)
                    start = today - timedelta(days=days)
                    res["inactive_since"] = int(start.timestamp())
            except Exception as err:
                debug(f"could not read --inactive-since: {err}")

        if args.active_since is not None:
            try:
                if (rel := await db.release_get(args.active_since)) is not None:
                    # debug(f'active_since={rel}')
                    if (prev := await db.release_get_previous(rel)) is not None:
                        # debug(f'active_since: prev={prev}')
                        res["active_since"] = prev.cut_off
                else:
                    days = int(args.active_since)
                    start = today - timedelta(days=days)
                    res["active_since"] = int(start.timestamp())
            except Exception as err:
                debug(f"could not read --active-since: {err}")

        return res
    except Exception as err:
        error(f"{err}")
    return None
