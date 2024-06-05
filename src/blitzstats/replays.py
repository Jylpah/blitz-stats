from argparse import ArgumentParser, Namespace
from configparser import ConfigParser
from typing import Optional, Iterable, Any, List, Dict
import logging
import queue
from asyncio import create_task, gather, run, Queue, CancelledError, Task
from os import getpid
from alive_progress import alive_bar  # type: ignore
from multiprocessing import Manager, cpu_count
from multiprocessing.pool import Pool, AsyncResult
from pydantic import BaseModel

from pyutils import EventCounter, AsyncQueue
from pyutils.utils import is_alphanum
from pydantic_exportables import JSONExportable

# from blitzmodels.replay import ReplayJSON, ReplayData
# from blitzmodels.wotinspector.wi_apiv2 import Replay
from .models import BSReplay
from .backend import Backend, BSTableType, get_sub_type
from .accounts import add_args_fetch_wi as add_args_accounts_fetch_wi
from .accounts import cmd_fetch_wi as cmd_accounts_fetch_wi

logger = logging.getLogger()
error = logger.error
message = logger.warning
verbose = logger.info
debug = logger.debug

WI_MAX_PAGES: int = 100
WI_MAX_OLD_REPLAYS: int = 30
WI_RATE_LIMIT: Optional[float] = 20 / 3600
WI_AUTH_TOKEN: Optional[str] = None
REPLAY_Q_MAX: int = 100
REPLAYS_BATCH: int = 50

# Globals

# FB 	: ForkedBackend
db: Backend
readQ: AsyncQueue[List[Any] | None]
writeQ: queue.Queue
in_model: type[BaseModel]
mp_options: Dict[str, Any] = dict()


###########################################
#
# add_args_accouts functions
#
###########################################


def add_args(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
    try:
        replays_parsers = parser.add_subparsers(
            dest="replays_cmd",
            title="replays commands",
            description="valid commands",
            help="replays help",
            metavar="add | fetch | export | import",
        )
        replays_parsers.required = True

        add_parser = replays_parsers.add_parser("add", help="replays add help")
        if not add_args_add(add_parser, config=config):
            raise Exception("Failed to define argument parser for: replays add")

        fetch_parser = replays_parsers.add_parser("fetch", help="replays fetch help")
        if not add_args_fetch(fetch_parser, config=config):
            raise Exception("Failed to define argument parser for: replays fetch")

        export_parser = replays_parsers.add_parser("export", help="replays export help")
        if not add_args_export(export_parser, config=config):
            raise Exception("Failed to define argument parser for: replays export")

        import_parser = replays_parsers.add_parser("import", help="replays import help")
        if not add_args_import(import_parser, config=config):
            raise Exception("Failed to define argument parser for: replays import")

        return True
    except Exception as err:
        error(f"add_args(): {err}")
    return False


def add_args_add(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
    parser.add_argument(
        "files",
        nargs="+",
        type=str,
        metavar="REPLAY [REPLAY1 ...]",
        default=False,
        help="Replay files to add to DB",
    )
    return True


def add_args_fetch(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Ignore existing accounts exporting",
    )
    return add_args_accounts_fetch_wi(parser=parser, config=config)


def add_args_export(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    try:
        parser.add_argument(
            "--file",
            action="store_true",
            default=False,
            dest="replay_export_file",
            help="Export replay(s) to file(s)",
        )
        replays_export_parsers = parser.add_subparsers(
            dest="replays_export_query_type",
            title="replays export query-type",
            description="valid query-types",
            metavar="id",
        )
        replays_export_parsers.required = True

        replays_export_id_parser = replays_export_parsers.add_parser(
            "id", help="replays export id help"
        )
        if not add_args_export_id(replays_export_id_parser, config=config):
            raise Exception("Failed to define argument parser for: replays export id")

        ## Not implemented yet
        # replays_export_find_parser = replays_export_parsers.add_parser('find', help='replays export find help')
        # if not add_args_export_find(replays_export_find_parser, config=config):
        # 	raise Exception("Failed to define argument parser for: replays export find")

        return True
    except Exception as err:
        error(f"add_args_export() : {err}")
    return False


def add_args_export_id(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    """Add argparse arguments for replays export id -subcommand"""
    try:
        parser.add_argument(
            "replay_export_id",
            type=str,
            metavar="REPLAY-ID",
            help="Replay ID to export",
        )
        return True
    except Exception as err:
        error(f"add_args_export_id() : {err}")
    return False


def add_args_export_find(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    """Add argparse arguments for replays export find -subcommand"""
    ## NOT IMPLEMENTED
    try:
        return True
    except Exception as err:
        error(f"add_args_export_find() : {err}")
    return False


def add_args_import(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    try:
        import_parsers = parser.add_subparsers(
            dest="import_backend",
            title="replays import backend",
            description="valid import backends",
            metavar=" | ".join(Backend.list_available()),
        )
        import_parsers.required = True

        for backend in Backend.get_registered():
            import_parser = import_parsers.add_parser(
                backend.driver, help=f"replays import {backend.driver} help"
            )
            if not backend.add_args_import(import_parser, config=config):
                raise Exception(
                    f"Failed to define argument parser for: replays import {backend.driver}"
                )
        parser.add_argument(
            "--workers",
            type=int,
            default=0,
            help="Set number of worker processes (default=0 i.e. auto)",
        )
        parser.add_argument(
            "--import-model",
            metavar="IMPORT-TYPE",
            type=str,
            required=True,
            choices=["BSReplay"],
            help="Data format to import. Default is blitz-stats native format.",
        )

        parser.add_argument("--sample", type=float, default=0, help="Sample size")
        parser.add_argument(
            "--force",
            action="store_true",
            default=False,
            help="Overwrite existing file(s) when exporting",
        )

        return True
    except Exception as err:
        error(f"add_args_import() : {err}")
    return False


###########################################
#
# cmd_accouts functions
#
###########################################


async def cmd(db: Backend, args: Namespace) -> bool:
    try:
        debug("replays")
        if args.replays_cmd == "fetch":
            return await cmd_fetch(db, args)

        elif args.replays_cmd == "export":
            if args.replays_export_query_type == "id":
                return await cmd_export_id(db, args)
            elif args.replays_export_query_type == "find":
                return await cmd_export_find(db, args)
            else:
                error("replays: unknown or missing subcommand")

        elif args.replays_cmd == "import":
            return await cmd_importMP(db, args)

    except Exception as err:
        error(f"{err}")
    return False


# BUG: something is wrong: "could not retrieve valid replay list" error. Maybe related to rate-limiter?
async def cmd_fetch(db: Backend, args: Namespace) -> bool:
    try:
        debug("starting")
        stats = EventCounter("replays fetch")
        stats.merge(await cmd_accounts_fetch_wi(db, args, accountQ=None))
        stats.print()
        return True
    except Exception as err:
        error(f"{err}")
    return False


async def cmd_export_id(db: Backend, args: Namespace) -> bool:
    try:
        debug("starting")
        id: str = args.replay_export_id
        replay: BSReplay | None = await db.replay_get(id)
        if replay is None:
            error("Could not find replay id: {id}")
            return False
        if args.replay_export_file:
            return await cmd_export_files(args, [replay])
        else:
            print(replay.json_src(indent=4))
        return True
    except Exception as err:
        error(f"{err}")
    return False


async def cmd_export_files(args: Namespace, replays: Iterable[BSReplay]) -> bool:
    raise NotImplementedError
    return False


async def cmd_export_find(db: Backend, args: Namespace) -> bool:
    raise NotImplementedError
    return False


async def cmd_import(db: Backend, args: Namespace) -> bool:
    """Import replays from other backend"""
    try:
        assert is_alphanum(
            args.import_model
        ), f"invalid --import-model: {args.import_model}"

        stats: EventCounter = EventCounter("replays import")
        replayQ: Queue[JSONExportable] = Queue(REPLAY_Q_MAX)
        sample: float = args.sample
        import_db: Backend | None = None
        import_backend: str = args.import_backend
        # import_model: type[BaseModel] | None = None

        if (_ := get_sub_type(args.import_model, BaseModel)) is None:
            raise ValueError("--import-model has to be subclass of BaseModel")

        importer: Task = create_task(
            db.replays_insert_worker(replayQ=replayQ, force=args.force)
        )

        if (
            import_db := Backend.create_import_backend(
                driver=import_backend,
                args=args,
                import_type=BSTableType.Replays,
                copy_from=db,
                config_file=args.import_config,
            )
        ) is None:
            raise ValueError(f"Could not init {import_backend} to import releases from")

        N: int = await import_db.replays_count(sample=sample)
        with alive_bar(N, title="Importing replays ", enrich_print=False) as bar:
            async for replay in import_db.replays_export(sample=sample):
                await replayQ.put(replay)
                bar()
                stats.log("read")

        await replayQ.join()
        importer.cancel()
        worker_res: tuple[EventCounter | BaseException] = await gather(
            importer, return_exceptions=True
        )
        if type(worker_res[0]) is EventCounter:
            stats.merge_child(worker_res[0])
        elif type(worker_res[0]) is BaseException:
            error(f"replays insert worker threw an exception: {worker_res[0]}")
        stats.print()
        return True
    except Exception as err:
        error(f"{err}")
    return False


async def cmd_importMP(db: Backend, args: Namespace) -> bool:
    """Import replays from other backend"""
    try:
        debug("starting")
        stats: EventCounter = EventCounter("replays import")
        import_db: Backend | None = None
        import_backend: str = args.import_backend
        import_model: type[BaseModel] | None = None
        WORKERS: int = args.workers

        if WORKERS == 0:
            WORKERS = max([cpu_count() - 1, 1])

        if (import_model := get_sub_type(args.import_model, BaseModel)) is None:
            raise ValueError(
                "--import-model not defined or not is a subclass of BaseModel"
            )

        if (
            import_db := Backend.create_import_backend(
                driver=import_backend,
                args=args,
                import_type=BSTableType.Replays,
                copy_from=db,
                config_file=args.import_config,
            )
        ) is None:
            raise ValueError(f"Could not init {import_backend} to import replays from")

        with Manager() as manager:
            readQ: queue.Queue[List[Any] | None] = manager.Queue(REPLAY_Q_MAX)
            options: Dict[str, Any] = dict()
            options["force"] = args.force

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

                message("Counting replays to import ...")
                N: int = await import_db.replays_count(sample=args.sample)

                with alive_bar(
                    N, title="Importing replays ", enrich_print=False, refresh_secs=1
                ) as bar:
                    async for objs in import_db.objs_export(
                        table_type=BSTableType.Replays,
                        sample=args.sample,
                        batch=REPLAYS_BATCH,
                    ):
                        read: int = len(objs)
                        readQ.put(objs)
                        stats.log(f"{db.driver}:stats read", read)
                        bar(read)

                debug(
                    f"Finished exporting {import_model} from {import_db.table_uri(BSTableType.Replays)}"
                )
                for _ in range(WORKERS):
                    readQ.put(None)  # add sentinel

                for res in results.get():
                    stats.merge_child(res)
                pool.join()

        message(stats.print(do_print=False, clean=True))
        return True
    except Exception as err:
        error(f"{err}")
    return False


def import_mp_init(
    backend_config: Dict[str, Any],
    inputQ: queue.Queue[List[Any] | None],
    import_model: type[BaseModel],
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
    """Forkable replay import worker"""
    debug(f"starting import worker #{id}")
    return run(import_mp_worker(id), debug=False)


async def import_mp_worker(id: int = 0) -> EventCounter:
    """Forkable replay import worker"""
    debug(f"#{id}: starting")
    stats: EventCounter = EventCounter("importer")
    workers: List[Task] = list()
    try:
        global db, readQ, in_model, mp_options
        THREADS: int = 4
        import_model: type[BaseModel] = in_model
        writeQ: Queue[JSONExportable] = Queue(500)
        force: bool = mp_options["force"]

        for _ in range(THREADS):
            workers.append(
                create_task(db.replays_insert_worker(replayQ=writeQ, force=force))
            )
        errors: int = 0

        while (objs := await readQ.get()) is not None:
            debug(f"#{id}: read {len(objs)} objects")
            try:
                read: int = len(objs)
                debug(f"read {read} documents")
                stats.log("stats read", read)
                replays = BSReplay.from_objs(objs=objs, in_type=import_model)
                errors = len(objs) - len(replays)
                stats.log("replays read", len(replays))
                stats.log("conversion errors", errors)
                for replay in replays:
                    await writeQ.put(replay)
            except Exception as err:
                error(f"{err}")
            finally:
                readQ.task_done()
        debug(f"#{id}: finished reading objects")
        readQ.task_done()
        await writeQ.join()  # add sentinel for other workers
        await stats.gather_stats(workers)
    except CancelledError:
        pass
    except Exception as err:
        error(f"{err}")
    return stats
