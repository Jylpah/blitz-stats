from argparse import ArgumentParser, Namespace
from configparser import ConfigParser
from typing import Optional, Any, List, Dict
import logging
from asyncio import create_task, gather, wait, Queue, Task, sleep
from sortedcollections import NearestDict  # type: ignore
from math import ceil
from datetime import date, datetime

from eventcounter import EventCounter
from queutils import IterableQueue
from pydantic_exportables import export
from pyutils.utils import is_alphanum
from blitzmodels import Release  # noqa

from .backend import Backend, BSTableType
from .models import BSBlitzRelease

MAX_EPOCH: int = (
    2**36
)  ##  Sunday, August 20, 4147 7:32:16, I doubt Python3 is supported anymore then

logger = logging.getLogger(__name__)
error = logger.error
message = logger.warning
verbose = logger.info
debug = logger.debug

###########################################
#
# add_args functions
#
###########################################


def add_args(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
    try:
        debug("starting")
        releases_parsers = parser.add_subparsers(
            dest="releases_cmd",
            title="releases commands",
            description="valid commands",
            help="releases help",
            metavar="add | edit | remove | export | list",
        )
        releases_parsers.required = True
        add_parser = releases_parsers.add_parser("add", help="releases add help")
        if not add_args_add(add_parser, config=config):
            raise Exception("Failed to define argument parser for: releases add")

        edit_parser = releases_parsers.add_parser("edit", help="releases edit help")
        if not add_args_edit(edit_parser, config=config):
            raise Exception("Failed to define argument parser for: releases edit")

        remove_parser = releases_parsers.add_parser(
            "remove", help="releases remove help"
        )
        if not add_args_remove(remove_parser, config=config):
            raise Exception("Failed to define argument parser for: releases remove")

        import_parser = releases_parsers.add_parser(
            "import", help="releases import help"
        )
        if not add_args_import(import_parser, config=config):
            raise Exception("Failed to define argument parser for: releases import")

        export_parser = releases_parsers.add_parser(
            "export", aliases=["list"], help="releases export help"
        )
        if not add_args_export(export_parser, config=config):
            raise Exception("Failed to define argument parser for: releases export")

        return True
    except Exception as err:
        error(f"add_args(): {err}")
    return False


def add_args_add(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
    try:
        debug("starting")
        parser.add_argument(
            "release", type=str, default=None, metavar="RELEASE", help="RELEASE to add"
        )
        parser.add_argument(
            "--launch-date",
            "--launch",
            type=str,
            default=None,
            metavar="DATE",
            help="release launch date",
        )
        parser.add_argument(
            "--cut-off",
            type=str,
            default=0,
            metavar="EPOCH",
            help="release cut-off time",
        )
        parser.add_argument(
            "--round-cut-off",
            "--round",
            type=int,
            default=0,
            metavar="MINUTES",
            help="round cut-off time to the next full MINUTES",
        )
        return True
    except Exception as err:
        error(f"add_args_add() : {err}")
    return False


def add_args_edit(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    try:
        debug("starting")
        parser.add_argument(
            "release", type=str, metavar="RELEASE", help="RELEASE to edit"
        )
        parser.add_argument(
            "--cut-off",
            type=int,
            default=-1,
            metavar="EPOCH",
            help="new release cut-off time",
        )
        parser.add_argument(
            "--launch-date",
            "--launch",
            type=str,
            default=None,
            metavar="DATE",
            help="new release launch date",
        )
        parser.add_argument(
            "--round-cut-off",
            "--round",
            type=int,
            default=10,
            metavar="MINUTES",
            help="round cut-off time to the next full MINUTES",
        )
        return True
    except Exception as err:
        error(f"add_args_edit() : {err}")
    return False


def add_args_remove(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    try:
        debug("starting")
        parser.add_argument(
            "release", type=str, metavar="RELEASE", help="RELEASE to remove"
        )
        parser.add_argument(
            "--force",
            action="store_true",
            default=False,
            help="do not wait before removing releases ",
        )
        return True
    except Exception as err:
        error(f"add_args_remove() : {err}")
    return False


def add_args_import(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    try:
        debug("starting")
        import_parsers = parser.add_subparsers(
            dest="import_backend",
            title="releases import backend",
            description="valid import backends",
            metavar=", ".join(Backend.list_available()),
        )
        import_parsers.required = True

        for backend in Backend.get_registered():
            import_parser = import_parsers.add_parser(
                backend.driver, help=f"releases import {backend.driver} help"
            )
            if not backend.add_args_import(import_parser, config=config):
                raise Exception(
                    f"Failed to define argument parser for: releases import {backend.driver}"
                )

        parser.add_argument(
            "--import-model",
            metavar="IMPORT-TYPE",
            type=str,
            required=True,
            choices=["BSBlitzRelease"],
            help="data format to import. Default is blitz-stats native format.",
        )
        parser.add_argument(
            "--sample", type=int, default=0, metavar="SAMPLE", help="sample size"
        )
        parser.add_argument(
            "--force",
            action="store_true",
            default=False,
            help="overwrite existing file(s) when exporting",
        )
        parser.add_argument(
            "--releases",
            type=str,
            metavar="RELEASE_MATCH",
            default=None,
            nargs="?",
            help="search by RELEASE_MATCH. By default list all.",
        )
        parser.add_argument(
            "--since",
            type=str,
            metavar="LAUNCH_DATE",
            default=None,
            nargs="?",
            help="import release launched after LAUNCH_DATE. By default, imports all releases.",
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
        parser.add_argument(
            "release_match",
            type=str,
            metavar="RELEASE_MATCH",
            default=None,
            nargs="?",
            help="Search by RELEASE_MATCH. By default list all.",
        )
        parser.add_argument(
            "--since",
            type=str,
            metavar="LAUNCH_DATE",
            default=None,
            nargs="?",
            help="Import release launched after LAUNCH_DATE. By default, imports all releases.",
        )
        parser.add_argument(
            "--format",
            type=str,
            choices=["json", "txt", "csv"],
            metavar="FORMAT",
            default="txt",
            help="releases list format",
        )
        parser.add_argument(
            "--file",
            metavar="FILE",
            type=str,
            default="-",
            help="File to export releases to. Use '-' for STDIN",
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


def read_args_releases(strs: List[str] | None) -> List[BSBlitzRelease] | None:
    """Read releases from arguments"""
    debug("starting")
    try:
        releases: List[BSBlitzRelease] = list()
        if strs is not None:
            for r in strs:
                try:
                    releases.append(BSBlitzRelease(release=r))
                except Exception as err:
                    error(f"{err}")
            return releases
    except Exception as err:
        error(f"{err}")
    return None


###########################################
#
# cmd functions
#
###########################################


async def cmd(db: Backend, args: Namespace) -> bool:
    try:
        debug("starting")
        if args.releases_cmd == "add":
            debug("releases add")
            return await cmd_add(db, args)
        elif args.releases_cmd == "edit":
            debug("releases edit")
            return await cmd_edit(db, args)
        elif args.releases_cmd == "remove":
            debug("releases remove")
            return await cmd_remove(db, args)
        elif args.releases_cmd == "import":
            debug("releases import")
            return await cmd_import(db, args)
        elif args.releases_cmd == "export" or args.releases_cmd == "list":
            debug("releases export")
            return await cmd_export(db, args)
        else:
            error(f"Unknown or missing subcommand: {args.releases_cmd}")

    except Exception as err:
        error(f"{err}")
    return False


async def cmd_add(db: Backend, args: Namespace) -> bool:
    debug("starting")
    release: BSBlitzRelease | None = None
    try:
        release = BSBlitzRelease(
            release=args.release,
            launch_date=args.launch_date,
            cut_off=args.cut_off,
        )
        release.cut_off = round_epoch(release.cut_off, args.round_cut_off * 60)
        return await db.release_insert(release=release)
    except Exception as err:
        debug("%s: %s", type(err), err)
        error("No valid release given as argument")

    # if release is None:
    #     release = await db.release_get_latest()
    #     if release is None:
    #         raise ValueError(
    #             "Could not find previous release and no new release set"
    #         )
    #     return await db.release_insert(release.next())
    # else:
    #     return await db.release_insert(release=release)

    return False


async def cmd_edit(db: Backend, args: Namespace) -> bool:
    try:
        debug("starting")
        release: BSBlitzRelease | None
        cut_off: int = int(args.cut_off)
        fields: List[str] = list()
        if (release := await db.release_get(args.release)) is not None:
            if cut_off >= 0:
                fields.append("cut_off")
                if cut_off == 0:
                    release.cut_off = MAX_EPOCH
                else:
                    release.cut_off = round_epoch(cut_off, args.round_cut_off * 60)
            if args.launch_date is not None:
                fields.append("launch_date")
                release.launch_date = datetime.fromisoformat(args.launch_date)
            if len(fields) > 0:
                update: Dict[str, Any] = release.dict(
                    exclude_unset=True, exclude_none=True
                )
                del update["release"]
                return await db.release_update(release, update=update, fields=fields)
    except Exception as err:
        error(f"{err}")
    return False


async def cmd_remove(db: Backend, args: Namespace) -> bool:
    try:
        debug("starting")
        message(f"Removing release {args.release} in 3 seconds. Press CTRL+C to cancel")
        await sleep(3)
        release = BSBlitzRelease(release=args.release)
        if await db.release_delete(release=release.release):
            message(f"release {release.release} removed")
            return True
        else:
            error(f"Could not remove release {release.release}")
    except Exception as err:
        error(f"{err}")
    return False


async def cmd_export(db: Backend, args: Namespace) -> bool:
    try:
        debug("starting")
        releaseQ: IterableQueue[BSBlitzRelease] = IterableQueue(maxsize=100)
        since: int = 0
        export_worker: Task
        export_worker = create_task(
            export(
                iterable=releaseQ,
                format=args.format,
                filename=args.file,
                force=args.force,
            )
        )

        if args.since is not None:
            since = int(
                datetime.combine(
                    date.fromisoformat(args.since), datetime.min.time()
                ).timestamp()
            )
        await releaseQ.add_producer()
        async for release in db.releases_get(
            release_match=args.release_match, since=since
        ):
            debug(f"adding release {release.release} to the export queue")
            await releaseQ.put(release)
        await releaseQ.finish()
        await releaseQ.join()
        await wait([export_worker])

        return True
    except Exception as err:
        error(f"{err}")
    return False


async def cmd_import(db: Backend, args: Namespace) -> bool:
    """Import releases from other backend"""
    try:
        assert is_alphanum(args.import_model), (
            f"invalid --import-model: {args.import_model}"
        )

        stats: EventCounter = EventCounter("releases import")
        releaseQ: Queue[BSBlitzRelease] = Queue(100)
        import_db: Backend | None = None
        import_backend: str = args.import_backend

        write_worker: Task = create_task(
            db.releases_insert_worker(releaseQ=releaseQ, force=args.force)
        )

        if (
            import_db := Backend.create_import_backend(
                driver=import_backend,
                args=args,
                import_type=BSTableType.Releases,
                copy_from=db,
                config_file=args.import_config,
            )
        ) is None:
            raise ValueError(f"Could not init {import_backend} to import releases from")

        async for release in import_db.releases_export(sample=args.sample):
            await releaseQ.put(release)
            stats.log("read")

        await releaseQ.join()
        write_worker.cancel()
        worker_res: tuple[EventCounter | BaseException] = await gather(
            write_worker, return_exceptions=True
        )
        if type(worker_res[0]) is EventCounter:
            stats.merge_child(worker_res[0])
        elif type(worker_res[0]) is BaseException:
            error(f"releases insert worker threw an exception: {worker_res[0]}")
        stats.print()
        return True
    except Exception as err:
        error(f"{err}")
    return False


async def get_releases(db: Backend, releases: List[str]) -> List[BSBlitzRelease]:
    debug(f"starting, releases={releases}")
    res: List[BSBlitzRelease] = list()
    try:
        for r in releases:
            if (rel := await db.release_get(r)) is not None:
                res.append(rel)
        res = [*set(res)]
    except Exception as err:
        error(f"{err}")
    return res


# ## REFACTOR: Use sortedcollections.NearestDict instead. Should be faster.

# async def release_mapper_OLD(db: Backend) -> BucketMapper[BSBlitzRelease]:
# 	"""Fetch all releases and create a release BucketMapper object"""
# 	releases : BucketMapper[BSBlitzRelease] = BucketMapper[BSBlitzRelease](attr='cut_off')
# 	async for r in db.releases_get():
# 		releases.insert(r)
# 	return releases


async def release_mapper(db: Backend) -> NearestDict[int, BSBlitzRelease]:
    """Fetch all releases and create a NearestDict() with cut-off as key"""
    releases: NearestDict[int, BSBlitzRelease] = NearestDict(
        rounding=NearestDict.NEAREST_NEXT
    )
    async for r in db.releases_get():
        if r.cut_off in releases.keys():
            message(f"Cannot store releases with duplicate cut-off times: {r}")
            continue
        releases[r.cut_off] = r
    return releases


def round_epoch(epoch: int, round_to: int = 0) -> int:
    """Round epoch time to the next even 'round_to'.
    Adds round_to/2 to the epoch first to ensure there is enough gap"""
    assert isinstance(epoch, int), "epoch has to be an int"
    if round_to > 0:
        epoch = epoch + int(round_to / 2)
        return ceil(epoch / round_to) * round_to
    return epoch
