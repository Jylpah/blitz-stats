import logging
from aiofiles import open
from argparse import ArgumentParser, Namespace, SUPPRESS
from configparser import ConfigParser
from typing import Optional, cast
from asyncio import create_task, Queue, CancelledError, Task
import json

# from yappi import profile 					# type: ignore

from pyutils import export, IterableQueue, EventCounter
from blitzutils import (
    EnumNation,
    EnumVehicleTier,
    EnumVehicleTypeStr,
    EnumVehicleTypeInt,
)
from blitzutils import (
    Tank,
    Region,
    add_args_wg,
    WGApi,
    WGApiWoTBlitzTankopedia,
    WoTBlitzTankString,
)

from .backend import (
    Backend,
    OptAccountsInactive,
    BSTableType,
    ACCOUNTS_Q_MAX,
    MIN_UPDATE_INTERVAL,
    get_sub_type,
)
from .models import BSAccount, BSBlitzRelease, StatsTypes, BSTank

logger = logging.getLogger()
error = logger.error
message = logger.warning
verbose = logger.info
debug = logger.debug

TANKOPEDIA_FILE: str = "tanks.json"
EXPORT_SUPPORTED_FORMATS: list[str] = ["json", "txt", "csv"]  # , 'csv'

########################################################
#
# add_args_ functions
#
########################################################


def add_args(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
    try:
        debug("starting")
        tankopedia_parsers = parser.add_subparsers(
            dest="tankopedia_cmd",
            title="tankopedia commands",
            description="valid commands",
            metavar="add | update | edit | import | export",
        )
        tankopedia_parsers.required = True

        add_parser = tankopedia_parsers.add_parser("add", help="tankopedia add help")
        if not add_args_add(add_parser, config=config):
            raise Exception("Failed to define argument parser for: tankopedia add")

        update_parser = tankopedia_parsers.add_parser(
            "update", help="tankopedia update help"
        )
        if not add_args_update(update_parser, config=config):
            raise Exception("Failed to define argument parser for: tankopedia update")

        edit_parser = tankopedia_parsers.add_parser("edit", help="tankopedia edit help")
        if not add_args_edit(edit_parser, config=config):
            raise Exception("Failed to define argument parser for: tankopedia edit")

        import_parser = tankopedia_parsers.add_parser(
            "import", help="tankopedia import help"
        )
        if not add_args_import(import_parser, config=config):
            raise Exception("Failed to define argument parser for: tankopedia import")

        export_parser = tankopedia_parsers.add_parser(
            "export", help="tankopedia export help"
        )
        if not add_args_export(export_parser, config=config):
            raise Exception("Failed to define argument parser for: tankopedia export")
        debug("Finished")
        return True
    except Exception as err:
        error(f"{err}")
    return False


def add_args_add(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
    try:
        debug("starting")
        parser.add_argument("tank_id", type=int, help="tank_id > 0")
        parser.add_argument(
            "--name",
            type=str,
            dest="tank_name",
            metavar="NAME",
            default=None,
            help="tank name",
        )
        parser.add_argument(
            "--nation",
            type=str,
            dest="tank_nation",
            metavar="NATION",
            default=None,
            help="tank nation: " + ", ".join([n.name for n in EnumNation]),
        )
        parser.add_argument(
            "--tier",
            type=str,
            dest="tank_tier",
            metavar="TIER",
            default=None,
            help="tank tier I-X or 1-10",
        )
        parser.add_argument(
            "--type",
            type=str,
            dest="tank_type",
            metavar="TYPE",
            default=None,
            help="tank type: " + ", ".join([n.name for n in EnumVehicleTypeInt]),
        )
        parser.add_argument(
            "--premium",
            action="store_true",
            default=False,
            dest="is_premium",
            help="premium tank",
        )

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
            dest="tankopedia_update_source",
            title="tankopedia update commands",
            description="valid commands",
            metavar="file | wg",
        )
        update_parsers.required = True
        update_file_parser = update_parsers.add_parser(
            "file", help="tankopedia update file help"
        )
        if not add_args_update_file(update_file_parser, config=config):
            raise Exception(
                "Failed to define argument parser for: tankopedia update file"
            )

        update_wg_parser = update_parsers.add_parser(
            "wg", help="tankopedia update wg help"
        )
        if not add_args_update_wg(update_wg_parser, config=config):
            raise Exception(
                "Failed to define argument parser for: tankopedia update help"
            )

        parser.add_argument(
            "--add-only",
            action="store_true",
            default=False,
            help="Only add new tanks, do not update existing ones",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Read and compare update, but do not write to the backend",
        )

        return True
    except Exception as err:
        error(f"{err}")
    return False


def add_args_update_file(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    try:
        tankopedia_file: str = TANKOPEDIA_FILE
        if config is not None:
            tankopedia_file = config.get(
                "METADATA", "tankopedia_json", fallback=TANKOPEDIA_FILE
            )
        debug("starting")
        parser.add_argument(
            "file",
            type=str,
            default=tankopedia_file,
            metavar="FILE",
            help=f"read tankopedia update from FILE (default: {tankopedia_file})",
        )

        return True
    except Exception as err:
        error(f"{err}")
    return False


def add_args_update_wg(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    debug("starting")
    return add_args_wg(parser, config)


def add_args_edit(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    try:
        debug("starting")
        parser.add_argument("tank_id", type=int, help="BSTank to edit: tank_id > 0")
        parser.add_argument(
            "--name",
            type=str,
            dest="tank_name",
            metavar="NAME",
            default=None,
            help="edit tank's name",
        )
        parser.add_argument(
            "--nation",
            type=str,
            dest="tank_nation",
            metavar="NATION",
            default=None,
            help="edit tank's nation: " + ", ".join([n.name for n in EnumNation]),
        )
        parser.add_argument(
            "--tier",
            type=str,
            dest="tank_tier",
            metavar="TIER",
            default=None,
            help="edit tank's tier I-X or 1-10",
        )
        parser.add_argument(
            "--type",
            type=str,
            dest="tank_type",
            metavar="TYPE",
            default=None,
            help="edit tank's type: " + ", ".join([n.name for n in EnumVehicleTypeInt]),
        )
        parser.add_argument(
            "--premium",
            action="store_true",
            default=False,
            dest="is_premium",
            help="set tank as premium tank",
        )
        parser.add_argument(
            "--non-premium",
            default=None,
            action="store_false",
            dest="is_premium",
            help="set tank as a non-premium tank",
        )
        return True
    except Exception as err:
        error(f"{err}")
    return False


def add_args_import(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    try:
        debug("starting")
        import_parsers = parser.add_subparsers(
            dest="import_backend",
            title="tankopedia import backend",
            description="valid import backends",
            metavar=", ".join(Backend.list_available()),
        )
        import_parsers.required = True

        for backend in Backend.get_registered():
            import_parser = import_parsers.add_parser(
                backend.driver, help=f"tankopedia import {backend.driver} help"
            )
            if not backend.add_args_import(import_parser, config=config):
                raise Exception(
                    f"Failed to define argument parser for: tankopedia import {backend.driver}"
                )

        parser.add_argument(
            "--import-model",
            metavar="IMPORT-TYPE",
            type=str,
            required=True,
            choices=["BSTank", "Tank"],
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
        error(f"{err}")
    return False


def add_args_export(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    try:
        debug("starting")
        EXPORT_FORMAT = "txt"
        EXPORT_FILE = "-"

        if config is not None and "TANKOPEDIA" in config.sections():
            configTP = config["TANKOPEDIA"]
            EXPORT_FORMAT = configTP.get("export_format", EXPORT_FORMAT)
            EXPORT_FILE = configTP.get("export_file", EXPORT_FILE)

        parser.add_argument(
            "format",
            type=str,
            nargs="?",
            choices=EXPORT_SUPPORTED_FORMATS,
            default=EXPORT_FORMAT,
            help="Export file format",
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
            "--tanks",
            type=int,
            default=None,
            nargs="*",
            metavar="TANK_ID [TANK_ID1 ...]",
            help="export tank stats for the listed TANK_ID(s)",
        )
        parser.add_argument(
            "--tier",
            type=int,
            default=None,
            metavar="TIER",
            choices=range(1, 11),
            help="export tanks of TIER",
        )
        parser.add_argument(
            "--type",
            type=str,
            default=None,
            metavar="TYPE",
            choices=[n.name for n in EnumVehicleTypeInt],
            help="export tanks of TYPE",
        )
        parser.add_argument(
            "--nation",
            type=str,
            default=None,
            metavar="NATION",
            choices=[n.name for n in EnumNation],
            help="export tanks of NATION",
        )
        parser.add_argument(
            "--premium",
            default=None,
            action="store_true",
            dest="is_premium",
            help="export premium tanks",
        )
        parser.add_argument(
            "--non-premium",
            default=None,
            action="store_false",
            dest="is_premium",
            help="export non-premium tanks",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            default=False,
            help="overwrite existing expoirt file",
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
        if args.tankopedia_cmd == "update":
            return await cmd_update(db, args)

        # elif args.tankopedia_cmd == 'edit':
        # 	return await cmd_edit(db, args)

        elif args.tankopedia_cmd == "export":
            return await cmd_export(db, args)

        elif args.tankopedia_cmd == "import":
            return await cmd_import(db, args)

        elif args.tankopedia_cmd == "add":
            return await cmd_add(db, args)

    except Exception as err:
        error(f"{err}")
    return False


########################################################
#
# cmd_add()
#
########################################################


async def cmd_add(db: Backend, args: Namespace) -> bool:
    """Add a tank to tankopedia"""
    try:
        debug("starting")
        # debug(f'{args}')
        tank_id: int = args.tank_id
        assert tank_id > 0, f"tank_id must be positive, was {tank_id}"
        tank_name: str | None = args.tank_name
        tank_nation: EnumNation | None = None
        tank_tier: EnumVehicleTier | None = None
        tank_type: EnumVehicleTypeInt | None = None
        is_premium: bool = args.is_premium

        if args.tank_nation is not None:
            try:
                tank_nation = EnumNation[args.tank_nation]
            except Exception as err:
                raise ValueError(
                    f"could not set nation from '{args.tank_nation}': {err}"
                )
        if args.tank_tier is not None:
            try:
                tank_tier = EnumVehicleTier.read_tier(args.tank_tier)
            except Exception as err:
                raise ValueError(f"could not set tier from '{args.tank_tier}': {err}")
        if args.tank_type is not None:
            try:
                tank_type = EnumVehicleTypeInt[args.tank_type]
            except Exception as err:
                raise ValueError(f"could not set nation from '{args.tank_type}': {err}")
        tank: BSTank = BSTank(
            tank_id=tank_id,
            name=tank_name,
            nation=tank_nation,
            tier=tank_tier,
            type=tank_type,
            is_premium=is_premium,
        )

        if await db.tankopedia_insert(tank):
            message(f"Added tank to {db.driver}: {tank} ({tank.tank_id})")
            return True
        else:
            error(f"Could not add tank to {db.driver}: {tank} ({tank.tank_id})")
    except Exception as err:
        error(f"{err}")
    return False


########################################################
#
# cmd_export()
#
########################################################


async def cmd_export(db: Backend, args: Namespace) -> bool:
    try:
        debug("starting")
        stats: EventCounter = EventCounter("tankopedia export")
        filename: str = args.filename
        nation: EnumNation | None = None
        tier: EnumVehicleTier | None = None
        tank_type: EnumVehicleTypeInt | None = None
        is_premium: bool | None = None
        tanks: list[BSTank] | None = None
        std_out: bool = filename == "-"

        if args.nation is not None:
            nation = EnumNation[args.nation]
            debug(f"--nation: {nation}")
        if args.tier is not None:
            tier = EnumVehicleTier(args.tier)
        if args.type is not None:
            tank_type = EnumVehicleTypeInt[args.type]
        if args.is_premium is not None:
            is_premium = args.is_premium
        if args.tanks is not None:
            tanks = list()
            for tank_id in args.tanks:
                tanks.append(BSTank(tank_id=tank_id))

        export_worker: Task

        if args.format in ["txt", "csv"]:
            tankQ: IterableQueue[BSTank] = IterableQueue(maxsize=100)

            export_worker = create_task(
                export(
                    iterable=tankQ,
                    format=args.format,
                    filename=filename,
                    force=args.force,
                    append=False,
                )
            )

            await tankQ.add_producer()
            stats.merge_child(
                await db.tankopedia_get_worker(
                    tankQ,
                    tanks=tanks,
                    tier=tier,
                    tank_type=tank_type,
                    nation=nation,
                    is_premium=is_premium,
                )
            )
            await tankQ.finish()
            await tankQ.join()
            await stats.gather_stats([export_worker], cancel=False)
        elif args.format == "json":
            tankopedia = WGApiWoTBlitzTankopedia()
            tankopedia.data = dict()
            async for tank in db.tankopedia_get_many(
                tanks=tanks,
                tier=tier,
                tank_type=tank_type,
                nation=nation,
                is_premium=is_premium,
            ):
                stats.log("tanks read")
                if (wgtank := Tank.transform(tank)) is not None:
                    tankopedia.add(wgtank)
                else:
                    error(f"could not transform tank_id={tank.tank_id}: {tank}")
            if std_out:
                print(json.dumps([wgtank.obj_src() for wgtank in tankopedia], indent=4))
            else:
                if await tankopedia.save_json(filename) > 0:
                    stats.log("tanks exported", len(tankopedia.data))
                else:
                    error("could not export tankopedia")
                    stats.log("error")

        if not std_out:
            stats.print()

    except Exception as err:
        error(f"{err}")
    return False


########################################################
#
# cmd_export()
#
########################################################


async def cmd_import(db: Backend, args: Namespace) -> bool:
    """Import tankopedia from other backend"""
    try:
        debug("starting")
        debug(f"{args}")
        stats: EventCounter = EventCounter("tankopedia import")
        tankQ: Queue[BSTank] = Queue(100)
        import_db: Backend | None = None
        import_backend: str = args.import_backend

        insert_worker: Task = create_task(
            db.tankopedia_insert_worker(tankQ=tankQ, force=args.force)
        )

        if (
            import_db := Backend.create_import_backend(
                driver=import_backend,
                args=args,
                import_type=BSTableType.Tankopedia,
                copy_from=db,
                config_file=args.import_config,
            )
        ) is None:
            raise ValueError(
                f"Could not init {import_backend} to import tankopedia from"
            )

        debug(f"import_db: {import_db.table_uri(BSTableType.Tankopedia)}")

        async for tank in import_db.tankopedia_export(sample=args.sample):
            await tankQ.put(tank)
            stats.log("tanks read")

        await tankQ.join()
        await stats.gather_stats([insert_worker])
        stats.print()
        return True
    except Exception as err:
        error(f"{err}")
    return False


########################################################
#
# cmd_update()
#
########################################################


async def cmd_update(db: Backend, args: Namespace) -> bool:
    try:
        debug("starting")
        stats = EventCounter("tankopedia update")
        force: bool = not args.add_only
        dry_run: bool = args.dry_run
        stats.log("added", 0)
        stats.log("updated", 0)
        tankopedia_new: WGApiWoTBlitzTankopedia | None
        if args.tankopedia_update_source == "file":
            debug("wi")
            if (tankopedia_new := await cmd_update_file(db, args)) is None:
                raise ValueError(f"failed to read tankopedia from file: {args.file}")
            stats.log("tanks read from file", len(tankopedia_new))

        elif args.tankopedia_update_source == "wg":
            debug("wg")
            if (tankopedia_new := await cmd_update_wg(db, args)) is None:
                raise ValueError(f"failed to read tankopedia from WG API")
            stats.log("tanks read from WG API", len(tankopedia_new))

        else:
            raise ValueError(
                f"tankopedia update: unknown or missing subcommand: {args.setup_cmd}"
            )

        tankopedia: WGApiWoTBlitzTankopedia | None
        if (tankopedia := await get_tankopedia(db)) is None:
            error(f"could not read Tankopedia from backend: {db.backend}")
            return False

        added: set[int]
        updated: set[int]
        try:
            (added, updated) = tankopedia.update(tankopedia_new)
        except Exception as err:
            error(f"failed to update tankopedia: {err}")
            raise

        for tank_id in added:
            if (tank := BSTank.transform(tankopedia[tank_id])) is None:
                error(
                    f"Could not transform {tankopedia[tank_id].name} (tank_id={tank_id}) to BSTank format"
                )
                stats.log("errors")
                continue

            if dry_run:
                verbose("would add: tank_id=%d %s", tank.tank_id, tank.name)
                stats.log("would add")
            else:
                if await db.tankopedia_insert(tank=tank, force=force):
                    verbose("added: tank_id=%d %s", tank.tank_id, tank.name)
                    stats.log("added")
                else:
                    stats.log("not added")

        for tank_id in updated:
            if (tank := BSTank.transform(tankopedia[tank_id])) is None:
                error(
                    f"Could not transform {tankopedia[tank_id].name} (tank_id={tank_id}) to BSTank format"
                )
                continue

            if dry_run:
                stats.log("would update")
                verbose("would update: tank_id=%d %s", tank.tank_id, tank.name)
            else:
                if await db.tankopedia_insert(tank=tank, force=force):
                    verbose("updated: tank_id=%d %s", tank.tank_id, tank.name)
                    stats.log("updated")
                else:
                    stats.log("not updated")

        stats.print()

    except Exception as err:
        error(f"{err}")
    return False


async def get_tankopedia(db: Backend) -> WGApiWoTBlitzTankopedia | None:
    """return Tankopedia from backend"""
    debug("starting")
    tankopedia = WGApiWoTBlitzTankopedia()
    try:
        async for tank in db.tankopedia_get_many():
            debug("read: tank_id=%d %s", tank.tank_id, tank.name)
            if (wgtank := Tank.transform(tank)) is not None:
                tankopedia.add(wgtank)
            else:
                debug(f"failed to transform tank to Tank format: {tank}")
        debug("read %d tanks from backend", len(tankopedia))
        return tankopedia
    except Exception as err:
        error(f"{err}")
    return None


async def cmd_update_file(
    db: Backend, args: Namespace
) -> WGApiWoTBlitzTankopedia | None:
    """Update tankopedia in the database from a file"""
    debug("starting")
    filename: str = args.file
    return await WGApiWoTBlitzTankopedia.open_json(filename)


async def cmd_update_wg(db: Backend, args: Namespace) -> WGApiWoTBlitzTankopedia | None:
    """Update tankopedia in the database from a WG API"""
    debug("starting")
    region: Region = Region[args.wg_region]
    async with WGApi(
        app_id=args.wg_app_id,
        ru_app_id=args.ru_app_id,
        rate_limit=args.wg_rate_limit,
        ru_rate_limit=args.ru_rate_limit,
    ) as wg:
        return await wg.get_tankopedia(region=region)
