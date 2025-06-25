from argparse import ArgumentParser, Namespace
from configparser import ConfigParser
from typing import Optional, List
import logging

from .backend import Backend, BSTableType

logger = logging.getLogger(__name__)
error = logger.error
message = logger.warning
verbose = logger.info
debug = logger.debug

###########################################
#
# add_args_accouts functions
#
###########################################


def add_args(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
    try:
        debug("starting")
        setup_parsers = parser.add_subparsers(
            dest="setup_cmd",
            title="setup commands",
            description="valid commands",
            help="setup help",
            metavar="init | list | test",
        )
        setup_parsers.required = True
        init_parser = setup_parsers.add_parser("init", help="setup init help")
        if not add_args_init(init_parser, config=config):
            raise Exception("Failed to define argument parser for: setup init")

        list_parser = setup_parsers.add_parser("list", help="setup list help")
        if not add_args_list(list_parser, config=config):
            raise Exception("Failed to define argument parser for: setup list")

        test_parser = setup_parsers.add_parser("test", help="setup test help")
        if not add_args_test(test_parser, config=config):
            raise Exception("Failed to define argument parser for: setup test")

        return True
    except Exception as err:
        error(f"{err}")
    return False


def add_args_init(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    try:
        debug("starting")
        tables: List[str] = ["all"] + sorted([tt.value for tt in BSTableType])
        parser.add_argument(
            "setup_init_tables",
            nargs="*",
            default="all",
            choices=tables,
            metavar="TABLE [TABLE...]",
            help="TABLE(S) to initialize: " + ", ".join(tables),
        )
        return True
    except Exception as err:
        error(f"{err}")
    return False


def add_args_list(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    try:
        debug("starting")
        tables: List[str] = ["all"] + sorted([tt.value for tt in BSTableType])
        parser.add_argument(
            "setup_list_tables",
            nargs="*",
            default="all",
            choices=tables,
            metavar="TABLE [TABLE...]",
            help="TABLE(S) to list: " + ", ".join(tables),
        )
        return True
    except Exception as err:
        error(f"{err}")
    return False


def add_args_test(
    parser: ArgumentParser, config: Optional[ConfigParser] = None
) -> bool:
    try:
        debug("starting")
        tables: List[str] = ["all"] + sorted([tt.value for tt in BSTableType])
        parser.add_argument(
            "setup_test_tables",
            nargs="*",
            default="all",
            choices=tables,
            metavar="TABLE [TABLE...]",
            help="TABLE(S) to test: " + ", ".join(tables),
        )
        parser.add_argument(
            "setup_test_tests",
            default=1000,
            nargs="?",
            type=int,
            metavar="N",
            help="number of tests to run",
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
        if args.setup_cmd == "init":
            debug("setup init")
            return await cmd_init(db, args)

        elif args.setup_cmd == "list":
            debug("setup list")
            return await cmd_list(db, args)

        elif args.setup_cmd == "test":
            debug("setup test")
            return await cmd_test(db, args)
        else:
            error(f"setup: unknown or missing subcommand: {args.setup_cmd}")

    except Exception as err:
        error(f"{err}")
    return False


async def cmd_init(db: Backend, args: Namespace) -> bool:
    try:
        debug("starting")
        tables: List[str] = args.setup_init_tables

        if "all" in tables:
            tables = [tt.value for tt in BSTableType]
        await db.init(tables=tables)
        return True
    except Exception as err:
        error(f"{err}")
    return False


async def cmd_list(db: Backend, args: Namespace) -> bool:
    try:
        debug("starting")
        tables: List[str] = args.setup_list_tables

        if "all" in tables:
            tables = [tt.value for tt in BSTableType]
        return db.list_config(tables=tables)
    except Exception as err:
        error(f"{err}")
    return False


async def cmd_test(db: Backend, args: Namespace) -> bool:
    try:
        debug("starting")
        tables: List[str] = args.setup_test_tables

        if "all" in tables:
            tables = [tt.value for tt in BSTableType]
        await db.test_config(tables=tables, tests=args.setup_test_tests)
        return True
    except Exception as err:
        error(f"{err}")
    return False
