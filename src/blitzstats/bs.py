#!/usr/bin/env python3

# Script fetch Blitz player stats and tank stats

from datetime import datetime
from typing import Optional
from pyutils.multilevelformatter import MultilevelFormatter

try:
    import yappi  # type: ignore

    # import start, stop, get_func_stats, set_clock_type, COLUMNS_FUNCSTATS  # type: ignore
except (ImportError, ModuleNotFoundError):
    yappi = None

from configparser import ConfigParser
import logging
from argparse import ArgumentParser
from sys import stdout
from os import linesep
from os.path import isfile, dirname, realpath, expanduser
from asyncio import run

# sys.path.insert(0, dirname(dirname(realpath(__file__))))

from blitzstats.backend import Backend
from blitzstats.mongobackend import MongoBackend # noqa
from blitzstats import accounts
from blitzstats import replays
from blitzstats import releases
from blitzstats import tank_stats
from blitzstats import player_achievements
from blitzstats import setup
from blitzstats import tankopedia

# logging.getLogger("asyncio").setLevel(logging.DEBUG)
logger = logging.getLogger()
error = logger.error
message = logger.warning
verbose = logger.info
debug = logger.debug


# Utils
def get_datestr(date_time: datetime | None = None) -> str:
    if date_time is None:
        date_time = datetime.now()
    return f"{date_time:%Y%m%d_%H%M}"


def time_str(date_time: datetime | None = None) -> str:
    """Return time as string in YYYY-MM-DD HH:mm format.
    Default is current local time"""
    if date_time is None:
        date_time = datetime.now()
    return f"{date_time:%Y-%m-%d %H:%M}"


def time_elapsed(start: datetime, end: datetime | None = None) -> str:
    """Return time difference as string in HH:mm:ss format"""
    if end is None:
        end = datetime.now()
    seconds: int = int((end - start).total_seconds())
    hours, remainder = divmod(seconds, 60 * 60)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours}h {minutes}min {seconds}sec"


# main() -------------------------------------------------------------


async def main() -> int:
    # set the directory for the script
    global logger, error, debug, verbose, message
    start_time: datetime = datetime.now()

    # Default params
    _PKG_NAME = "blitzstats"
    CONFIG = _PKG_NAME + ".ini"
    LOG = _PKG_NAME + ".log"
    # THREADS 	= 20    # make it module specific?
    BACKEND: Optional[str] = None
    config: Optional[ConfigParser] = None
    CONFIG_FILE: Optional[str] = None

    CONFIG_FILES: list[str] = [
        "./" + CONFIG,
        dirname(realpath(__file__)) + "/" + CONFIG,
        "~/." + CONFIG,
        "~/.config/" + CONFIG,
        "~/.config/" + _PKG_NAME + "/config",
    ]
    for fn in [expanduser(f) for f in CONFIG_FILES]:
        if isfile(fn):
            CONFIG_FILE = fn
            verbose(f"config file: {CONFIG_FILE}")
            break
    if CONFIG_FILE is None:
        error("config file not found in: " + ", ".join(CONFIG_FILES))

    parser = ArgumentParser(description="Fetch and manage WoT Blitz stats", add_help=False)
    arggroup_verbosity = parser.add_mutually_exclusive_group()
    arggroup_verbosity.add_argument(
        "--debug", "-d", dest="LOG_LEVEL", action="store_const", const=logging.DEBUG, help="Debug mode"
    )
    arggroup_verbosity.add_argument(
        "--verbose", "-v", dest="LOG_LEVEL", action="store_const", const=logging.INFO, help="Verbose mode"
    )
    arggroup_verbosity.add_argument(
        "--silent", "-s", dest="LOG_LEVEL", action="store_const", const=logging.CRITICAL, help="Silent mode"
    )
    parser.add_argument(
        "--log", type=str, nargs="?", default=None, const=f"{LOG}_{get_datestr()}", help="Enable file logging"
    )
    parser.add_argument("--config", type=str, default=CONFIG_FILE, metavar="CONFIG", help="Read config from CONFIG")
    parser.set_defaults(LOG_LEVEL=logging.WARNING)

    args, argv = parser.parse_known_args()

    debug(f"Args parsed: {str(args)}")
    debug(f"Args not parsed yet: {str(argv)}")

    try:
        # setup logging
        logger.setLevel(args.LOG_LEVEL)
        logger_conf: dict[int, str] = {
            logging.INFO: "%(message)s",
            logging.WARNING: "%(message)s",
            # logging.ERROR: 		'%(levelname)s: %(message)s'
        }
        MultilevelFormatter.setLevels(
            logger, fmts=logger_conf, fmt="%(levelname)s: %(funcName)s(): %(message)s", log_file=args.log
        )
        error = logger.error
        message = logger.warning
        verbose = logger.info
        debug = logger.debug

        if args.config is not None and isfile(args.config):
            debug(f"Reading config from {args.config}")
            config = ConfigParser()
            config.read(args.config)
            if "BACKEND" in config.sections():
                debug("Reading config section BACKEND")
                configDef = config["BACKEND"]
                BACKEND = configDef.get("driver", None)
            else:
                error(f"No section BACKEND found from config: {args.config}")
                error(f"Sections found: {' ,'.join([s for s in config.sections() ])}")
        else:
            debug("No config file found")

        # Parse command args
        parser.add_argument("-h", "--help", action="store_true", help="Show help")
        if yappi is not None:
            parser.add_argument(
                "--profile", type=int, default=0, metavar="N", help="Profile performance for N slowest function calls"
            )
        parser.add_argument(
            "--backend", type=str, choices=Backend.list_available(), default=BACKEND, help="Choose backend to use"
        )

        cmd_parsers = parser.add_subparsers(
            dest="main_cmd",
            title="main commands",
            description="valid subcommands",
            metavar="accounts | tank-stats | player-achievements | replays | tankopedia | releases | setup",
        )
        cmd_parsers.required = True

        accounts_parser = cmd_parsers.add_parser("accounts", aliases=["ac"], help="accounts help")
        tank_stats_parser = cmd_parsers.add_parser("tank-stats", aliases=["ts"], help="tank-stats help")
        player_achievements_parser = cmd_parsers.add_parser(
            "player-achievements", aliases=["pa"], help="player-achievements help"
        )
        replays_parser = cmd_parsers.add_parser("replays", help="replays help")
        tankopedia_parser = cmd_parsers.add_parser("tankopedia", help="tankopedia help")
        releases_parser = cmd_parsers.add_parser("releases", help="releases help")
        setup_parser = cmd_parsers.add_parser("setup", help="setup help")

        if not accounts.add_args(accounts_parser, config):
            raise Exception("Failed to define argument parser for: accounts")
        if not replays.add_args(replays_parser, config):
            raise Exception("Failed to define argument parser for: replays")
        if not releases.add_args(releases_parser, config):
            raise Exception("Failed to define argument parser for: releases")
        if not tankopedia.add_args(tankopedia_parser, config):
            raise Exception("Failed to define argument parser for: tankopedia")
        if not setup.add_args(setup_parser, config):
            raise Exception("Failed to define argument parser for: setup")
        if not tank_stats.add_args(tank_stats_parser, config):
            raise Exception("Failed to define argument parser for: tank-stats")
        if not player_achievements.add_args(player_achievements_parser, config):
            raise Exception("Failed to define argument parser for: player-achievements")

        debug("parsing full args")
        args = parser.parse_args(args=argv)
        if args.help:
            parser.print_help()
        debug("arguments given:")
        debug(str(args))

        backend: Backend | None = Backend.create(args.backend, config=config)
        assert backend is not None, "Could not initialize backend"

        if yappi is not None and args.profile > 0:
            print("Starting profiling...")
            yappi.set_clock_type("cpu")
            yappi.start(builtins=True)

        if args.main_cmd == "accounts":
            await accounts.cmd(backend, args)
        elif args.main_cmd == "tank-stats":
            await tank_stats.cmd(backend, args)
        elif args.main_cmd == "player-achievements":
            await player_achievements.cmd(backend, args)
        elif args.main_cmd == "replays":
            await replays.cmd(backend, args)
        elif args.main_cmd == "releases":
            await releases.cmd(backend, args)
        elif args.main_cmd == "tankopedia":
            await tankopedia.cmd(backend, args)
        elif args.main_cmd == "setup":
            await setup.cmd(backend, args)
        else:
            parser.print_help()

        if yappi is not None and args.profile > 0:
            print("Stopping profiling")
            yappi.stop()
            stats = yappi.get_func_stats().sort(sort_type="ttot", sort_order="desc")
            print_all(stats, stdout, args.profile)

    except Exception as err:
        error(f"{err}")
    if args.main_cmd in ["accounts", "tank-stats", "player-achievements", "replays"]:
        message(f"program finished: {time_str()} ({time_elapsed(start_time)})")
    return 0


def print_all(stats, out, limit: int | None = None) -> None:
    if stats.empty() or yappi is None:
        return
    sizes = [150, 12, 8, 8, 8]
    columns = dict(zip(range(len(yappi.COLUMNS_FUNCSTATS)), zip(yappi.COLUMNS_FUNCSTATS, sizes)))
    show_stats = stats
    if limit:
        show_stats = stats[:limit]
    out.write(linesep)
    for stat in show_stats:
        stat._print(out, columns)


########################################################
#
# main() entry
#
########################################################


def cli_main():
    run(main())


if __name__ == "__main__":
    # asyncio.run(main(sys.argv[1:]), debug=True)
    cli_main()
