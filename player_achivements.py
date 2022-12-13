from argparse import ArgumentParser, Namespace
from configparser import ConfigParser
from typing import Optional, Iterable, cast
import logging
from asyncio import create_task, gather, Queue, CancelledError, Task, sleep
from aiofiles import open
from os.path import isfile
from asyncstdlib import enumerate
from alive_progress import alive_bar		# type: ignore

from backend import Backend, OptAccountsInactive, ACCOUNTS_Q_MAX, MIN_UPDATE_INTERVAL
from models import BSAccount, BSBlitzRelease, StatsTypes
from pyutils import BucketMapper, CounterQueue, EventCounter
from pyutils.utils import get_url, get_url_JSON_model, epoch_now
from blitzutils.models import Region, WGApiWoTBlitzPlayerAchievements, PlayerAchievementsMaxSeries
from blitzutils.wg import WGApi 

logger = logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug

# Constants

WORKERS_WGAPI 		: int = 40