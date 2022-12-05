from configparser import ConfigParser
from argparse import Namespace, ArgumentParser
import logging
from abc import ABCMeta, abstractmethod
from bson import ObjectId
#from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCursor, AsyncIOMotorCollection # type: ignore
from os.path import isfile
from typing import Optional, Any, Iterable, AsyncGenerator, TypeVar, cast
from time import time
from datetime import date
from enum import Enum, StrEnum
from asyncio import Queue, CancelledError
# from pydantic import ValidationError

from models import BSAccount, BSBlitzRelease, StatsTypes
from blitzutils.models import Region, WoTBlitzReplayJSON, WGtankStat, Account, Tank, WGBlitzRelease
from pyutils.utils import epoch_now
from pyutils import EventCounter
# from mongobackend import MongoBackend

# Setup logging
logger	= logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug

# Constants
MAX_UPDATE_INTERVAL : int = 4*30*24*60*60 # 4 months
INACTIVE_THRESHOLD 	: int = 2*30*24*60*60 # 2 months
WG_ACCOUNT_ID_MAX 	: int = int(31e8)
MIN_INACTIVITY_PERIOD : int = 7 # days
MAX_RETRIES 		: int = 3
CACHE_VALID 		: int = 5   # days
ACCOUNTS_Q_MAX 		: int = 10000
TANK_STATS_BATCH	: int = 1000

class OptAccountsInactive(StrEnum):
	auto	= 'auto'
	no		= 'no'
	yes 	= 'yes'
	both	= 'both'

	@classmethod
	def default(cls) -> 'OptAccountsInactive':
		return cls.auto
	

class OptAccountsDistributed():

	def __init__(self, mod: int, div: int):
		assert type(mod) is int and mod >=0 , 'Modulus has to be integer >= 0'
		assert type(div) is int and div > 0, 'Divisor has to be positive integer'
		self.mod : int = mod
		self.div : int = div


	@classmethod
	def parse(cls, input: str) -> Optional['OptAccountsDistributed']:
		try:
			res : list[str] = input.split(':')
			if len(res) != 2:
				raise ValueError(f'Input ({input} does not match format "I:N")')
			mod : int = int(res[0])
			div : int = int(res[1])
			return OptAccountsDistributed(mod, div)
		except Exception as err:
			error(f'{err}')
		return None


	def match(self, value : int) -> bool:
		assert type(value) is int, "value has to be integere"
		return value % self.div == self.mod

# BackendSelf = TypeVar('BackendSelf', bound='Backend')
class Backend(metaclass=ABCMeta):
	"""Abstract class for a backend (mongo, postgres, files)"""

	name : str = 'Backend'
	_cache_valid : int = CACHE_VALID
	_backends : dict[str, type['Backend']] = dict()

	@abstractmethod
	def __init__(self, config: ConfigParser | None = None, 
					**kwargs):
		"""Init MongoDB backend from config file and CLI args
			CLI arguments overide settings in the config file"""
		raise NotImplementedError

	# @classmethod
	# def create(cls, backend : str, 
	# 			config : ConfigParser | None = None, **kwargs) -> Optional['Backend']:
	# 	try:
	# 		if backend == 'mongodb':
	# 			return MongoBackend(config, **kwargs)
	# 		else:				
	# 			assert False, f'Backend not implemented: {backend}'
	# 	except Exception as err:
	# 		error(f'Error creating backend {backend}: {err}')
	# 	return None

	@classmethod
	def register(cls, name : str, backend: type['Backend']) -> bool:
		try:
			if name not in cls._backends:
				cls._backends[name] = backend
				return True
			else:
				error(f'Backend {name} has already been registered')
		except Exception as err:
			error(f'Error registering backend {name}: {err}')
		return False

	
	@classmethod
	def get_registered(cls) -> list[type['Backend']]:
		return list(cls._backends.values())
		
		
	@classmethod
	def get(cls, backend: str) -> Optional[type['Backend']]:
		try:
			return cls._backends[backend]
		except:
			return None


	@classmethod
	def create(cls, backend : str, config : ConfigParser | None = None, 
					copy_from: 'Backend' | None = None, 
					**kwargs) -> Optional['Backend']:
		try:
			if copy_from is not None and copy_from.name == backend:
				return copy_from.copy(config, **kwargs)
			# elif backend == 'mongodb':
			# 	return MongoBackend(config=config, **kwargs)
			elif backend in cls._backends:
				return cls._backends[backend](config=config, **kwargs)
			else:
				assert False, f'Backend not implemented: {backend}'
		except Exception as err:
			error(f'Error creating backend {backend}: {err}')
		return None


	@classmethod	
	def add_args_import(cls, parser: ArgumentParser, config: Optional[ConfigParser] = None, 
							import_types: list[str] = list()) -> bool:
		parser.add_argument('--import-type', metavar='IMPORT-TYPE', type=str, 
							default=import_types[0], choices=import_types, 
							help='Collection to use. Uses current database as default')
		parser.add_argument('--import-config', metavar='CONFIG', type=str, default=None, 
								help='Config file for backend to import from. \
								Default is to use existing backend')
		parser.add_argument('--sample', type=float, default=0, help='Sample size')
		
		return True


	@classmethod
	def read_args(cls, args : Namespace, backend: str) -> dict[str, Any]:
		"""Read Argparse args for creating a Backend()"""
		if backend in cls._backends:
			return cls._backends[backend].read_args(args, backend=backend)
		else:
			raise ValueError(f'Backend not implemented: {backend}')


	@classmethod
	def list_available(cls) -> list[str]:
		return ['mongodb']

	
	@property
	def cache_valid(self) -> int:
		return self._cache_valid


	@abstractmethod
	def __eq__(self, __o: object) -> bool:
		raise NotImplementedError


	@abstractmethod
	async def init(self) -> bool:
		"""Init backend and indexes"""
		raise NotImplementedError


	@abstractmethod
	def copy(self, config : ConfigParser | None = None, **kwargs) -> Optional['Backend']:
		"""Create a copy of backend"""
		raise NotImplementedError


	@property
	@abstractmethod
	def database(self) -> str:
		raise NotImplementedError


	@abstractmethod
	def set_database(self, database : str) -> bool:
		"""Set database"""
		raise NotImplementedError


	@property
	@abstractmethod
	def table_accounts(self) -> str:
		raise NotImplementedError


	@property
	@abstractmethod
	def table_tank_stats(self) -> str:
		raise NotImplementedError


	@property
	@abstractmethod
	def table_player_achievements(self) -> str:
		raise NotImplementedError


	@property
	@abstractmethod
	def table_releases(self) -> str:
		raise NotImplementedError


	@property
	@abstractmethod
	def table_replays(self) -> str:
		raise NotImplementedError


	@property
	@abstractmethod
	def table_tankopedia(self) -> str:
		raise NotImplementedError


	@abstractmethod
	def set_table(self, table_type: str, new: str) -> bool:
		"""Set database table/collection"""
		raise NotImplementedError


	#----------------------------------------
	# replays
	#----------------------------------------

	@abstractmethod
	async def replay_insert(self, replay: WoTBlitzReplayJSON) -> bool:
		"""Store replay into backend"""
		raise NotImplementedError


	@abstractmethod
	async def replay_get(self, replay_id: str | ObjectId) -> WoTBlitzReplayJSON | None:
		"""Get a replay from backend based on replayID"""
		raise NotImplementedError


	# replay fields that can be searched: protagonist, battle_start_timestamp, account_id, vehicle_tier
	@abstractmethod
	async def replay_find(self, **kwargs) -> AsyncGenerator[WoTBlitzReplayJSON, None]:
		"""Find a replay from backend based on search string"""
		raise NotImplementedError

	#----------------------------------------
	# accounts
	#----------------------------------------
	
	@abstractmethod
	async def account_get(self, account_id: int) -> BSAccount | None:
		"""Get account from backend"""
		raise NotImplementedError


	@abstractmethod
	async def accounts_get(self, stats_type : StatsTypes | None = None, 
							regions: set[Region] = Region.API_regions(), 
							inactive : OptAccountsInactive = OptAccountsInactive.default(), 	
							disabled: bool = False, 
							dist : OptAccountsDistributed | None = None, sample : float = 0, 
							force : bool = False, cache_valid: int | None = None ) -> AsyncGenerator[BSAccount, None]:
		"""Get accounts from backend"""
		raise NotImplementedError
		yield BSAccount(id=-1)


	async def accounts_get_worker(self, accountQ : Queue[BSAccount], **kwargs) -> EventCounter:
		debug('starting')
		stats : EventCounter = EventCounter('accounts')
		try:
			async for account in self.accounts_get(**kwargs):
				await accountQ.put(account)
				stats.log('queued')		
		except CancelledError as err:
			debug(f'Cancelled')
		except Exception as err:
			error(f'{err}')
		return stats

	
	@abstractmethod
	async def accounts_export(self, account_type: type[Account] = BSAccount, 
								regions: set[Region] = Region.has_stats(), 
								sample : float = 0) -> AsyncGenerator[BSAccount, None]:
		"""import accounts"""
		raise NotImplementedError
		yield BSAccount()


	async def accounts_insert_worker(self, accountQ : Queue[BSAccount], force: bool = False) -> EventCounter:
		debug(f'starting, force={force}')
		stats : EventCounter = EventCounter('accounts insert')
		try:
			while True:
				account = await accountQ.get()
				try:
					if force:
						debug(f'Trying to upsert account_id={account.id} into {self.name}:{self.database}.{self.table_accounts}')
						await self.account_update(account, upsert=True)
					else:
						debug(f'Trying to insert account_id={account.id} into {self.name}:{self.database}.{self.table_accounts}')
						await self.account_insert(account)
					if force:
						stats.log('accounts added/updated')
					else:
						stats.log('accounts added')
				except Exception as err:
					debug(f'Error: {err}')
					stats.log('accounts not added')
				finally:
					accountQ.task_done()
		except CancelledError as err:
			debug(f'Cancelled')
		except Exception as err:
			error(f'{err}')
		return stats
	

	@abstractmethod
	async def accounts_count(self, stats_type : StatsTypes | None = None, 
							regions: set[Region] = Region.API_regions(), 
							inactive : OptAccountsInactive = OptAccountsInactive.default(), 	
							disabled: bool = False, 
							dist : OptAccountsDistributed | None = None, sample : float = 0, 
							force : bool = False, cache_valid: int | None = None ) -> int:
		"""Get number of accounts from backend"""
		raise NotImplementedError


	@abstractmethod
	async def account_update(self, account: BSAccount, upsert: bool = True) -> bool:
		"""Update an account in the backend. Returns False 
			if the account was not updated"""
		raise NotImplementedError


	@abstractmethod
	async def account_insert(self, account: BSAccount) -> bool:
		"""Store account to the backend. Returns False 
			if the account was not added"""
		raise NotImplementedError
	

	@abstractmethod
	async def accounts_insert(self, accounts: Iterable[BSAccount]) -> tuple[int, int]:
		"""Store accounts to the backend. Returns number of accounts inserted and not inserted""" 			
		raise NotImplementedError

	#----------------------------------------
	# tank stats
	#----------------------------------------

	@abstractmethod
	async def tank_stats_insert(self, tank_stats: Iterable[WGtankStat]) -> tuple[int, int, int]:
		"""Store tank stats to the backend. Returns number of stats inserted and not inserted"""
		raise NotImplementedError

	
	@abstractmethod
	async def tank_stats_update(self, tank_stats: list[WGtankStat], upsert: bool = False) -> tuple[int, int, int]:
		"""Update or upsert tank stats to the backend. Returns number of stats updated and not updated"""
		raise NotImplementedError


	@abstractmethod
	async def tank_stats_get(self, account: BSAccount, tank_id: int | None = None, 
								last_battle_time: int | None = None) -> AsyncGenerator[WGtankStat, None]:
		"""Return tank stats from the backend"""
		raise NotImplementedError

	
	@abstractmethod
	async def tank_stats_count(self, release: BSBlitzRelease | None = None,
							regions: set[Region] = Region.API_regions(), 
							sample : float = 0, 
							force : bool = False) -> int:
		"""Get number of tank-stats from backend"""
		raise NotImplementedError


	async def tank_stats_insert_worker(self, tank_statsQ : Queue[list[WGtankStat]], force: bool = False) -> EventCounter:
		debug(f'starting, force={force}')
		stats : EventCounter = EventCounter('tank-stats insert')
		try:
			added : int
			not_added : int
			read : int
			while True:
				added = 0
				not_added = 0
				tank_stats = await tank_statsQ.get()
				read = len(tank_stats)
				try:
					if force:
						debug(f'Trying to upsert {read} tank stats into {self.name}:{self.database}.{self.table_tank_stats}')
						added, not_added, _ = await self.tank_stats_update(tank_stats, upsert=True)
						stats.log('tank stats added/updated', added)
					else:
						debug(f'Trying to insert {read} tank stats into {self.name}:{self.database}.{self.table_tank_stats}')
						added, not_added, _ = await self.tank_stats_insert(tank_stats)
						stats.log('accounts added', added)
					stats.log('accounts not added', not_added)
				except Exception as err:
					debug(f'Error: {err}')
					stats.log('errors', read)
				finally:
					tank_statsQ.task_done()
		except CancelledError as err:
			debug(f'Cancelled')
		except Exception as err:
			error(f'{err}')
		return stats


	#----------------------------------------
	# Releases
	#----------------------------------------

	@abstractmethod
	async def release_get(self, release : str) -> BSBlitzRelease | None:
		raise NotImplementedError
	

	@abstractmethod
	async def releases_get(self, release: str | None = None, since : date | None = None, 
							first : BSBlitzRelease | None = None) -> list[BSBlitzRelease]:
		raise NotImplementedError


	@abstractmethod
	async def release_get_latest(self) -> BSBlitzRelease | None:
		"""Get the latest release in the backend"""
		raise NotImplementedError


	@abstractmethod
	async def release_get_current(self) -> BSBlitzRelease | None:
		"""Get the latest release in the backend"""
		raise NotImplementedError


	@abstractmethod
	async def release_update(self, release: BSBlitzRelease, upsert: bool = True) -> bool:
		"""Update an release in the backend. Returns False 
			if the release was not updated"""
		raise NotImplementedError


	@abstractmethod
	async def release_insert(self, release: BSBlitzRelease) -> bool:
		"""Insert new release to the backend"""
		raise NotImplementedError
	

	async def releases_insert_worker(self, releaseQ : Queue[BSBlitzRelease], force: bool = False) -> EventCounter:
		debug(f'starting, force={force}')
		stats : EventCounter = EventCounter('accounts insert')
		try:
			while True:
				release = await releaseQ.get()
				try:
					if force:
						debug(f'Trying to upsert release={release.release} into {self.name}:{self.database}.{self.table_releases}')
						await self.release_update(release, upsert=True)
					else:
						debug(f'Trying to insert release={release.release} into {self.name}:{self.database}.{self.table_releases}')
						await self.release_insert(release)
					if force:
						stats.log('releases added/updated')
					else:
						stats.log('releases added')
				except Exception as err:
					debug(f'Error: {err}')
					stats.log('releases not added')
				finally:
					releaseQ.task_done()
		except CancelledError as err:
			debug(f'Cancelled')
		except Exception as err:
			error(f'{err}')
		return stats
