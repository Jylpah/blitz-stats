from configparser import ConfigParser
from argparse import Namespace, ArgumentParser
import logging
from abc import ABC, abstractmethod
from bson import ObjectId
#from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCursor, AsyncIOMotorCollection # type: ignore
from os.path import isfile
from typing import Optional, Any, Iterable, AsyncGenerator, TypeVar, cast
from time import time
from re import compile
from datetime import date, datetime
from enum import Enum, StrEnum, IntEnum
from asyncio import Queue, CancelledError
from pydantic import BaseModel, Field


from models import BSAccount, BSBlitzRelease, StatsTypes
from blitzutils.models import Region, WoTBlitzReplayJSON, WGtankStat, Account, Tank, WGBlitzRelease
from pyutils.utils import epoch_now, is_alphanum_
from pyutils import EventCounter, JSONExportable
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
MAX_RETRIES 		: int = 3
CACHE_VALID 		: int = 3   # days
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


class BSTableType(StrEnum):
	Accounts 			= 'Accounts'
	Tankopedia 			= 'Tankopedia'
	Releases			= 'Releases'
	Replays 			= 'Replays'
	TankStats 			= 'TankStats'
	PlayerAchievements 	= 'PlayerAchievements'
	ErrorLog			= 'ErrorLog'
	AccountLog			= 'AccountLog'

class ErrorLogType(IntEnum):
	OK				= 0
	Info			= 1
	Warning 		= 2
	Error 			= 3
	Critical 		= 4
	
	ValidationError = 10
	ValueError		= 11
	NotFoundError 	= 12

	Duplicate 		= 20


class ErrorLog(JSONExportable, ABC):
	table 	: str 					= Field(alias='t')
	doc_id 	: Any | None			= Field(default=None, alias='did')
	date 	: datetime				= Field(default=datetime.now(), alias='d')
	msg 	: str | None			= Field(default=None, alias='e')
	type 	: ErrorLogType			= Field(default=ErrorLogType.Error, alias='t')


	class Config:
		arbitrary_types_allowed = True
		allow_mutation 			= True
		validate_assignment 	= True
		allow_population_by_field_name = True
		# json_encoders = { ObjectId: str }


class Backend(ABC):
	"""Abstract class for a backend (mongo, postgres, files)"""

	driver 		: str = 'Backend'
	_cache_valid : int = CACHE_VALID
	_backends 	: dict[str, type['Backend']] = dict()	


	def __init__(self, config: ConfigParser | None = None, 
					**kwargs):					
		"""Init MongoDB backend from config file and CLI args
			CLI arguments overide settings in the config file"""
		
		self._database : str 	= 'BlitzStats'
		# default tables/collections
		self._T 	: dict[BSTableType,str] = dict()

		self._T[BSTableType.Accounts] 			= 'Accounts'
		self._T[BSTableType.Tankopedia] 		= 'Tankopedia'
		self._T[BSTableType.Releases] 			= 'Releases'
		self._T[BSTableType.Replays] 			= 'Replays'
		self._T[BSTableType.AccountLog] 		= 'BSAccountLog' 	# Rename after transition
		self._T[BSTableType.ErrorLog] 			= 'BSErrorLog'		# Rename after transition
		message('Reminder: Rename Backend ErrorLog & AccountLog')
		self._T[BSTableType.TankStats] 			= 'TankStats'
		self._T[BSTableType.PlayerAchievements] = 'PlayerAchievements'
		# raise NotImplementedError


	@classmethod
	def register(cls, name : str, backend: type['Backend']) -> bool:
		try:
			debug(f'Registering backend: {name}')
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
					copy_from: Optional['Backend'] = None, 
					**kwargs) -> Optional['Backend']:
		try:
			debug('starting')
			if copy_from is not None and copy_from.driver == backend:
				return copy_from.copy(config, **kwargs)
			elif backend in cls._backends:
				return cls._backends[backend](config=config, **kwargs)
			else:
				assert False, f'Backend not implemented: {backend}'
		except Exception as err:
			error(f'Error creating backend {backend}: {err}')
		return None


	@classmethod	
	def add_args_import(cls, parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
		debug('starting')
		parser.add_argument('--import-config', metavar='CONFIG', type=str, default=None, 
								help='Config file for backend to import from. \
								Default is to use existing backend')
		return True


	@classmethod
	def read_args(cls, args : Namespace, backend: str) -> dict[str, Any]:
		"""Read Argparse args for creating a Backend()"""
		debug('starting')
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
	def backend(self) -> str:
		raise NotImplementedError


	@property
	def database(self) -> str:
		return self._database

	
	def set_database(self, database : str) -> None:
		"""Set database"""
		assert is_alphanum_(database), f'Illegal characters in the table name: {database}'
		self._database = database


	@property
	def table_accounts(self) -> str:
		return self._T[BSTableType.Accounts]


	@property
	def table_tankopedia(self) -> str:
		return self._T[BSTableType.Tankopedia]


	@property
	def table_releases(self) -> str:
		return self._T[BSTableType.Releases]


	@property
	def table_replays(self) -> str:
		return self._T[BSTableType.Replays]


	@property	
	def table_tank_stats(self) -> str:
		return self._T[BSTableType.TankStats]


	@property
	def table_player_achievements(self) -> str:
		return self._T[BSTableType.PlayerAchievements]


	@property	
	def table_account_log(self) -> str:
		return self._T[BSTableType.AccountLog]


	@property	
	def table_error_log(self) -> str:
		return self._T[BSTableType.ErrorLog]


	def get_table(self, table_type: BSTableType) -> str:
		"""Get database table/collection"""		
		return self._T[table_type] 


	def set_table(self, table_type: BSTableType, new: str) -> None:
		"""Set database table/collection"""
		assert len(new) > 0, 'table name cannot be zero-sized'
		assert is_alphanum_(new), f'Illegal characters in the table name: {new}'
		self._T[table_type] = new


	#----------------------------------------
	# accounts
	#----------------------------------------
	
	@abstractmethod
	async def account_insert(self, account: BSAccount) -> bool:
		"""Store account to the backend. Returns False 
			if the account was not added"""
		raise NotImplementedError
	

	@abstractmethod
	async def account_get(self, account_id: int) -> BSAccount | None:
		"""Get account from backend"""
		raise NotImplementedError


	@abstractmethod
	async def account_update(self, account: BSAccount, upsert: bool = True) -> bool:
		"""Update an account in the backend. Returns False 
			if the account was not updated"""
		raise NotImplementedError
	

	@abstractmethod
	async def account_delete(self, account: BSAccount) -> bool:
		"""Delete account from the backend. Returns False 
			if the account was not found/deleted"""
		raise NotImplementedError


	@abstractmethod
	async def accounts_get(self, stats_type : StatsTypes | None = None, 
							regions: set[Region] = Region.API_regions(), 
							inactive : OptAccountsInactive = OptAccountsInactive.default(), 
							disabled: bool = False, 
							dist : OptAccountsDistributed | None = None, 
							sample : float = 0, 
							cache_valid: int | None = None ) -> AsyncGenerator[BSAccount, None]:
		"""Get accounts from backend"""
		raise NotImplementedError
		yield BSAccount(id=-1)
	

	@abstractmethod
	async def accounts_count(self, stats_type : StatsTypes | None = None, 
							regions: set[Region] = Region.API_regions(), 
							inactive : OptAccountsInactive = OptAccountsInactive.default(), 	
							disabled: bool = False, 
							dist : OptAccountsDistributed | None = None, sample : float = 0, 
							cache_valid: int | None = None ) -> int:
		"""Get number of accounts from backend"""
		raise NotImplementedError


	async def accounts_get_worker(self, accountQ : Queue[BSAccount], **getargs) -> EventCounter:
		debug('starting')
		stats : EventCounter = EventCounter('accounts')
		try:
			async for account in self.accounts_get(**getargs):
				await accountQ.put(account)
				stats.log('queued')		
		except CancelledError as err:
			debug(f'Cancelled')
		except Exception as err:
			error(f'{err}')
		return stats


	@abstractmethod
	async def accounts_insert(self, accounts: Iterable[BSAccount]) -> tuple[int, int]:
		"""Store accounts to the backend. Returns number of accounts inserted and not inserted"""
		raise NotImplementedError


	async def accounts_insert_worker(self, accountQ : Queue[BSAccount], force: bool = False) -> EventCounter:
		debug(f'starting, force={force}')
		stats : EventCounter = EventCounter('accounts insert')
		try:
			while True:
				account = await accountQ.get()
				try:
					if force:
						debug(f'Trying to upsert account_id={account.id} into {self.backend}.{self.table_accounts}')
						await self.account_update(account, upsert=True)
					else:
						debug(f'Trying to insert account_id={account.id} into {self.backend}.{self.table_accounts}')
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
	async def accounts_export(self, account_type: type[Account] = BSAccount, 
								sample : float = 0) -> AsyncGenerator[BSAccount, None]:
		"""import accounts"""
		raise NotImplementedError
		yield BSAccount()


	#----------------------------------------
	# Releases
	#----------------------------------------

	@abstractmethod
	async def release_insert(self, release: BSBlitzRelease) -> bool:
		"""Insert new release to the backend"""
		raise NotImplementedError


	@abstractmethod
	async def release_get(self, release : str) -> BSBlitzRelease | None:
		raise NotImplementedError


	@abstractmethod
	async def release_update(self, update: BSBlitzRelease, upsert=False) -> bool:
		"""Update an release in the backend. Returns False 
			if the release was not updated"""
		raise NotImplementedError

	
	@abstractmethod
	async def release_delete(self, release: BSBlitzRelease) -> bool:
		"""Delete a release from backend"""
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
	async def releases_get(self, release_match: str | None = None, 
							since : date | None = None, 
							first : BSBlitzRelease | None = None) -> AsyncGenerator[BSBlitzRelease, None]:
		raise NotImplementedError
		yield BSBlitzRelease()


	@abstractmethod	
	async def releases_export(self, release_type: type[BSBlitzRelease] = BSBlitzRelease, 
								sample: float = 0) -> AsyncGenerator[BSBlitzRelease, None]:
		"""Import releases"""
		raise NotImplementedError
		yield BSBlitzRelease()


	async def releases_insert_worker(self, releaseQ : Queue[BSBlitzRelease], force: bool = False) -> EventCounter:
		debug(f'starting, force={force}')
		stats : EventCounter = EventCounter('releases insert')
		try:
			while True:
				release = await releaseQ.get()
				try:
					if force:
						debug(f'Trying to upsert release={release.release} into {self.backend}.{self.table_releases}')
						await self.release_update(release, upsert=True)
					else:
						debug(f'Trying to insert release={release.release} into {self.backend}.{self.table_releases}')
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


	@abstractmethod
	async def replay_delete(self, replay_id: str | ObjectId) -> WoTBlitzReplayJSON | None:
		"""Delete replay from backend based on replayID"""
		raise NotImplementedError


	# replay fields that can be searched: protagonist, battle_start_timestamp, account_id, vehicle_tier
	@abstractmethod
	async def replays_get(self, protagonist: int | None = None,
							since: date | None = None,
							**summary_fields) -> AsyncGenerator[WoTBlitzReplayJSON, None]:
		"""Get replays from backed"""
		raise NotImplementedError
		yield WoTBlitzReplayJSON()


	@abstractmethod
	async def replays_count(self, protagonist: int | None = None,
							since: date | None = None,
							**summary_fields) -> int:
		"""Get replays from backed"""
		raise NotImplementedError
	

	@abstractmethod
	async def replays_insert(self, replays: Iterable[WoTBlitzReplayJSON]) -> tuple[int, int]:
		"""Store replays to the backend. Returns number of replays inserted and not inserted"""
		raise NotImplementedError	


	async def replay_insert_worker(self, replayQ : Queue[WoTBlitzReplayJSON], force: bool = False) -> EventCounter:
		debug(f'starting, force={force}')
		stats : EventCounter = EventCounter('replays insert')
		try:
			while True:
				replay = await replayQ.get()
				try:
					debug(f'Insertting replay={replay.id} into {self.backend}.{self.table_replays}')
					await self.replay_insert(replay)					
					stats.log('added')
				except Exception as err:
					debug(f'Error: {err}')
					stats.log('not added')
				finally:
					replayQ.task_done()
		except CancelledError as err:
			debug(f'Cancelled')
		except Exception as err:
			error(f'{err}')
		return stats
	

	async def replays_export(self, replay_type: type[WoTBlitzReplayJSON] = WoTBlitzReplayJSON) -> AsyncGenerator[WoTBlitzReplayJSON, None]:			
		"""Export replays from Mongo DB"""
		raise NotImplementedError
		yield WoTBlitzReplayJSON()


	#----------------------------------------
	# tank stats
	#----------------------------------------

	@abstractmethod
	async def tank_stat_insert(self, tank_stat: WGtankStat) -> bool:
		"""Store tank stats to the backend. Returns number of stats inserted and not inserted"""
		raise NotImplementedError


	@abstractmethod
	async def tank_stat_get(self, account: BSAccount, 
							tank_id: int | None, last_battle_time: int) -> WGtankStat | None:
		"""Store tank stats to the backend. Returns number of stats inserted and not inserted"""
		raise NotImplementedError


	@abstractmethod
	async def tank_stat_delete(self, account: BSAccount, 
								tank_id: int | None, last_battle_time: int) -> bool:
		"""Store tank stats to the backend. Returns number of stats inserted and not inserted"""
		raise NotImplementedError


	@abstractmethod
	async def tank_stats_insert(self, tank_stats: Iterable[WGtankStat]) -> tuple[int, int]:
		"""Store tank stats to the backend. Returns number of stats inserted and not inserted"""
		raise NotImplementedError


	@abstractmethod
	async def tank_stats_get(self, release: BSBlitzRelease | None = None,
							regions: set[Region] = Region.API_regions(), 
							sample : float = 0) -> AsyncGenerator[WGtankStat, None]:
		"""Return tank stats from the backend"""
		raise NotImplementedError


	@abstractmethod
	async def tank_stats_count(self, release: BSBlitzRelease | None = None,
							regions: set[Region] = Region.API_regions(), 
							sample : float = 0) -> int:
		"""Get number of tank-stats from backend"""
		raise NotImplementedError


	@abstractmethod
	async def tank_stats_update(self, tank_stats: list[WGtankStat], upsert: bool = False) -> tuple[int, int]:
		"""Update or upsert tank stats to the backend. Returns number of stats updated and not updated"""
		raise NotImplementedError


	@abstractmethod
	async def tank_stats_export(self, replay_type: type[WGtankStat] = WGtankStat
									) -> AsyncGenerator[WGtankStat, None]:
		"""Export tank stats from Mongo DB"""
		raise NotImplementedError
		yield WGtankStat()


	async def tank_stats_insert_worker(self, tank_statsQ : Queue[list[WGtankStat]], 
										force: bool = False) -> EventCounter:
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
						debug(f'Trying to upsert {read} tank stats into {self.backend}.{self.table_tank_stats}')
						added, not_added = await self.tank_stats_update(tank_stats, upsert=True)
						stats.log('tank stats added/updated', added)
					else:
						debug(f'Trying to insert {read} tank stats into {self.backend}.{self.table_tank_stats}')
						added, not_added = await self.tank_stats_insert(tank_stats)
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
	# ErrorLog
	#----------------------------------------

	@abstractmethod
	async def error_log(self, error: ErrorLog) -> bool:
		"""Log an error into the backend's ErrorLog"""
		raise NotImplementedError


	@abstractmethod
	async def errors_get(self, table_type: BSTableType | None = None, doc_id : Any | None = None, 
							after: datetime | None = None) -> AsyncGenerator[ErrorLog, None]:
		"""Return errors from backend ErrorLog"""
		raise NotImplementedError
		yield ErrorLog(table='foo', error='bar')
	

	@abstractmethod
	async def errors_clear(self, table_type: BSTableType, doc_id : Any | None = None, 
							after: datetime | None = None) -> int:
		"""Clear errors from backend ErrorLog"""
		raise NotImplementedError
		