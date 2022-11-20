from configparser import ConfigParser
from argparse import Namespace
import logging
from abc import ABCMeta, abstractmethod
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCursor, AsyncIOMotorCollection # type: ignore
from pymongo.results import InsertManyResult
from pymongo.errors import BulkWriteError
from os.path import isfile
from typing import Optional, Any, Iterable, AsyncGenerator
from time import time
from enum import Enum
from asyncio import Queue, CancelledError

from models import BSAccount, StatsTypes
from blitzutils.models import Region, WoTBlitzReplayJSON, WGtankStat
from pyutils.utils import epoch_now
from pyutils.eventcounter import EventCounter

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
ACCOUNTS_Q_MAX 		: int = 500

class OptAccountsInactive(str, Enum):
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
	def create(cls, input: str) -> Optional['OptAccountsDistributed']:
		try:
			res : list[str] = input.split(':')
			if len(res) != 2:
				raise ValueError(f'Input ({input} does not match format "I:N")')
			mod : int = int(res[0])
			div : int = int(res[1])
			return OptAccountsDistributed(mod, div)
		except Exception as err:
			error(str(err))
		return None


	def match(self, value : int) -> bool:
		assert type(value) is int, "value has to be integere"
		return value % self.div == self.mod


class Backend(metaclass=ABCMeta):
	"""Abstract class for a backend (mongo, postgres, files)"""
	# def __init__(self, parser: Namespace, config: ConfigParser | None = None):
	# 	try:
	# 		if config is not None and 'BACKEND' in config.sections():

	# 	except Exception as err:
	# 		error(str(err))
	name : str = 'Backend'


	@classmethod
	async def create(cls, backend : str, config : ConfigParser | None) -> Optional['Backend']:
		try:
			if backend == 'mongodb':
				return MongoBackend(config)
			else:				
				assert False, f'Backend not implemented: {backend}'
		except Exception as err:
			error(f'Error creating backend {backend}: {str(err)}')
		return None		


	@classmethod
	def list_available(cls) -> list[str]:
		return ['mongodb']


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
							force : bool = False, cache_valid: int = CACHE_VALID ) -> AsyncGenerator[BSAccount, None]:
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
			error(str(err))
		return stats
	

	@abstractmethod
	async def accounts_count(self, stats_type : StatsTypes | None = None, 
							regions: set[Region] = Region.API_regions(), 
							inactive : OptAccountsInactive = OptAccountsInactive.default(), 	
							disabled: bool = False, 
							dist : OptAccountsDistributed | None = None, sample : float = 0, 
							force : bool = False, cache_valid: int = CACHE_VALID ) -> int:
		"""Get number of accounts from backend"""
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


	@abstractmethod
	async def tank_stats_insert(self, tank_stats: Iterable[WGtankStat]) -> tuple[int, int]:
		"""Store tank stats to the backend. Returns number of stats inserted and not inserted"""
		raise NotImplementedError


	@abstractmethod
	async def tank_stats_get(self, account: BSAccount, tank_id: int | None = None, 
								last_battle_time: int | None = None) -> AsyncGenerator[WGtankStat, None]:
		"""Return tank stats from the backend"""
		raise NotImplementedError


##############################################
#
## class MongoBackend(Backend)
#
##############################################

class MongoBackend(Backend):

	name : str = 'mongodb'

	def __init__(self, config: ConfigParser | None = None, database: str = 'BlitzStats', *args, **kwargs):
		try:
			client : AsyncIOMotorClient | None = None
			self.db : AsyncIOMotorDatabase
			self.C : dict[str,str] = dict()

			self.C['ACCOUNTS'] 		= 'Accounts'
			self.C['TANKOPEDIA'] 	= 'Tankopedia'
			self.C['REPLAYS'] 		= 'Replays'
			self.C['TANK_STATS'] 	= 'TankStats'
			self.C['PLAYER_ACHIEVEMENTS'] = 'PlayerAchievements'

			# defaults
			SERVER 	: str 	= 'localhost'
			PORT 	: int  	= 27017
			TLS 	: bool	= False
			INVALID_CERT	: bool = False
			INVALID_HOST	: bool = False
			AUTHDB			: str | None = None
			USER 			: str | None = None
			PASSWD 			: str | None = None
			CERT			: str | None = None
			CA				: str | None = None			

			if config is not None and 'MONGO' in config.sections():
				configMongo = config['MONGO']				
				SERVER 		= configMongo.get('server', SERVER)
				PORT 		= configMongo.getint('port', PORT)
				database	= configMongo.get('database', database)
				TLS 		= configMongo.getboolean('tls', TLS)
				INVALID_CERT= configMongo.getboolean('tls_invalid_certs', INVALID_CERT)
				INVALID_HOST= configMongo.getboolean('tls_invalid_hosts', INVALID_HOST)
				AUTHDB		= configMongo.get('auth_db', AUTHDB)
				USER 		= configMongo.get('user', USER)
				PASSWD 		= configMongo.get('password', PASSWD)
				CERT		= configMongo.get('cert', CERT)
				CA			= configMongo.get('ca', CA)
				self.C['ACCOUNTS'] 		= configMongo.get('c_accounts', self.C['ACCOUNTS'])
				self.C['TANKOPEDIA'] 	= configMongo.get('c_tankopedia', self.C['TANKOPEDIA'])
				self.C['REPLAYS'] 		= configMongo.get('c_replays', self.C['REPLAYS'])
				self.C['TANK_STATS']	= configMongo.get('c_tank_stats', self.C['TANK_STATS'])
				self.C['PLAYER_ACHIEVEMENTS'] 	= configMongo.get('c_player_achievements', self.C['PLAYER_ACHIEVEMENTS'])					
			else:					
				debug(f'"MONGO" section not found from config file')
		
			if USER is None:
				client =  AsyncIOMotorClient(host=SERVER,port=PORT, tls=TLS, 
											tlsAllowInvalidCertificates=INVALID_CERT, 
											tlsAllowInvalidHostnames=INVALID_HOST,
											tlsCertificateKeyFile=CERT, tlsCAFile=CA, *args, **kwargs)
			else:
				client =  AsyncIOMotorClient(host=SERVER,port=PORT, tls=TLS, 
											tlsAllowInvalidCertificates=INVALID_CERT, 
											tlsAllowInvalidHostnames=INVALID_HOST,
											tlsCertificateKeyFile=CERT, tlsCAFile=CA, 
											authSource=AUTHDB, username=USER, password=PASSWD,  *args, **kwargs)
			
			assert client is not None, "Failed to initialize Mongo DB connection"			
			self.db = client[database]
	
			debug('Mongo DB connection succeeded')
		except FileNotFoundError as err:
			error(str(err))
		except Exception as err:
			error(f'Error connecting Mongo DB: {str(err)}')


	async def replay_insert(self, replay: WoTBlitzReplayJSON) -> bool:
		"""Store replay into backend"""
		try:
			DBC : str = self.C['REPLAYS']
			dbc : AsyncIOMotorCollection = self.db[DBC]
			await dbc.insert_one(replay.export_db())
			return True
		except Exception as err:
			debug(f'Could not insert replay (_id: {replay.id}) into {self.name}: {str(err)}')	
		return False


	async def replay_get(self, replay_id: str | ObjectId) -> WoTBlitzReplayJSON | None:
		"""Get a replay from backend based on replayID"""
		try:
			debug(f'Getting replay (id={replay_id} from {self.name})')
			DBC : str = self.C['REPLAYS']
			dbc : AsyncIOMotorCollection = self.db[DBC]
			res : Any | None = await dbc.find_one({'_id': str(replay_id)})
			if res is not None:
				# replay : WoTBlitzReplayJSON  = WoTBlitzReplayJSON.parse_obj(res) 
				# debug(replay.json_src())
				return WoTBlitzReplayJSON.parse_obj(res)   # returns None if not found
		except Exception as err:
			debug(f'Error reading replay (id_: {replay_id}) from {self.name}: {str(err)}')	
		return None
	

	# replay fields that can be searched: protagonist, battle_start_timestamp, account_id, vehicle_tier
	async def replay_find(self, **kwargs) -> AsyncGenerator[WoTBlitzReplayJSON, None]:
		"""Find a replay from backend based on search string"""
		raise NotImplementedError

	
	async def account_get(self, account_id: int) -> BSAccount | None:
		"""Get account from backend"""
		try:
			DBC : str = self.C['ACCOUNTS']
			dbc : AsyncIOMotorCollection = self.db[DBC]
			return BSAccount.parse_obj(await dbc.find_one({'_id': account_id}))
		except Exception as err:
			error(f'Error fetching account_id: {account_id}) from {self.name}: {str(err)}')	
		return None


	async def accounts_count(self, stats_type : StatsTypes | None = None, 
							regions: set[Region] = Region.API_regions(), 
							inactive : OptAccountsInactive = OptAccountsInactive.default(), 	
							disabled: bool = False, 
							dist : OptAccountsDistributed | None = None, sample : float = 0, 
							force : bool = False, cache_valid: int = CACHE_VALID ) -> int:
		try:
			NOW = int(time())	
			DBC : str = self.C['ACCOUNTS']
			dbc : AsyncIOMotorCollection = self.db[DBC]
			pipeline : list[dict[str, Any]] | None = await self._accounts_mk_pipeline(stats_type=stats_type, regions=regions, 
																	inactive=inactive, disabled=disabled, 
																	dist=dist, sample=sample, 
																	force=force, cache_valid=cache_valid)

			if pipeline is None:
				raise ValueError(f'could not create get-accounts {self.name} cursor')
			pipeline.append({ '$count': 'accounts' })
			cursor : AsyncIOMotorCursor = dbc.aggregate(pipeline, allowDiskUse=False)
			res : Any =  (await cursor.to_list(length=100))[0]
			if type(res) is dict and 'accounts' in res:
				return int(res['accounts'])
			else:
				raise ValueError('pipeline returned malformed data')
		except Exception as err:
			error(f'counting accounts failed: {err}')
		return -1


	async def accounts_get(self, stats_type : StatsTypes | None = None, 
							regions: set[Region] = Region.API_regions(), 
							inactive : OptAccountsInactive = OptAccountsInactive.default(), 	
							disabled: bool = False, 
							dist : OptAccountsDistributed | None = None, sample : float = 0, 
							force : bool = False, cache_valid: int = CACHE_VALID ) -> AsyncGenerator[BSAccount, None]:
		"""Get accounts from Mongo DB
			inactive: true = only inactive, false = not inactive, none = AUTO
		"""
		try:
			NOW = int(time())	
			DBC : str = self.C['ACCOUNTS']
			dbc : AsyncIOMotorCollection = self.db[DBC]
			pipeline : list[dict[str, Any]] | None = await self._accounts_mk_pipeline(stats_type=stats_type, regions=regions, 
																	inactive=inactive, disabled=disabled, 
																	dist=dist, sample=sample, 
																	force=force, cache_valid=cache_valid)

			update_field : str | None = None
			if stats_type is not None:
				update_field = stats_type.value

			if pipeline is None:
				raise ValueError(f'could not create get-accounts {self.name} cursor')
			cursor : AsyncIOMotorCursor = dbc.aggregate(pipeline)
			
			async for account_obj in cursor:
				try:
					player = BSAccount.parse_obj(account_obj)
					if not force and not disabled and inactive is None and player.inactive:
						assert update_field is not None, "automatic inactivity detection requires stat_type"
						updated = dict(player)[update_field]
						if (NOW - updated) < min(MAX_UPDATE_INTERVAL, (updated - player.last_battle_time)/2):
							continue
					yield player
				except Exception as err:
					error(str(err))
					continue
		except Exception as err:
			error(f'Error fetching accounts from Mongo DB: {str(err)}')	


	async def _accounts_mk_pipeline(self, stats_type : StatsTypes | None = None, 
							regions: set[Region] = Region.API_regions(), 
							inactive : OptAccountsInactive = OptAccountsInactive.default(), 
							dist : OptAccountsDistributed | None = None,
							disabled: bool = False, sample : float = 0, 
							force : bool = False, cache_valid: int = CACHE_VALID ) -> list[dict[str, Any]] | None:
		try:
			# id						: int		= Field(default=..., alias='_id')
			# region 					: Region | None= Field(default=None, alias='r')
			# last_battle_time			: int | None = Field(default=None, alias='l')
			# updated_tank_stats 		: int | None = Field(default=None, alias='ut')
			# updated_player_achievements : int | None = Field(default=None, alias='up')
			# added 					: int | None = Field(default=None, alias='a')
			# inactive					: bool | None = Field(default=None, alias='i')
			# disabled					: bool | None = Field(default=None, alias='d')
			
			NOW = int(time())			
			DBC : str = self.C['ACCOUNTS']
			update_field : str | None = None
			if stats_type is not None:
				update_field = stats_type.value
			dbc : AsyncIOMotorCollection = self.db[DBC]
			match : list[dict[str, str|int|float|dict|list]] = list()
			
			match.append({ '_id' : {  '$lt' : WG_ACCOUNT_ID_MAX}})  # exclude Chinese account ids
			
			if dist is not None:
				match.append({ '_id' : {  '$mod' :  [ dist.div, dist.mod ]}})
			
			match.append({ 'r' : { '$in' : [ r.value for r in regions ]} })
	
			if disabled:
				match.append({ 'd': True })
			else:
				match.append({ 'd': { '$ne': True }})
				# check inactive only if disabled == False
				if inactive == OptAccountsInactive.auto:
					if not force:
						assert update_field is not None, "automatic inactivity detection requires stat_type"
						match.append({ '$or': [ { update_field: None}, { update_field: { '$lt': NOW - cache_valid }} ] })
				elif inactive == OptAccountsInactive.yes:
					match.append({ 'i': True })
				elif inactive == OptAccountsInactive.no:
					match.append({ 'i': { '$ne': True }})
				else:
					# do not add a filter in case both inactive and active players are included
					pass						

			pipeline : list[dict[str, Any]] = [ { '$match' : { '$and' : match } }]

			if sample >= 1:				
				pipeline.append({'$sample': {'size' : int(sample) } })
			elif sample > 0:
				n = await dbc.estimated_document_count()
				pipeline.append({'$sample': {'size' : int(n * sample) } })
			return pipeline		
		except Exception as err:
			error(str(err))
		return None

	async def account_insert(self, account: BSAccount) -> bool:
		"""Store account to the backend. Returns False 
			if the account was not added"""
		try:
			DBC : str = self.C['ACCOUNTS']
			dbc : AsyncIOMotorCollection = self.db[DBC]
			account.added = epoch_now()
			await dbc.insert_one(account.json_obj('db'))
			debug(f'Account add to {self.name}: {account.id}')
			return True			
		except Exception as err:
			debug(f'Failed to add account_id={account.id} to {self.name}: {str(err)}')	
		return False
	

	async def accounts_insert(self, accounts: Iterable[BSAccount]) -> tuple[int, int]:
		"""Store account to the backend. Returns False 
			if the account was not added"""
		added		: int = 0
		not_added 	: int = 0
		try:
			DBC : str = self.C['ACCOUNTS']
			dbc : AsyncIOMotorCollection = self.db[DBC]
			res : InsertManyResult
			
			for account in accounts:
				# modifying Iterable items is OK since the item object ref stays the sam
				account.added = epoch_now()   

			res = await dbc.insert_many( (account.json_obj('db') for account in accounts), 
										  ordered=False)
			added = len(res.inserted_ids)
		except BulkWriteError as err:
			if err.details is not None:
				added = err.details['nInserted']
				not_added = len(err.details["writeErrors"])
				debug(f'Added {added}, could not add {not_added} accounts')
			else:
				error('BulkWriteError.details is None')
		except Exception as err:
			error(f'Unknown error when adding acconts: {str(err)}')
		return added, not_added

	
	async def tank_stats_insert(self, tank_stats: Iterable[WGtankStat]) -> tuple[int, int]:
		"""Store tank stats to the backend. Returns number of stats inserted and not inserted"""
		added		: int = 0
		not_added 	: int = 0
		try:
			DBC : str = self.C['TANK_STATS']
			dbc : AsyncIOMotorCollection = self.db[DBC]
			res : InsertManyResult
			
			res = await dbc.insert_many( (tank_stat.export_db() for tank_stat in tank_stats), 
										  ordered=False)
			added = len(res.inserted_ids)
		except BulkWriteError as err:
			if err.details is not None:
				added = err.details['nInserted']
				not_added = len(err.details["writeErrors"])
				debug(f'Added {added}, could not add {not_added} tank stats')
			else:
				error('BulkWriteError.details is None')
		except Exception as err:
			error(f'Unknown error when adding tank stats: {str(err)}')
		return added, not_added


	async def tank_stats_get(self, account: BSAccount, tank_id: int | None = None, last_battle_time: int | None = None) -> AsyncGenerator[WGtankStat, None]:
		"""Return tank stats from the backend"""
		raise NotImplementedError