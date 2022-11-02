from configparser import ConfigParser
from argparse import Namespace
import logging
from abc import ABCMeta, abstractmethod
from collections.abc import AsyncGenerator
from bson import ObjectId
from models import Account, Region, WoTBlitzReplayJSON
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCursor, AsyncIOMotorCollection # type: ignore
from os.path import isfile
from typing import Optional, Any
from time import time

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
MAX_RETRIES : int = 3
CACHE_VALID : int = 5   # days

class Backend(metaclass=ABCMeta):
	"""Abstract class for a backend (mongo, postgres, files)"""
	# def __init__(self, parser: Namespace, config: ConfigParser | None = None):
	# 	try:
	# 		if config is not None and 'BACKEND' in config.sections():

	# 	except Exception as err:
	# 		error(str(err))

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


	@abstractmethod
	async def replay_store(self, replay: WoTBlitzReplayJSON) -> bool:
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

	@abstractmethod
	async def account_get(self, account_id: int) -> Account | None:
		"""Get account from backend"""
		raise NotImplementedError

	@abstractmethod
	async def accounts_get(self, stats_type : str | None = None, region: Region | None = None, 
							inactive : bool | None = False, disabled: bool = False, sample : float = 0, 
							force : bool = False, cache_valid: int = CACHE_VALID ) -> AsyncGenerator[Account, None]:
		"""Get account from backend"""
		raise NotImplementedError
		yield Account(id=-1)



class MongoBackend(Backend):

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


	async def replay_store(self, replay: WoTBlitzReplayJSON) -> bool:
		"""Store replay into backend"""
		raise NotImplementedError


	async def replay_get(self, replay_id: str | ObjectId) -> WoTBlitzReplayJSON | None:
		"""Get a replay from backend based on replayID"""
		try:
			DBC : str = self.C['REPLAYS']
			dbc : AsyncIOMotorCollection = self.db[DBC]
			return WoTBlitzReplayJSON.parse_obj(await dbc.find_one({'_id': str(replay_id)}))
		except Exception as err:
			error(f'Error fetching replay (id_: {replay_id}) from Mongo DB: {str(err)}')	
		return None
		


	# replay fields that can be searched: protagonist, battle_start_timestamp, account_id, vehicle_tier
	async def replay_find(self, **kwargs) -> AsyncGenerator[WoTBlitzReplayJSON, None]:
		"""Find a replay from backend based on search string"""
		raise NotImplementedError

	
	async def account_get(self, account_id: int) -> Account | None:
		"""Get account from backend"""
		try:
			DBC : str = self.C['ACCOUNTS']
			dbc : AsyncIOMotorCollection = self.db[DBC]
			return Account.parse_obj(await dbc.find_one({'_id': account_id}))
		except Exception as err:
			error(f'Error fetching account_id: {account_id}) from Mongo DB: {str(err)}')	
		return None


	async def accounts_get(self, stats_type : str | None = None, region: Region | None = None, 
							inactive : bool | None = False, disabled: bool = False, sample : float = 0, 
							force : bool = False, cache_valid: int = CACHE_VALID ) -> AsyncGenerator[Account, None]:
		"""Get accounts from Mongo DB
			inactive: true = only inactive, false = not inactive, none = AUTO
		"""
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
			
			update_field : str | None = Account.get_update_field(stats_type)

			dbc : AsyncIOMotorCollection = self.db[DBC]
			match : list[dict[str, str|int|float|dict|list]] = list()
			
			match.append({ '_id' : {  '$lt' : WG_ACCOUNT_ID_MAX}})  # exclude Chinese account ids

			if region is not None:
				match.append({ 'r' : region.name })
	
			if disabled:
				match.append({ 'd': True })
			else:
				match.append({ 'd': { '$ne': True }})
				if inactive is None:
					if not force:
						assert update_field is not None, "automatic inactivity detection requires stat_type"
						match.append({ '$or': [ { update_field: None}, { update_field: { '$lt': NOW - cache_valid }} ] })
				elif inactive:
					match.append({ 'i': True })
				else:
					match.append({ 'i': { '$ne': True }})
			
						

			pipeline : list[dict[str, Any]] = [ { '$match' : { '$and' : match } }]

			if sample >= 1:				
				pipeline.append({'$sample': {'size' : int(sample) } })
			elif sample > 0:
				n = await dbc.estimated_document_count()
				pipeline.append({'$sample': {'size' : int(n * sample) } })

			cursor : AsyncIOMotorCursor = dbc.aggregate(pipeline, allowDiskUse=False)

			async for account_obj in cursor:
				try:
					player = Account.parse_obj(account_obj)
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
