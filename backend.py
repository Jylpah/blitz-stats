from configparser import ConfigParser
from argparse import Namespace
import logging
from abc import ABCMeta, abstractmethod
from collections.abc import AsyncGenerator
from bson import ObjectId
from models import Account, Region, WoTBlitzReplayJSON
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCursor, AsyncIOMotorCollection # type: ignore
from os.path import isfile
from typing import Optional

logger	= logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug


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
	async def account_get(self, account_id: int) -> AsyncGenerator[Account, None]:
		"""Get account from backend"""
		raise NotImplementedError

	@abstractmethod
	async def accounts_get(self, region: Region |None = None, inactive : bool = False, disable: bool =False ) -> AsyncGenerator[Account, None]:
		"""Get account from backend"""
		raise NotImplementedError



class MongoBackend(Backend):

	def __init__(self, config: ConfigParser | None = None, database: str = 'default', *args, **kwargs):
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

	async def account_get(self, account_id: int) -> AsyncGenerator[Account, None]:
		"""Get account from backend"""
		raise NotImplementedError


	async def accounts_get(self, region: Region |None = None, inactive : bool = False, disable: bool =False ) -> AsyncGenerator[Account, None]:
		"""Get account from backend"""
		raise NotImplementedError
