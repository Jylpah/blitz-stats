from configparser import ConfigParser
from argparse import Namespace, ArgumentParser
from datetime import date, datetime
from os.path import isfile
from typing import Optional, Any, Iterable, Sequence, Union, Tuple, Literal, Final, \
					AsyncGenerator, TypeVar, ClassVar, cast, Generic, Callable
import random
import logging
import re

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCursor, AsyncIOMotorCollection  # type: ignore
from pymongo.results import InsertManyResult, InsertOneResult, UpdateResult, DeleteResult
from pymongo.errors import BulkWriteError, CollectionInvalid, ConnectionFailure
from pydantic import BaseModel, ValidationError, Field
from asyncstdlib import enumerate

from backend import Backend, OptAccountsDistributed, OptAccountsInactive, BSTableType, \
					MAX_UPDATE_INTERVAL, WG_ACCOUNT_ID_MAX, MIN_UPDATE_INTERVAL, ErrorLog, \
					ErrorLogType, A
from models import BSAccount, BSBlitzRelease, StatsTypes
from pyutils import epoch_now, JSONExportable, AliasMapper, I, D, O, Idx, \
	BackendIndexType, BackendIndex, DESCENDING, ASCENDING, TEXT
# from pyutils.utils import transform_objs
from blitzutils.models import Region, Account, Tank, WoTBlitzReplayJSON, WoTBlitzReplayData, \
								WoTBlitzReplaySummary, WGTankStat, WGBlitzRelease, \
								WGPlayerAchievementsMaxSeries, WoTBlitzTankString,\
								EnumNation, EnumVehicleTier, EnumVehicleTypeStr

# Setup logging
logger	= logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug

# Constants
TANK_STATS_BATCH	: int = 1000
MONGO_BATCH_SIZE	: int = 1000

class MongoErrorLog(ErrorLog):
	doc_id : ObjectId | int | str | None	= Field(default=None, alias='did')

	class Config:
		arbitrary_types_allowed = True
		allow_mutation 			= True
		validate_assignment 	= True
		allow_population_by_field_name = True
		json_encoders = { ObjectId: str }


##############################################
#
## class MongoBackend(Backend)
#
##############################################

# I = TypeVar('I', str, int, ObjectId)
# D = TypeVar('D', bound=JSONExportable)
# O = TypeVar('O', bound=JSONExportable)

MongoIndexAscDesc 	= BackendIndexType
MongoIndex 			= BackendIndex

class MongoBackend(Backend):

	driver : str = 'mongodb'
	# default_db : str = 'BlitzStats'

	def __init__(self,
				config		: ConfigParser | None = None,
				db_config 	: dict[str, Any] | None = None,
				database 	: str | None = None,
				table_config: dict[BSTableType, str] | None = None,
				model_config: dict[BSTableType, type[JSONExportable]] | None = None,
				**kwargs):
		"""Init MongoDB backend from config file and CLI args
			CLI arguments overide settings in the config file"""

		debug('starting')
		try:
			super().__init__(config=config,
							db_config=db_config,
							database=database,
							**kwargs)

			mongodb_rc 		: dict[str, Any] = dict()
			self._client 	: AsyncIOMotorClient
			self.db 		: AsyncIOMotorDatabase

			# server defaults
			mongodb_rc['host'] 						= 'localhost'
			mongodb_rc['port'] 						= 27017
			mongodb_rc['tls']						= False
			mongodb_rc['tlsAllowInvalidCertificates']= False
			mongodb_rc['tlsAllowInvalidHostnames']	= False
			mongodb_rc['tlsCertificateKeyFile']		= None
			mongodb_rc['tlsCAFile']					= None
			mongodb_rc['authSource']				= None
			mongodb_rc['username']					= None
			mongodb_rc['password']					= None

			if config is not None and 'MONGODB' in config.sections():
				configMongo = config['MONGODB']
				self._database		= configMongo.get('database', self.database)
				mongodb_rc['host'] 	= configMongo.get('server', mongodb_rc['host'])
				mongodb_rc['port'] 	= configMongo.getint('port', mongodb_rc['port'])
				mongodb_rc['tls'] 	= configMongo.getboolean('tls', mongodb_rc['tls'])
				mongodb_rc['tlsAllowInvalidCertificates']	= configMongo.getboolean('tls_invalid_certs',
																	mongodb_rc['tlsAllowInvalidCertificates'])
				mongodb_rc['tlsAllowInvalidHostnames']	= configMongo.getboolean('tls_invalid_hosts',
																		mongodb_rc['tlsAllowInvalidHostnames'])
				mongodb_rc['tlsCertificateKeyFile']	= configMongo.get('cert', mongodb_rc['tlsCertificateKeyFile'])
				mongodb_rc['tlsCAFile']				= configMongo.get('ca', mongodb_rc['tlsCAFile'])
				mongodb_rc['authSource']			= configMongo.get('auth_db', mongodb_rc['authSource'])
				mongodb_rc['username']				= configMongo.get('user', mongodb_rc['username'])
				mongodb_rc['password']				= configMongo.get('password', mongodb_rc['password'])

				self.set_table(BSTableType.Accounts, 	configMongo.get('t_accounts'))
				self.set_table(BSTableType.Tankopedia, 	configMongo.get('t_tankopedia'))
				self.set_table(BSTableType.TankStrings, configMongo.get('t_tank_strings'))
				self.set_table(BSTableType.Releases, 	configMongo.get('t_releases'))
				self.set_table(BSTableType.Replays, 	configMongo.get('t_replays'))
				self.set_table(BSTableType.TankStats, 	configMongo.get('t_tank_stats'))
				self.set_table(BSTableType.PlayerAchievements, configMongo.get('t_player_achievements'))
				self.set_table(BSTableType.AccountLog, 	configMongo.get('t_account_log'))
				self.set_table(BSTableType.ErrorLog,	configMongo.get('t_error_log'))

				self.set_model(BSTableType.Accounts, 	configMongo.get('m_accounts'))
				self.set_model(BSTableType.Tankopedia, 	configMongo.get('m_tankopedia'))
				self.set_model(BSTableType.TankStrings, configMongo.get('m_tank_strings'))
				self.set_model(BSTableType.Releases, 	configMongo.get('m_releases'))
				self.set_model(BSTableType.Replays, 	configMongo.get('m_replays'))
				self.set_model(BSTableType.TankStats, 	configMongo.get('m_tank_stats'))
				self.set_model(BSTableType.PlayerAchievements, configMongo.get('m_player_achievements'))
				self.set_model(BSTableType.AccountLog, 	configMongo.get('m_account_log'))
				self.set_model(BSTableType.ErrorLog,	configMongo.get('m_error_log'))

			if db_config is not None:
				kwargs = db_config | kwargs
			kwargs = mongodb_rc | kwargs
			kwargs = {k: v for k, v in kwargs.items() if v is not None} 	# remove unset kwargs

			self.set_database(database)
			self._client 	= AsyncIOMotorClient(**kwargs)
			debug(f'{self._client}')
			self.db 	 	= self._client[self.database]
			self._db_config = kwargs
			self.config_tables(table_config=table_config)
			self.config_models(model_config=model_config)

			# debug(f'Mongo DB: {self.backend}')
			debug(f'config: ' + ', '.join([ "{0}={1}".format(k, str(v)) for k, v in kwargs.items()]))
		except FileNotFoundError as err:
			error(f'{err}')
			raise err
		except Exception as err:
			error(f'Error connecting Mongo DB: {err}')
			raise err


	def debug(self) -> None:
		"""Print out debug info"""
		print(f'###### DEBUG {self.driver} ######')
		print(f'DB Client: {self._client}')


	def copy(self, **kwargs) -> Optional['Backend']:
		"""Create a copy of the backend"""
		try:
			debug('starting')

			database : str = self.database
			if 'database' in kwargs.keys():
				database = kwargs['database']
				del kwargs['database']

			return MongoBackend(config=None,
								db_config=self.db_config,
								database=database,
								table_config=self.table_config,
								model_config=self.model_config,
								**kwargs)
		except Exception as err:
			error(f'Error creating copy: {err}')
		return None


	def reconnect(self) -> bool:
		"""Reconnect backend"""
		try:
			self._client = AsyncIOMotorClient(**self._db_config)
			self.db 	 = self._client[self.database]
			return True
		except Exception as err:
			error(f'Error connection: {self.backend}')
		return False


	async def test(self) -> bool:
		try:
			debug(f'trying to connect: {self.backend}')
			# The ping command is cheap and does not require auth.
			await self.db.command('ping')
			# await self._client.server_info()
			debug(f'connection succeeded: {self.backend}')
			return True
		except ConnectionFailure:
			error(f'Server not available: {self.backend}')
		except Exception as err:
			error(f'Error connection: {self.backend}')
		return False


	def get_collection(self, table_type: BSTableType) -> AsyncIOMotorCollection:
		return self.db[self.get_table(table_type)]


	@property
	def collection_accounts(self) -> AsyncIOMotorCollection:
		return self.get_collection(BSTableType.Accounts)

	@property
	def collection_releases(self) -> AsyncIOMotorCollection:
		return self.get_collection(BSTableType.Releases)

	@property
	def collection_replays(self) -> AsyncIOMotorCollection:
		return self.get_collection(BSTableType.Replays)

	@property
	def collection_tankopedia(self) -> AsyncIOMotorCollection:
		return self.get_collection(BSTableType.Tankopedia)

	@property
	def collection_tank_strings(self) -> AsyncIOMotorCollection:
		return self.get_collection(BSTableType.TankStrings)

	@property
	def collection_player_achievements(self) -> AsyncIOMotorCollection:
		return self.get_collection(BSTableType.PlayerAchievements)

	@property
	def collection_tank_stats(self) -> AsyncIOMotorCollection:
		return self.get_collection(BSTableType.TankStats)

	@property
	def collection_error_log(self) -> AsyncIOMotorCollection:
		return self.get_collection(BSTableType.ErrorLog)

	@property
	def collection_account_log(self) -> AsyncIOMotorCollection:
		return self.get_collection(BSTableType.AccountLog)


	async def _create_index(self, table_type: BSTableType,
							mapper 	: AliasMapper,
							index	: Sequence[MongoIndex],
							db_fields: list[str] | None = None ) -> bool:
		"""Helper to create index to a collection """
		try:
			debug(f'starting: collection={self.get_table(table_type)}')
			dbc 		: AsyncIOMotorCollection = self.get_collection(table_type)
			index_str 	: list[str] = list()
			field 		: Final = 0
			direction 	: Final = 1
			for idx_elem in index:
				index_str.append(f'{idx_elem[field]}: {idx_elem[direction]}')
			message(f"Adding index: {', '.join(index_str)}")

			db_index : list[MongoIndex]
			if db_fields is None:
				db_index = list(mapper.map(index).items())
			else:
				db_index = list()
				for i in range(len(index)):
					db_index.append( (db_fields[i], index[i][direction]) )
			await dbc.create_index(db_index, background=True)
			return True
		except Exception as err:
			error(f'{err}')
		return False


	@classmethod
	def add_args_import(cls, parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
		"""Add argument parser for import backend"""
		try:
			debug('starting')
			super().add_args_import(parser=parser, config=config)

			parser.add_argument('--server-url', metavar='URL', type=str, default=None, dest='import_host',
										help='Server URL to connect to. Required if the imported other than the current backend')
			parser.add_argument('--database', metavar='DATABASE', type=str, default=None, dest='import_database',
										help='Database to use. Uses current database as default')
			parser.add_argument('--collection', metavar='COLLECTION', type=str, default=None, dest='import_table',
										help='Collection/table to import from. Uses current database as default')
			return True
		except Exception as err:
			error(f'{err}')
		return False


	@classmethod
	def read_args(cls, args : Namespace,
					driver: str,
					importdb: bool = False) -> dict[str, Any]:
		debug('starting')
		if driver != cls.driver:
			raise ValueError(f'calling {cls}.read_args() for {driver} backend')
		kwargs : dict[str, Any] = Backend.read_args_helper(args, ['host', 'database'], importdb=importdb)
		kwargs = { k:v for k, v in kwargs.items() if v is not None }
		debug(f'args={kwargs}')
		return kwargs


	@property
	def backend(self: 'MongoBackend') -> str:
		host : str = 'UNKNOWN'
		try:
			host, port = self._client.address
			return f'{self.driver}://{host}:{port}/{self.database}'
		except Exception as err:
			debug(f'Error determing host: {err}')
		return f'{self.driver}://{host}/{self.database}'


	def __eq__(self, __o: object) -> bool:
		return __o is not None and isinstance(__o, MongoBackend) and \
					self._client.address == __o._client.address and \
					self.database == __o.database


	async def init_collection(self,
							  table_type: BSTableType,
							  indexes: list[list[BackendIndex]] | None = None ) -> bool:
		"""Helper to create index to a collection"""
		debug('starting')
		try:
			DBC		: str 					= self.get_table(table_type)
			model 	: type[JSONExportable]	= self.get_model(table_type)
			mapper 	: AliasMapper 			= AliasMapper(model)

			if indexes is None:
				indexes = model.backend_indexes()

			try:
				await self.db.create_collection(DBC)
				message(f'Collection created: {DBC}')
			except CollectionInvalid:
				message(f'Collection exists: {DBC}')

			if len(indexes) == 0:
				print(f'No indexes defined for {self.table_uri(table_type)}')
			for index in indexes:
				await self._create_index(table_type, mapper, index)
			return True
		except Exception as err:
			error(f'{err}')
		return False


	async def init(self, tables: list[str] = [ tt.name for tt in BSTableType]) -> bool:   # type: ignore
		"""Init MongoDB backend: create tables and set indexes"""
		try:
			debug('starting')
			DBC 	: str = 'NOT SET'
			indexes : list[list[MongoIndex]]

			for table in tables:
				try:
					table_type : BSTableType = BSTableType(table)
					await self.init_collection(table_type)
				except Exception as err:
					error(f'{self.backend}: Could not init collection for table: {err}')

			# # Accounts
			# if BSTableType.Accounts.name in tables:
			# 	try:
			# 		DBC 	= self.table_accounts
			# 		# indexes = list()
			# 		# indexes.append([ 	('disabled', ASCENDING),
			# 		# 					('inactive', ASCENDING),
			# 		# 					('region', 	ASCENDING),
			# 		# 					('id', 		ASCENDING)
			# 		# 				])
			# 		# indexes.append([ 	('disabled', ASCENDING),
			# 		# 					('inactive', ASCENDING),
			# 		# 					('id', 		ASCENDING),
			# 		# 					('updated_tank_stats', ASCENDING)
			# 		# 				])
			# 		await self.init_collection(BSTableType.Accounts)

			# 	except Exception as err:
			# 		error(f'{self.backend}: Could not init collection {DBC} for accounts: {err}')

			# # Releases
			# if BSTableType.Releases.name in tables:
			# 	try:
			# 		DBC = self.table_releases
			# 		# indexes = list()
			# 		# indexes.append([ 	('name', ASCENDING),
			# 		# 					('launch_date', DESCENDING)
			# 		# 				])
			# 		await self.init_collection(BSTableType.Releases)

			# 	except Exception as err:
			# 		error(f'{self.backend}: Could not init collection {DBC} for releases: {err}')

			# # Tankopedia
			# if BSTableType.Tankopedia.name in tables:
			# 	try:
			# 		DBC = self.table_tankopedia
			# 		# indexes = list()
			# 		# indexes.append([ 	('tier', ASCENDING),
			# 		# 					('type', ASCENDING)
			# 		# 				])
			# 		# indexes.append([ 	('tier', ASCENDING),
			# 		# 					('nation', ASCENDING)
			# 		# 				])
			# 		# indexes.append([ 	('name', TEXT)
			# 		# 				])
			# 		await self.init_collection(BSTableType.Tankopedia)

			# 	except Exception as err:
			# 		error(f'{self.backend}: Could not init collection {DBC} for releases: {err}')

			# # Replays
			# if BSTableType.Replays.name in tables:
			# 	try:
			# 		DBC = self.table_replays
			# 		# indexes = list()
			# 		# indexes.append([('data.summary.protagonist', ASCENDING),
			# 		# 				('data.summary.room_type', ASCENDING),
			# 		# 				('data.summary.vehicle_tier', ASCENDING),
			# 		# 				('data.summary.battle_start_timestamp', DESCENDING)
			# 		# 			])
			# 		# indexes.append([('data.summary.room_type', ASCENDING),
			# 		# 				('data.summary.vehicle_tier', ASCENDING),
			# 		# 				('data.summary.battle_start_timestamp', DESCENDING)
			# 		# 			])

			# 		await self.init_collection(BSTableType.Replays)

			# 	except Exception as err:
			# 		error(f'{self.backend}: Could not init collection {DBC} for replays: {err}')

			# # Tank stats
			# if BSTableType.TankStats.name in tables:
			# 	try:
			# 		DBC = self.table_tank_stats
			# 		# indexes.append([ 	('region', ASCENDING),
			# 		# 					('account_id', ASCENDING),
			# 		# 					('tank_id', ASCENDING),
			# 		# 					('last_battle_time', DESCENDING)
			# 		# 				])
			# 		# indexes.append([ 	('region', ASCENDING),
			# 		# 					('release', DESCENDING),
			# 		# 					('tank_id', ASCENDING),
			# 		# 					('account_id', ASCENDING),
			# 		# 				])
			# 		await self.init_collection(BSTableType.TankStats)

			# 	except Exception as err:
			# 		error(f'{self.backend}: Could not init collection {DBC} for tank_stats: {err}')

			# # Player Achievements
			# if BSTableType.PlayerAchievements.name in tables:
			# 	try:
			# 		DBC = self.table_player_achievements
			# 		# indexes = list()
			# 		# indexes.append([ 	('account_id', ASCENDING),
			# 		# 					('added', DESCENDING)
			# 		# 				])
			# 		# indexes.append([ 	('release', DESCENDING),
			# 		# 					('account_id', ASCENDING),
			# 		# 					('added', DESCENDING)
			# 		# 				])
			# 		await self.init_collection(BSTableType.PlayerAchievements)

			# 	except Exception as err:
			# 		error(f'{self.backend}: Could not init collection {DBC} for player_achievements: {err}')

		except Exception as err:
			error(f'Error initializing {self.backend}: {err}')
		return False


########################################################
#
# MongoBackend(): generic data_funcs
#
########################################################


	# async def _data_insert(self, dbc : AsyncIOMotorCollection, data: D) -> bool:  		# type: ignore
	# 	"""Generic method to get one object of data_type"""
	# 	try:
	# 		# debug('starting')
	# 		res : InsertOneResult = await dbc.insert_one(data.obj_db())
	# 		# debug(f'Inserted {type(data)} (_id={res.inserted_id}) into {self.backend}.{dbc.name}: {data}')
	# 		return True
	# 	except Exception as err:
	# 		debug(f'Failed to insert {type(data)}={data} into {self.backend}.{dbc.name}: {err}')
	# 	return False


	# async def _data_get(self, dbc : AsyncIOMotorCollection,
	# 					data_type: type[D],
	# 					id: Idx) -> Optional[D]:
	# 	"""Generic method to get one object of data_type"""
	# 	try:
	# 		# debug('starting')
	# 		res : Any = await dbc.find_one({ '_id': id})
	# 		if res is not None:
	# 			return data_type.parse_obj(res)
	# 		else:
	# 			return None
	# 	except ValidationError as err:
	# 		error(f'Could not validate {type(data_type)} _id={id} from {self.backend}.{dbc.name}: {err}')
	# 		await self.error_log(MongoErrorLog(table=dbc.name, doc_id=id, type=ErrorLogType.ValidationError))
	# 	except Exception as err:
	# 		error(f'Error getting {type(data_type)} _id={id} from {self.backend}.{dbc.name}: {err}')
	# 	return None


	# async def _data_update(self, dbc : AsyncIOMotorCollection, id: Idx,
	# 						obj : BaseModel | None = None,
	# 						update: dict | None = None,
	# 						fields : list[str] | None = None) -> bool:
	# 	"""Generic method to update an object of data_type"""
	# 	try:
	# 		debug('starting')
	# 		model = self.get_model(dbc.name)
	# 		if update is not None:
	# 			pass
	# 		elif fields is not None and obj is not None:
	# 			update = obj.dict(include=set(fields))
	# 		else:
	# 			raise ValueError("'update', 'obj' and 'fields' cannot be all None")
	# 		alias_fields : dict[str, Any] = AliasMapper(model).map(update.items())
	# 		if (res := await dbc.find_one_and_update({ '_id': id}, { '$set': alias_fields})) is None:
	# 			# debug(f'Failed to update _id={id} into {self.backend}.{dbc.name}')
	# 			return False
	# 		#debug(f'Updated (_id={id}) into {self.backend}.{dbc.name}')
	# 		return True
	# 	except Exception as err:
	# 		error(f'Could not update _id={id} in {self.backend}.{dbc.name}: {err}')
	# 	return False


	# async def _data_replace(self, dbc : AsyncIOMotorCollection, data: D, 	# type: ignore
	# 						id: Idx, upsert : bool = False) -> bool:
	# 	"""Generic method to update an object of data_type"""
	# 	try:
	# 		debug('starting')
	# 		if (res := await dbc.find_one_and_replace({ '_id': id}, data.obj_db(), upsert=upsert)) is None:
	# 			debug(f'Failed to replace _id={id} into {self.backend}.{dbc.name}')
	# 			return False
	# 		debug(f'Replaced (_id={id}) into {self.backend}.{dbc.name}')
	# 		return True
	# 	except Exception as err:
	# 		debug(f'Error while replacing _id={id} in {self.backend}.{dbc.name}: {err}')
	# 	return False


	# async def _data_delete(self, dbc : AsyncIOMotorCollection, id: Idx) -> bool:
	# 	"""Generic method to delete an object of data_type"""
	# 	try:
	# 		debug('starting')
	# 		res : DeleteResult = await dbc.delete_one({ '_id': id})
	# 		if res.deleted_count == 1:
	# 			# debug(f'Delete (_id={id}) from {self.backend}.{dbc.name}')
	# 			return True
	# 		else:
	# 			pass
	# 			# debug(f'Failed to delete _id={id} from {self.backend}.{dbc.name}')
	# 	except Exception as err:
	# 		debug(f'Error while deleting _id={id} from {self.backend}.{dbc.name}: {err}')
	# 	return False


	# async def _datas_insert(self, dbc : AsyncIOMotorCollection, datas: Sequence[D]) -> tuple[int, int]:
	# 	"""Store data to the backend. Returns the number of added and not added"""
	# 	debug('starting')
	# 	added		: int = 0
	# 	not_added 	: int = 0
	# 	try:
	# 		debug(f'inserting to {self.backend}.{dbc.name}')
	# 		if len(datas) == 0:
	# 			raise ValueError('No data to insert')
	# 		res : InsertManyResult = await dbc.insert_many( (data.obj_db() for data in datas),
	# 														ordered=False)
	# 		added = len(res.inserted_ids)
	# 	except BulkWriteError as err:
	# 		if err.details is not None:
	# 			added = err.details['nInserted']
	# 			not_added = len(err.details["writeErrors"])
	# 			debug(f'Added {added}, could not add {not_added} entries to {self.backend}.{dbc.name}')
	# 		else:
	# 			error('BulkWriteError.details is None')
	# 	except Exception as err:
	# 		error(f'Unknown error when adding entries to {self.backend}.{dbc.name}: {err}')
	# 	debug(f'added={added}, not_added={not_added}')
	# 	return added, not_added


	# async def _datas_update(self, dbc : AsyncIOMotorCollection,
	# 						datas: list[D], upsert: bool = False) -> tuple[int, int]:
	# 	"""Store data to the backend. Returns number of documents inserted and not inserted"""
	# 	debug('starting')
	# 	updated			: int = 0
	# 	not_updated 	: int = len(datas)
	# 	try:
	# 		res : UpdateResult
	# 		res = await dbc.update_many( (d.obj_db() for d in datas),
	# 									  upsert=upsert, ordered=False)
	# 		updated = res.modified_count
	# 		not_updated -= updated
	# 	except Exception as err:
	# 		error(f'Unknown error when updating tank stats: {err}')
	# 	return updated, not_updated


	async def _datas_get(self, table_type: BSTableType,						
						pipeline : list[dict[str, Any]]) -> AsyncGenerator[JSONExportable, None]:
		try:
			debug('starting')
			dbc : AsyncIOMotorCollection = self.get_collection(table_type)
			model : type[JSONExportable] = self.get_model(table_type)
			debug(f'collection={dbc.name}, model={model}, pipeline={pipeline}')
			async for obj in dbc.aggregate(pipeline, allowDiskUse=True):
				try:
					yield model.parse_obj(obj)
				except ValidationError as err:
					error(f'Could not validate {model} ob={obj} from {self.table_uri(table_type)}: {err}')
				except Exception as err:
					error(f'{err}')
		except Exception as err:
			error(f'Error getting data from {self.table_uri(table_type)}: {err}')


	# async def _datas_count(self, dbc : AsyncIOMotorCollection,
	# 						pipeline : list[dict[str, Any]]) -> int:
	# 	try:
	# 		debug('starting')
	# 		pipeline.append({ '$count': 'total' })
	# 		async for res in dbc.aggregate(pipeline, allowDiskUse=True):
	# 			# print(f"_data_count(): total={res['total']}")
	# 			return int(res['total'])
	# 	except Exception as err:
	# 		error(f'Error counting documents in {self.backend}.{dbc.name}: {err}')
	# 	return -1


	async def _datas_export(self, table_type: BSTableType,
							in_type: type[D],
							out_type: type[O],
							sample : float = 0) -> AsyncGenerator[O, None]:
		"""Export data from Mongo DB"""
		try:
			debug(f'starting export from: {self.table_uri(table_type)}')
			dbc : AsyncIOMotorCollection = self.get_collection(table_type)
			pipeline : list[dict[str, Any]] = list()
			
			if sample > 0 and sample < 1:
				N : int = await dbc.estimated_document_count()
				pipeline.append({ '$sample' : { 'size' : int(N * sample) }})
			elif sample >= 1:
				pipeline.append({ '$sample' : { 'size' : int(sample) }})

			async for obj in dbc.aggregate(pipeline, allowDiskUse=True):
				try:
					if (res:= out_type.transform_obj(obj, in_type)) is not None:
						yield res
				except Exception as err:
					error(f'Could not export object={obj} type={in_type} to type={out_type}')
					error(f'{err}: {obj}')

		except Exception as err:
			error(f'Error fetching data from {self.table_uri(table_type)}: {err}')


########################################################
#
# MongoBackend(): obj_
#
########################################################


	async def _data_insert(self, table_type: BSTableType, obj: JSONExportable) -> bool:
		"""Generic method to get one object of data_type"""
		try:
			# debug('starting')
			dbc : AsyncIOMotorCollection = self.get_collection(table_type)
			model : type[JSONExportable] = self.get_model(table_type)
			if (data := model.transform_obj(obj)) is not None:
				res : InsertOneResult = await dbc.insert_one(data.obj_db())
				# debug(f'Inserted {type(data)} (_id={res.inserted_id}) into {self.backend}.{dbc.name}: {data}')
				return True
		except Exception as err:
			debug(f'Failed to insert obj={obj} into {self.table_uri(table_type)}: {err}')
		return False


	async def _data_get(self, table_type: BSTableType, idx: Idx) -> JSONExportable | None:
		"""Get document from MongoDB in its native data type. 'idx' has to be the same as in the collection stored"""
		try:
			# debug('starting')
			dbc : AsyncIOMotorCollection = self.get_collection(table_type)
			model : type[JSONExportable] = self.get_model(table_type)
			if (res := await dbc.find_one({ '_id': idx})) is not None:
				return model.parse_obj(res)
		except Exception as err:
			error(f'Error getting _id={idx} from {self.table_uri(table_type)}: {err}')
		return None


	async def _data_replace(self,
							table_type	: BSTableType,
							obj			: JSONExportable,
							upsert 		: bool = False) -> bool:
		"""Generic method to update an object of data_type"""
		try:
			debug('starting')
			dbc : AsyncIOMotorCollection = self.get_collection(table_type)
			model : type[JSONExportable] = self.get_model(table_type)
			debug(f'obj={obj} type={type(obj)} model={model}')
			if (data := model.transform_obj(obj)) is not None:
				if (res := await dbc.find_one_and_replace({ '_id': data.index}, data.obj_db(), upsert=upsert)) is None:
					debug(f'Failed to replace _id={data.index} into {self.backend}.{dbc.name}')
					return False
				debug(f'Replaced (_id={data.index}) into {self.table_uri(table_type)}')
				return True
		except Exception as err:
			error(f'Could not replace obj in {self.table_uri(table_type)}: {err}')
			error(f'obj: {obj}')
		return False


	async def _data_update(self, table_type: BSTableType,
							idx		: Idx | None = None,
							obj 	: JSONExportable | None = None,
							update	: dict | None = None,
							fields 	: list[str] | None = None) -> bool:
		"""Generic method to update an object of data_type"""
		try:
			debug('starting')
			dbc : AsyncIOMotorCollection = self.get_collection(table_type)
			model : type[JSONExportable] = self.get_model(table_type)

			if obj is not None:
				if ( data:= model.transform_obj(obj)) is None:
					raise ValueError(f'Could not transform {type(obj)} to {model}: {obj}')

				if idx is None:
					idx = data.index

				if update is not None:
					pass
				elif fields is not None:
					update = data.dict(include=set(fields))
				else:
					raise ValueError("'update', 'obj' and 'fields' cannot be all None")

			elif idx is None or update is None:
				raise ValueError("'update' is required with 'idx'")

			alias_fields : dict[str, Any] = AliasMapper(model).map(update.items())

			if (res := await dbc.find_one_and_update({ '_id': idx }, { '$set': alias_fields})) is None:
				# debug(f'Failed to update _id={idx} into {self.backend}.{dbc.name}')
				return False
			#debug(f'Updated (_id={idx}) into {self.backend}.{dbc.name}')
			return True
		except Exception as err:
			error(f'Could not update _id={idx} in {self.table_uri(table_type)}: {err}')
		return False


	async def _data_delete(self, table_type: BSTableType, idx: Idx) -> bool:
		"""Delete a document from MongoDB"""
		try:
			# debug('starting')
			dbc : AsyncIOMotorCollection = self.get_collection(table_type)
			res : DeleteResult = await dbc.delete_one({ '_id': idx})
			if res.deleted_count == 1:
				# debug(f'Delete (_id={id}) from {self.backend}.{dbc.name}')
				return True
			else:
				pass
				# debug(f'Failed to delete _id={id} from {self.backend}.{dbc.name}')
		except Exception as err:
			debug(f'Error while deleting _id={id} from {self.table_uri(table_type)}: {err}')
		return False


	async def _datas_insert(self,
							table_type: BSTableType,
							objs		: Sequence[D]) -> tuple[int, int]:
		"""Store data to the backend. Returns the number of added and not added"""
		debug('starting')
		added		: int = 0
		not_added 	: int = 0
		try:
			debug(f'inserting to {self.table_uri(table_type)}')
			dbc : AsyncIOMotorCollection = self.get_collection(table_type)
			model : type[JSONExportable] = self.get_model(table_type)
			if len(objs) == 0:
				raise ValueError('No data to insert')
			datas : list[JSONExportable|None] = [ model.transform_obj(obj) for obj in objs ]

			res : InsertManyResult = await dbc.insert_many( ( data.obj_db() for data in datas if data is not None),
															ordered=False)
			added = len(res.inserted_ids)
		except BulkWriteError as err:
			if err.details is not None:
				added = err.details['nInserted']
				not_added = len(err.details["writeErrors"])
				debug(f'Added {added}, could not add {not_added} entries to {self.table_uri(table_type)}')
			else:
				error('BulkWriteError.details is None')
		except Exception as err:
			error(f'Unknown error when adding entries to {self.table_uri(table_type)}: {err}')
		debug(f'added={added}, not_added={not_added}')
		return added, not_added


	async def _datas_update(self, table_type: BSTableType,
							objs	: Sequence[JSONExportable],
							upsert	: bool = False) -> tuple[int, int]:
		"""Store data to the backend. Returns number of documents inserted and not inserted"""
		debug('starting')
		updated		: int = 0
		not_updated : int = len(objs)
		dbc 		: AsyncIOMotorCollection= self.get_collection(table_type)
		model 		: type[JSONExportable]	= self.get_model(table_type)
		try:
			datas : list[JSONExportable] = model.transform_objs(objs=objs, in_type=type(objs[0]))
			res : UpdateResult
			res = await dbc.update_many( (d.obj_db() for d in datas),
										  upsert=upsert, ordered=False)
			updated = res.modified_count
			not_updated -= updated
		except IndexError:
			pass
		except Exception as err:
			error(f'Unknown error when updating tank stats: {err}')
		return updated, not_updated


	# async def _datas_get(self, table_type: BSTableType,
	# 					out_type: type[D],
	# 					pipeline : list[dict[str, Any]]) -> AsyncGenerator[D, None]:
	# 	try:
	# 		debug('starting')
	# 		dbc : AsyncIOMotorCollection = self.get_collection(table_type)
	# 		model : type[JSONExportable] = self.get_model(table_type)

	# 		async for obj in dbc.aggregate(pipeline, allowDiskUse=True):
	# 			try:
	# 				debug(f'{obj}')
	# 				if (data := out_type.transform_obj(obj, model)) is not None:
	# 					yield data
	# 			except ValidationError as err:
	# 				error(f'Could not validate {out_type} ob={obj} from {self.table_uri(table_type)}: {err}')
	# 			except Exception as err:
	# 				error(f'{err}')
	# 	except Exception as err:
	# 		error(f'Error fetching data from {self.table_uri(table_type)}: {err}')


	async def _datas_count(self, table_type: BSTableType,
							pipeline : list[dict[str, Any]]) -> int:
		try:
			debug('starting')
			pipeline.append({ '$count': 'total' })
			dbc : AsyncIOMotorCollection = self.get_collection(table_type)
			async for res in dbc.aggregate(pipeline, allowDiskUse=True):
				# print(f"_data_count(): total={res['total']}")
				return int(res['total'])
		except Exception as err:
			error(f'Error counting documents in {self.table_uri(table_type)}: {err}')
		return -1


	async def obj_export(self, table_type: BSTableType,
						 sample: float = 0) -> AsyncGenerator[Any, None]:
		"""Export raw documents from Mongo DB"""
		try:
			debug(f'starting')
			dbc : AsyncIOMotorCollection = self.get_collection(table_type)
			debug(f'export from: {self.table_uri(table_type)}')
			pipeline : list[dict[str, Any]] = list()

			if sample > 0 and sample < 1:
				N : int = await dbc.estimated_document_count()
				pipeline.append({ '$sample' : { 'size' : int(N * sample) }})
			elif sample >= 1:
				pipeline.append({ '$sample' : { 'size' : int(sample) }})

			async for obj in dbc.aggregate(pipeline, allowDiskUse=True):
				yield obj

		except Exception as err:
			error(f'Error fetching data from {self.table_uri(table_type)}: {err}')


	async def objs_export(self, 
						table_type: BSTableType,
						pipeline : list[dict[str, Any]] = list(),
						sample: float = 0,
						batch: int = 0) -> AsyncGenerator[list[Any], None]:
		"""Export raw documents as a list from Mongo DB"""
		try:
			debug(f'starting')
			dbc : AsyncIOMotorCollection = self.get_collection(table_type)
			debug(f'export from: {self.table_uri(table_type)}')
			if batch == 0:
				batch = MONGO_BATCH_SIZE
			
			if sample > 0 and sample < 1:
				N : int = await dbc.estimated_document_count()
				pipeline.append({ '$sample' : { 'size' : int(N * sample) }})
			elif sample >= 1:
				pipeline.append({ '$sample' : { 'size' : int(sample) }})

			cursor : AsyncIOMotorCursor = dbc.aggregate(pipeline, allowDiskUse=True)
			while objs := await cursor.to_list(batch):
				yield objs
			debug(f'finished exporting {table_type}')
		except Exception as err:
			error(f'Error fetching data from {self.table_uri(table_type)}: {err}')


########################################################
#
# MongoBackend(): account
#
########################################################


	async def account_insert(self, account: BSAccount) -> bool:
		"""Store account to the backend. Returns False
			if the account was not added"""
		debug('starting')
		return await self._data_insert(BSTableType.Accounts, obj=account)


	async def account_get(self, account_id: int) -> BSAccount | None:
		"""Get account from backend"""
		debug('starting')
		idx : int = account_id
		if (res := await self._data_get(BSTableType.Accounts, idx=idx)) is not None:
			return BSAccount.transform_obj(res, self.model_accounts)
		return None


	async def account_update(self, account: BSAccount,
							 update: dict[str, Any] | None = None,
							 fields: list[str] | None = None) -> bool:
		"""Update an account in the backend. Returns False
			if the account was not updated"""
		try:
			debug('starting')
			return await self._data_update(BSTableType.Accounts, obj=account,
											update=update, fields=fields)
		except Exception as err:
			debug(f'Error while updating account (id={account.id}) into {self.table_uri(BSTableType.Accounts)}: {err}')
		return False


	async def account_replace(self, account: BSAccount, upsert: bool = True) -> bool:
		"""Update an account in the backend. Returns False
			if the account was not updated"""
		debug('starting')
		return await self._data_replace(BSTableType.Accounts, obj=account, upsert=upsert)


	async def account_delete(self, account_id: int) -> bool:
		"""Deleta account from MongoDB backend"""
		debug('starting')
		return await self._data_delete(BSTableType.Accounts, idx=account_id)


	async def _mk_pipeline_accounts(self, stats_type : StatsTypes | None = None,
							regions	: set[Region] = Region.API_regions(),
							inactive: OptAccountsInactive = OptAccountsInactive.auto,
							dist 	: OptAccountsDistributed | None = None,
							disabled: bool|None = False, 
							sample	: float = 0,
							cache_valid: int | None = None
							) -> list[dict[str, Any]] | None:
		assert sample >= 0, f"'sample' must be >= 0, was {sample}"
		try:
			debug('starting')
			a = AliasMapper(self.model_accounts)
			alias : Callable = a.alias

			if cache_valid is None:
				cache_valid = self._cache_valid
			update_field : str | None = None
			if stats_type is not None:
				update_field = alias(stats_type.value)
			dbc 	: AsyncIOMotorCollection = self.collection_accounts
			match 	: list[dict[str, str|int|float|dict|list]] = list()
			pipeline: list[dict[str, Any]] = list()

			# Pipeline build based on ESR rule
			# https://www.mongodb.com/docs/manual/tutorial/equality-sort-range-rule/#std-label-esr-indexing-rule

			if disabled is not None:
				match.append({ alias('disabled') : disabled })
			if inactive == OptAccountsInactive.yes:
				match.append({ alias('inactive'): True })
			elif inactive == OptAccountsInactive.no:
					match.append({ alias('inactive'): False })

			match.append({ alias('region') : { '$in' : [ r.value for r in regions ]} })
			# match.append({ alias('id') : {  '$lt' : WG_ACCOUNT_ID_MAX}})  # exclude Chinese account ids

			if dist is not None:
				match.append({ alias('id') : {  '$mod' :  [ dist.div, dist.mod ]}})
				
			if inactive == OptAccountsInactive.auto:
				assert update_field is not None, "automatic inactivity detection requires stat_type"
				match.append({ '$or': [ { update_field: None}, 
										{ update_field: { '$lt': epoch_now() - cache_valid }},											
										] })				

			if len(match) > 0:
				pipeline.append( { '$match' : { '$and' : match } })

			if sample >= 1:
				pipeline.append({ '$sample' : {'size' : int(sample) } })
			elif sample > 0:
				n : int = cast(int, await dbc.estimated_document_count())
				pipeline.append({ '$sample' : {'size' : int(n * sample) } })
			return pipeline
		except Exception as err:
			error(f'{err}')
		return None


	async def accounts_get(self, 
							stats_type 	: StatsTypes | None = None,
							regions		: set[Region] = Region.API_regions(),
							inactive 	: OptAccountsInactive = OptAccountsInactive.default(),
							disabled 	: bool | None = False,
							dist 		: OptAccountsDistributed | None = None,
							sample 		: float = 0,
							cache_valid: int | None = None ) -> AsyncGenerator[BSAccount, None]:
		"""Get accounts from Mongo DB
			inactive: true = only inactive, false = not inactive, none = AUTO"""
		try:
			debug('starting')
			NOW = epoch_now()
			pipeline : list[dict[str, Any]] | None
			pipeline = await self._mk_pipeline_accounts(stats_type=stats_type, regions=regions,
														inactive=inactive, disabled=disabled,
														dist=dist, sample=sample, cache_valid=cache_valid)

			update_field : str | None = None
			if stats_type is not None:
				update_field = stats_type.value

			if pipeline is None:
				raise ValueError(f'could not create get-accounts {self.table_uri(BSTableType.Accounts)} cursor')

			async for data in self._datas_get(BSTableType.Accounts, pipeline=pipeline):				
				try:
					if (player := BSAccount.transform_obj(data)) is None:
						continue
					# if not force and not disabled and inactive is None and player.inactive:
					if not disabled and inactive == OptAccountsInactive.auto and player.inactive:
						assert update_field is not None, "automatic inactivity detection requires stat_type"
						updated = getattr(player, update_field)
						if (player.last_battle_time is not None) and \
							(updated is not None) and \
							(NOW - updated) < min(MAX_UPDATE_INTERVAL, (updated - player.last_battle_time)/2):
							continue
					yield player
				except Exception as err:
					error(f'{err}')
		except Exception as err:
			error(f'Error fetching accounts from {self.table_uri(BSTableType.Accounts)}: {err}')


	async def accounts_count(self, stats_type : StatsTypes | None = None,
							regions: set[Region] = Region.API_regions(),
							inactive : OptAccountsInactive = OptAccountsInactive.auto,
							disabled: bool = False,
							dist : OptAccountsDistributed | None = None, sample : float = 0,
							cache_valid: int | None = None ) -> int:
		assert sample >= 0, f"'sample' must be >= 0, was {sample}"
		try:
			debug('starting')
			dbc : AsyncIOMotorCollection = self.collection_accounts
			if stats_type is None and regions == Region.has_stats() and \
			   inactive == OptAccountsInactive.both and disabled == False:
				total : int = cast(int, await dbc.estimated_document_count())
				if sample == 0:
					return total
				if sample < 1:
					return int(total * sample)
				else:
					return int(min(total, sample))
			else:
				pipeline : list[dict[str, Any]] | None
				pipeline = await self._mk_pipeline_accounts(stats_type=stats_type, regions=regions,
															inactive=inactive, disabled=disabled,
															dist=dist, sample=sample,
															cache_valid=cache_valid)
				if pipeline is None:
					raise ValueError(f'Could not create pipeline for accounts {self.table_uri(BSTableType.Accounts)}')
				return await self._datas_count(BSTableType.Accounts, pipeline)
		except Exception as err:
			error(f'counting accounts failed: {err}')
		return -1


	async def accounts_export(self, sample : float = 0) -> AsyncGenerator[BSAccount, None]:
		"""Import accounts from Mongo DB"""
		debug('starting')
		async for obj in self.obj_export(BSTableType.Accounts, sample=sample):
			if (acc := BSAccount.transform_obj(obj, self.model_accounts)) is not None:
				yield acc


	async def accounts_insert(self, accounts: Sequence[BSAccount]) -> tuple[int, int]:
		"""Store account to the backend. Returns the number of added and not added"""
		debug('starting')
		return await self._datas_insert(BSTableType.Accounts, accounts)

	
	async def accounts_latest(self, regions: set[Region]) -> dict[Region, BSAccount]:
		"""Return the latest accounts (=highest account_id) per region"""
		debug('starting')
		res : dict[Region, BSAccount] = dict()
		try:
			model 	: type[JSONExportable] = self.model_accounts
			dbc 	: AsyncIOMotorCollection = self.collection_accounts
			a = AliasMapper(model)
			alias : Callable = a.alias
			pipeline: list[dict[str, Any]] | None 
			
			if (pipeline := await self._mk_pipeline_accounts(regions=regions, 
												  inactive=OptAccountsInactive.both, 
												  disabled=None)) is None:
				raise ValueError('could not create pipeline')

			pipeline.append({ '$sort': { alias('id'): DESCENDING }} )
			pipeline.append({ '$group' : {
								'_id' : '$' + alias('region'), 
								'latest': { '$first': '$$ROOT' }
			 				}})
			account : BSAccount | None
			async for doc in dbc.aggregate(pipeline, allowDiskUse = True):				
				try:
					if (account := BSAccount.transform_obj(doc['latest'], model)) is not None:
						res[account.region] = account
				except Exception as err:
					error(f'document: {doc}: error: {err}')		

		except Exception as err:
			error(f'{err}')	
		return res


########################################################
#
# MongoBackend(): player_achievements
#
########################################################


	async def player_achievement_insert(self, player_achievement: WGPlayerAchievementsMaxSeries) -> bool:
		"""Insert a single player achievement"""
		debug('starting')
		return await self._data_insert(BSTableType.PlayerAchievements, obj=player_achievement)


	async def player_achievement_get(self, account: BSAccount, added: int) -> WGPlayerAchievementsMaxSeries | None:
		"""Return a player_achievement from the backend"""
		debug('starting')
		try:
			idx	: ObjectId = WGPlayerAchievementsMaxSeries.mk_index(account_id=account.id,
																	region=account.region,
																	added=added)
			if (res := await self._data_get(BSTableType.PlayerAchievements, idx=idx)) is not None:
				return WGPlayerAchievementsMaxSeries.transform_obj(res, self.model_accounts)
		except Exception as err:
			error(f'Unknown error: {err}')
		return None


	async def player_achievement_delete(self, account: BSAccount, added: int) -> bool:
		"""Delete a player achievements from the backend"""
		try:
			debug('starting')
			debug(f'account={account}, added={added}')
			idx : ObjectId = WGPlayerAchievementsMaxSeries.mk_index(account.id,
																	region=account.region,
																	added=added)
			return await self._data_delete(BSTableType.PlayerAchievements, idx=idx)
		except Exception as err:
			error(f'Unknown error: {err}')
		return False


	async def player_achievements_insert(self, player_achievements: Sequence[WGPlayerAchievementsMaxSeries]) -> tuple[int, int]:
		"""Store player achievements to the backend. Returns number of stats inserted and not inserted"""
		debug('starting')
		return await self._datas_insert(BSTableType.PlayerAchievements, player_achievements)


	async def _mk_pipeline_player_achievements(self, release: BSBlitzRelease|None = None,
										regions: set[Region] = Region.API_regions(),
										accounts: Iterable[Account] | None = None,
										since:  int = 0,
										sample: float = 0) -> list[dict[str, Any]] | None:
		assert sample >= 0, f"'sample' must be >= 0, was {sample}"
		try:
			debug('starting')

			# class WGPlayerAchievementsMaxSeries(JSONExportable):
			# 	id 			: ObjectId | None	= Field(default=None, alias='_id')
			# 	jointVictory: int 				= Field(default=0, alias='jv')
			# 	account_id	: int		 		= Field(default=0, alias='a')
			## 	region		: Region | None 	= Field(default=None, alias='r')
			# 	release 	: str  | None 		= Field(default=None, alias='u')
			# 	added		: int 				= Field(default=epoch_now(), alias='t')

			a = AliasMapper(self.model_player_achievements)
			alias : Callable = a.alias

			dbc : AsyncIOMotorCollection = self.collection_player_achievements
			pipeline : list[dict[str, Any]] = list()
			match : list[dict[str, str|int|float|dict|list]] = list()

			# Pipeline build based on ESR rule
			# https://www.mongodb.com/docs/manual/tutorial/equality-sort-range-rule/#std-label-esr-indexing-rule

			if release is not None:
				match.append({ alias('release'): release.release })
			if regions != Region.has_stats():
				match.append({ alias('region') : { '$in' : [ r.value for r in regions ]} })
			if accounts is not None:
				match.append({ alias('account_id'): { '$in': [ a.id for a in accounts ]}})
			if since > 0:
				match.append({ alias('added'): { '$gte': since } })			
			
			if len(match) > 0:
				pipeline.append( { '$match' : { '$and' : match } })

			if sample >= 1:
				pipeline.append({ '$sample' : {'size' : int(sample) } })
			elif sample > 0:
				n : int = cast(int, await dbc.estimated_document_count())
				pipeline.append({ '$sample' : {'size' : int(n * sample) } })
			return pipeline
		except Exception as err:
			error(f'{err}')
		return None


	async def player_achievements_get(self, release: BSBlitzRelease | None = None,
							regions: set[Region] = Region.API_regions(),
							accounts: Iterable[Account] | None = None,
							since:  int = 0,
							sample : float = 0) -> AsyncGenerator[WGPlayerAchievementsMaxSeries, None]:
		"""Return player achievements from the backend"""
		try:
			debug('starting')
			pipeline : list[dict[str, Any]] | None
			pipeline = await self._mk_pipeline_player_achievements(release=release,
																	regions=regions,
																	accounts=accounts,
																	since=since,
																	sample=sample)
			if pipeline is None:
				raise ValueError(f'could not create pipeline for get player achievements {self.backend}')

			async for data in self._datas_get(BSTableType.PlayerAchievements,											
											pipeline=pipeline):
				if (pa := WGPlayerAchievementsMaxSeries.transform_obj(data)) is not None:
					yield pa
		except Exception as err:
			error(f'Error fetching player achievements from {self.table_uri(BSTableType.PlayerAchievements)}: {err}')


	async def player_achievements_count(self, release: BSBlitzRelease | None = None,
							regions: set[Region] = Region.API_regions(),
							accounts: Iterable[Account] | None = None,
							sample : float = 0) -> int:
		"""Get number of player achievements from backend"""
		assert sample >= 0, f"'sample' must be >= 0, was {sample}"
		try:
			debug('starting')
			dbc : AsyncIOMotorCollection = self.collection_player_achievements

			if release is None and regions == Region.has_stats():
				total : int = cast(int, await dbc.estimated_document_count())
				# print(f'player achievements: total={total}, sample={sample}')
				if sample == 0:
					return total
				if sample < 1:
					return int(total * sample)
				else:
					return int(min(total, sample))
			else:
				pipeline : list[dict[str, Any]] | None
				pipeline = await self._mk_pipeline_player_achievements(release=release, regions=regions,
																accounts=accounts, sample=sample)
				if pipeline is None:
					raise ValueError(f'could not create pipeline for player achievements {self.table_uri(BSTableType.PlayerAchievements)}')
				return await self._datas_count(BSTableType.PlayerAchievements, pipeline)
		except Exception as err:
			error(f'counting player achievements failed: {err}')
		return -1


	async def player_achievements_update(self, player_achievements: list[WGPlayerAchievementsMaxSeries], upsert: bool = False) -> tuple[int, int]:
		"""Update or upsert player achievements to the backend. Returns number of stats updated and not updated"""
		debug('starting')
		return await self._datas_update(BSTableType.PlayerAchievements,
										objs=player_achievements,
										upsert=upsert)


	async def player_achievement_export(self, sample: float = 0
											) -> AsyncGenerator[WGPlayerAchievementsMaxSeries, None]:
		"""Export player achievements from Mongo DB"""
		async for obj in self.obj_export(BSTableType.PlayerAchievements, sample=sample):
			if (pa := WGPlayerAchievementsMaxSeries.transform_obj(obj, self.model_player_achievements)) is not None:
				yield pa


	async def player_achievements_export(self, 
										sample: float = 0, 
										batch: int = 0,
										) -> AsyncGenerator[list[WGPlayerAchievementsMaxSeries], None]:
		"""Export player achievements as a list from Mongo DB"""
		debug('starting')
		if batch == 0:
			batch = MONGO_BATCH_SIZE
		async for objs in self.objs_export(BSTableType.PlayerAchievements,
											sample=sample,
											batch=batch):
			yield WGPlayerAchievementsMaxSeries.transform_objs(objs=objs, in_type=self.model_player_achievements)
	

	async def player_achievements_duplicates(self, 
											release: BSBlitzRelease,
											regions: set[Region] = Region.API_regions(), 									
											sample : int = 0) -> AsyncGenerator[WGPlayerAchievementsMaxSeries, None]:
		"""Find duplicate player achievements from the backend"""
		debug('starting')
		try:
			a 		: AliasMapper = AliasMapper(self.model_player_achievements)
			alias 	: Callable 	= a.alias			
			pipeline: list[dict[str, Any]] | None
			if (pipeline := await self._mk_pipeline_player_achievements(release = release,
																		regions = regions)) is None:
				raise ValueError('Could not create $match pipeline')

			pipeline.append({ '$sort': 		{ alias('added'): DESCENDING } })
			pipeline.append({ '$group': 	{ '_id': '$' + alias('account_id'),
												'all_ids': {'$push': '$_id' },
												'len': { "$sum": 1 } } 
							})
			pipeline.append({ '$match': 	{ 'len': { '$gt': 1 } } })
			pipeline.append({ '$project':	{ 'ids': {  '$slice': [  '$all_ids', 1, '$len' ] } } })										
		
			if sample > 0:
				pipeline.append({ '$sample' : { 'size': sample } })

			async for idxs in self.collection_player_achievements.aggregate(pipeline, allowDiskUse=True):
				try:
					for idx in idxs['ids']:
						if (obj := await self._data_get(BSTableType.PlayerAchievements, idx)) is not None:
							if( pa := WGPlayerAchievementsMaxSeries.transform_obj(obj)) is not None:
								# debug(f'tank stat duplicate: {tank_stat}')
								yield pa
				except Exception as err:
					error(f'{err}')

		except Exception as err:
			debug(f'Could not find duplicates from {self.table_uri(BSTableType.PlayerAchievements)}: {err}')


########################################################
#
# MongoBackend(): releases
#
########################################################


	async def release_get(self, release : str) -> BSBlitzRelease | None:
		"""Get release from backend"""
		debug('starting')
		try:
			release = WGBlitzRelease.validate_release(release)
			if (obj := await self._data_get(BSTableType.Releases, idx = release)) is not None:
				return BSBlitzRelease.transform_obj(obj, self.model_releases)
		except Exception as err:
			error(f'{err}')
		return None


	async def release_get_latest(self) -> BSBlitzRelease | None:
		"""Get the latest release in the backend"""
		debug('starting')
		rel : BSBlitzRelease | None = None
		try:
			dbc : AsyncIOMotorCollection = self.collection_releases
			async for obj in dbc.find().sort('launch_date', DESCENDING):
				return BSBlitzRelease.transform_obj(obj, self.model_releases)
		except ValidationError as err:
			error(f'Incorrect data format: {err}')
		except Exception as err:
			error(f'Could not find the latest release from {self.table_uri(BSTableType.Releases)}: {err}')
		return None


	async def release_get_current(self) -> BSBlitzRelease | None:
		"""Get the latest release in the backend"""
		debug('starting')
		try:
			dbc : AsyncIOMotorCollection = self.collection_releases
			async for obj in dbc.find({ 'launch_date': { '$lte': date.today() } }).sort('launch_date', ASCENDING):
				return BSBlitzRelease.transform_obj(obj, self.model_releases)
		except ValidationError as err:
			error(f'Incorrect data format: {err}')
		except Exception as err:
			error(f'Could not find the latest release: {err}')
		return None


	async def release_get_next(self, release: BSBlitzRelease) -> BSBlitzRelease | None:
		"""Get the latest release in the backend"""
		debug('starting')
		try:
			dbc : AsyncIOMotorCollection = self.collection_releases
			if (rel := await self.release_get(release.release)) is None:
				raise ValueError()
			else:
				release = rel
			async for obj in dbc.find({ 'cut_off': { '$gt': release.cut_off } }).sort('cut_off', ASCENDING):
				return BSBlitzRelease.transform_obj(obj, self.model_releases)
		except ValidationError as err:
			error(f'Incorrect data format: {err}')
		except Exception as err:
			error(f'Could not find the latest release from {self.table_uri(BSTableType.Releases)}: {err}')
		return None


	async def release_get_previous(self, release: BSBlitzRelease) -> BSBlitzRelease | None:
		"""Get the latest release in the backend"""
		debug('starting')
		try:
			dbc : AsyncIOMotorCollection = self.collection_releases
			if (rel := await self.release_get(release.release)) is None:
				raise ValueError()
			else:
				release = rel
			async for obj in dbc.find({ 'cut_off': { '$lt': release.cut_off } }).sort('cut_off', DESCENDING):
				return BSBlitzRelease.transform_obj(obj, self.model_releases)
			error('find() returned zero')
		except ValidationError as err:
			error(f'Incorrect data format: {err}')
		except Exception as err:
			error(f'Could not find the previous release for {release} from {self.table_uri(BSTableType.Releases)}: {err}')
		return None


	async def release_insert(self, release: BSBlitzRelease) -> bool:
		"""Insert new release to the backend"""
		debug('starting')
		return await self._data_insert(BSTableType.Releases, obj=release)


	async def release_update(self, release: BSBlitzRelease,
								update: dict[str, Any] | None = None,
								fields: list[str] | None= None) -> bool:
		"""Update an release in the backend. Returns False
			if the release was not updated"""
		try:
			debug('starting')
			# return await self._data_update(self.collection_releases,
			# 								id=release.release, obj=release,
			# 								update=update, fields=fields)
			return await self._data_update(BSTableType.Releases, obj=release,
											update=update, fields=fields)
		except Exception as err:
			debug(f'Error while updating release {release} into {self.table_uri(BSTableType.Releases)}: {err}')
		return False


	async def release_replace(self, release: BSBlitzRelease, upsert=False) -> bool:
		"""Update an account in the backend. Returns False
			if the account was not updated"""
		debug('starting')
		# return await self._data_replace(self.collection_releases, data=release,
		# 								id=release.release, upsert=upsert)
		return await self._data_replace(BSTableType.Releases, obj=release, upsert=upsert)


	async def release_delete(self, release: str) -> bool:
		"""Delete a release from backend"""
		debug('starting')
		release = WGBlitzRelease.validate_release(release)
		return await self._data_delete(BSTableType.Releases, idx=release)


	async def _mk_pipeline_releases(self, release_match: str | None = None,
							since : int = 0,
							first : BSBlitzRelease | None = None) -> list[dict[str, Any]]:
		"""Build aggregation pipeline for releases"""
		try:
			debug('starting')
			match : list[dict[str, str|int|float|datetime|dict|list]] = list()
			pipeline : list[dict[str, Any]] = list()
			a = AliasMapper(self.model_releases)
			alias : Callable = a.alias
			if since > 0:
				# match.append( { alias('launch_date'):  { '$gte': datetime.combine(since, datetime.min.time()) }})
				match.append( { alias('launch_date'):  { '$gte': since }})
			if first is not None:
				match.append( { alias('launch_date'):  { '$gte': first.launch_date }})
			if release_match is not None:
				match.append( { alias('release'): { '$regex' : '^' + release_match } } )

			if len(match) > 0:
				pipeline.append( { '$match' : { '$and' : match } })

			pipeline.append( { '$sort': { alias('release'): ASCENDING } })
			debug(f'pipeline: {pipeline}')
			return pipeline
		except Exception as err:
			error(f'Error creating pipeline: {err}')
			raise err


	async def releases_get(self, release_match: str | None = None,
							since : int = 0,
							first : BSBlitzRelease | None = None) -> AsyncGenerator[BSBlitzRelease, None]:
		assert since == 0 or first is None, 'Only one can be defined: since, first'
		debug('starting')
		try:
			pipeline : list[dict[str, Any]]
			pipeline = await self._mk_pipeline_releases(release_match=release_match, since=since, first=first)

			async for data in self._datas_get(BSTableType.Releases, pipeline=pipeline):
				if (release := BSBlitzRelease.transform_obj(data)) is not None:
					yield release

		except Exception as err:
			error(f'Error getting releases: {err}')


	async def releases_export(self, sample : float = 0) -> AsyncGenerator[BSBlitzRelease, None]:
		"""Import relaseses from Mongo DB"""
		debug('starting')
		async for obj in self.obj_export(BSTableType.Releases,
											sample=sample):
			if (rel := BSBlitzRelease.transform_obj(obj, self.model_releases)) is not None:
				yield rel


########################################################
#
# MongoBackend(): replay
#
########################################################

	async def replay_insert(self, replay: JSONExportable) -> bool:
		"""Store replay into backend"""
		debug('starting')
		# return await self._data_insert(self.collection_replays, replay)
		return await self._data_insert(BSTableType.Replays, obj=replay)


	async def replay_get(self, replay_id: str) -> WoTBlitzReplayData | None:
		"""Get replay from backend"""
		debug('starting')
		if (rep := await self._data_get(BSTableType.Replays, idx=replay_id)) is not None:
			return WoTBlitzReplayData.transform_obj(rep, self.model_replays)
		return None


	async def replay_delete(self, replay_id: str) -> bool:
		"""Delete a replay from backend"""
		debug('starting')
		return await self._data_delete(BSTableType.Replays, idx=replay_id)


	async def replays_insert(self, replays: Sequence[JSONExportable]) ->  tuple[int, int]:
		"""Insert replays to MongoDB backend"""
		debug('starting')
		#return await self._datas_insert(self.collection_replays, replays)
		return await self._datas_insert(BSTableType.Replays, replays)


	async def _mk_pipeline_replays(self, since: int = 0, sample : float = 0,
									**summary_fields) -> list[dict[str, Any]]:
		"""Build pipeline for replays"""
		assert sample >= 0, f"'sample' must be >= 0, was {sample}"
		debug('starting')
		match 	: list[dict[str, str|int|float|dict|list]] = list()
		pipeline: list[dict[str, Any]] = list()
		dbc   	: AsyncIOMotorCollection = self.collection_replays
		a 	  	: AliasMapper 	= AliasMapper(self.model_replays)
		alias 	: Callable 		= a.alias

		if since > 0:
			match.append( {'s.bts': { '$gte': since }})

		for sf, value in summary_fields.items():
			try:
				match.append( { f's.{alias(sf)}': value })
			except KeyError:
				error(f'No such a key in {self.model_replays.__qualname__}: {alias(sf)}')
			except Exception as err:
				error(f"Error setting filter for summary field '{alias(sf)}': {err}")

		if len(match) > 0:
			pipeline.append( { '$match' : { '$and' : match } })

		if sample >= 1:
			pipeline.append({ '$sample' : {'size' : int(sample) } })
		elif sample > 0:
			n : int = cast(int, await dbc.estimated_document_count())
			pipeline.append({ '$sample' : {'size' : int(n * sample) } })

		return pipeline


	async def replays_get(self, 
						  since: int = 0, 
						  sample : float = 0,
						  **summary_fields) ->  AsyncGenerator[WoTBlitzReplayData, None]:
		"""Get replays from mongodb backend"""
		debug('starting')
		try:
			debug('starting')
			pipeline : list[dict[str, Any]]
			pipeline = await self._mk_pipeline_replays(since=since, sample=sample, **summary_fields)
			async for data in self._datas_get(BSTableType.Replays, pipeline):
				if (replay := WoTBlitzReplayData.transform_obj(data)) is not None:
					yield replay				
		except Exception as err:
			error(f'Error exporting replays from {self.table_uri(BSTableType.Replays)}: {err}')


	async def replays_count(self, since: int = 0, sample : float = 0,
							**summary_fields) -> int:
		"""Count replays in backed"""
		try:
			debug('starting')
			pipeline : list[dict[str, Any]] = await self._mk_pipeline_replays(since=since, sample=sample, **summary_fields)
			return await self._datas_count(BSTableType.Replays, pipeline)

		except Exception as err:
			error(f'Error exporting replays from {self.table_uri(BSTableType.Replays)}: {err}')
		return -1


	async def replays_export(self, 
							 sample: float = 0) -> AsyncGenerator[WoTBlitzReplayData, None]:
		"""Export replays from Mongo DB"""
		debug('starting')
		async for replay in self._datas_export(BSTableType.Replays,
												in_type=self.model_replays,
												out_type=WoTBlitzReplayData,
												sample=sample):
			yield replay


########################################################
#
# MongoBackend(): tank_stats
#
########################################################


	async def tank_stat_insert(self, tank_stat: WGTankStat) -> bool:
		"""Insert a single tank stat"""
		debug('starting')
		return await self._data_insert(BSTableType.TankStats, obj=tank_stat)


	async def tank_stat_get(self, account_id: int, tank_id: int,
							last_battle_time: int) -> WGTankStat | None:
		"""Return tank stats from the backend"""
		try:
			debug('starting')
			idx : ObjectId = WGTankStat.mk_id(account_id, last_battle_time, tank_id)
			if (res:= await self._data_get(BSTableType.TankStats,  idx=idx)) is not None:
				return WGTankStat.transform_obj(res, self.model_tank_stats)
		except Exception as err:
			error(f'Unknown error: {err}')
		return None


	async def tank_stat_update(self, tank_stat: WGTankStat,
							 update: dict[str, Any] | None = None,
							 fields: list[str] | None = None) -> bool:
		"""Update an tank stat in the backend. Returns False
			if the tank stat was not updated"""
		try:
			debug('starting')
			return await self._data_update(BSTableType.TankStats,
											idx=tank_stat.id, obj=tank_stat,
											update=update, fields=fields)
		except Exception as err:
			debug(f'Error while updating tank stat (id={tank_stat.id}) into {self.table_uri(BSTableType.TankStats)}: {err}')
		return False


	async def tank_stat_delete(self, 
								account_id: int, 
								tank_id: int,
								last_battle_time: int) -> bool:
		try:
			debug('starting')
			idx : ObjectId = WGTankStat.mk_id(account_id, last_battle_time, tank_id)
			return await self._data_delete(BSTableType.TankStats, idx=idx)
		except Exception as err:
			error(f'Unknown error: {err}')
		return False


	async def tank_stats_insert(self, tank_stats: Sequence[WGTankStat]) -> tuple[int, int]:
		"""Store tank stats to the backend. Returns the number of added and not added"""
		debug('starting')
		return await self._datas_insert(BSTableType.TankStats, tank_stats)


	async def _mk_pipeline_tank_stats(self, release: BSBlitzRelease | None = None,
										regions: 	set[Region] = Region.API_regions(),
										accounts: 	Sequence[Account] | None = None,
										tanks: 		Sequence[Tank] | None = None,
										since:  	int = 0,
										sample: 	float = 0) -> list[dict[str, Any]] | None:
		assert sample >= 0, f"'sample' must be >= 0, was {sample}"
		try:
			debug('starting')

			# class WGTankStat(JSONExportable, JSONImportable):
			# id					: ObjectId | None = Field(None, alias='_id')
			# _region				: Region | None = Field(None, alias='r')
			# all					: WGTankStatAll = Field(..., alias='s')
			# last_battle_time		: int			= Field(..., alias='lb')
			# account_id			: int			= Field(..., alias='a')
			# tank_id				: int 			= Field(..., alias='t')
			# mark_of_mastery		: int 			= Field(..., alias='m')
			# battle_life_time		: int 			= Field(..., alias='l')
			# max_xp				: int  | None
			# in_garage_updated		: int  | None
			# max_frags				: int  | None
			# frags					: int  | None
			# in_garage 			: bool | None

			a = AliasMapper(self.model_tank_stats)
			alias : Callable = a.alias
			dbc : AsyncIOMotorCollection = self.collection_tank_stats
			pipeline : list[dict[str, Any]] = list()
			match : list[dict[str, str|int|float|dict|list]] = list()

			# Pipeline build based on ESR rule
			# https://www.mongodb.com/docs/manual/tutorial/equality-sort-range-rule/#std-label-esr-indexing-rule

			match.append({ alias('region') : { '$in' : [ r.value for r in regions ]} })
			if release is not None:
				match.append({ alias('release'): release.release })
			if accounts is not None:
				match.append({ alias('account_id'): { '$in': [ a.id for a in accounts ]}})
			if tanks is not None:
				match.append({ alias('tank_id'): { '$in': [ t.tank_id for t in tanks ]}})
			if since > 0:
				match.append({ alias('last_battle_time'): { '$gte': since } })

			pipeline.append( { '$match' : { '$and' : match } })

			if sample >= 1:
				pipeline.append({ '$sample' : {'size' : int(sample) } })
			elif sample > 0:
				n : int = cast(int, await dbc.estimated_document_count())
				pipeline.append({ '$sample' : {'size' : int(n * sample) } })
			
			#debug(f'pipeline={pipeline}')
			return pipeline
		except Exception as err:
			error(f'{err}')
		return None


	async def _mk_pipeline_tank_stats_latest(self, 
											account: 	Account, 
											release: 	BSBlitzRelease,
											# tanks: 		Sequence[Tank] | None = None,
											) -> list[dict[str, Any]] | None:		
		try:
			debug('starting')

			# class WGTankStat(JSONExportable, JSONImportable):
			# 	id					: ObjectId  	= Field(alias='_id')
			## 	region				: Region | None = Field(default=None, alias='r')
			# 	all					: WGTankStatAll = Field(..., alias='s')
			# 	last_battle_time	: int			= Field(..., alias='lb')
			# 	account_id			: int			= Field(..., alias='a')
			# 	tank_id				: int 			= Field(..., alias='t')
			# 	mark_of_mastery		: int 			= Field(default=0, alias='m')
			# 	battle_life_time	: int 			= Field(default=0, alias='l')
			# 	release 			: str  | None 	= Field(default=None, alias='u')
			# 	max_xp				: int  | None
			# 	in_garage_updated	: int  | None
			# 	max_frags			: int  | None
			# 	frags				: int  | None
			# 	in_garage 			: bool | None

			a = AliasMapper(self.model_tank_stats)
			alias : Callable = a.alias
			pipeline : list[dict[str, Any]] = list()
			match : list[dict[str, str|int|float|dict|list]] = list()

			# Pipeline build based on ESR rule
			# https://www.mongodb.com/docs/manual/tutorial/equality-sort-range-rule/#std-label-esr-indexing-rule

			match.append({ alias('region'): account.region.value })			
			match.append({ alias('account_id'): account.id })			
			match.append({ alias('last_battle_time') : { '$lte' : release.cut_off }})
			
			pipeline.append( { '$match' : { '$and' : match } })
			pipeline.append( { '$sort': { alias('last_battle_time'): DESCENDING }})
			pipeline.append( { '$group' :   { '_id' : '$' + alias('tank_id'), 'doc': { '$first': '$$ROOT'}  }})
			pipeline.append( { '$replaceWith' : '$doc'} )
			pipeline.append( { '$project' : { '_id': 0 }} )
			#debug(f'pipeline={pipeline}')
			return pipeline
		except Exception as err:
			error(f'{err}')
		return None


	async def tank_stats_get(self, release: BSBlitzRelease | None = None,
							regions : set[Region] = Region.API_regions(),
							accounts: Sequence[Account] | None = None,
							tanks 	: Sequence[Tank] | None = None,
							since	: int = 0,
							sample 	: float = 0) -> AsyncGenerator[WGTankStat, None]:
		"""Return tank stats from the backend"""
		try:
			debug('starting')
			pipeline : list[dict[str, Any]] | None
			pipeline = await self._mk_pipeline_tank_stats(release=release, regions=regions,
															tanks=tanks, accounts=accounts,
															since=since, sample=sample)
			if pipeline is None:
				raise ValueError(f'could not create pipeline for get tank stats {self.backend}')

			async for data in self._datas_get(BSTableType.TankStats, pipeline):
				if (tank_stat := WGTankStat.transform_obj(data)) is not None:
					yield tank_stat
				else:
					error(f'could not transform data to WGTankStat: {data}')
		except Exception as err:
			error(f'Error fetching tank stats from {self.table_uri(BSTableType.TankStats)}: {err}')


	async def tank_stats_export_career(self, 						
										account: Account,							
										release	: BSBlitzRelease,							
										) -> AsyncGenerator[list[WGTankStat], None]:
		"""Return tank stats from the backend"""
		try:
			debug('starting')
			pipeline : list[dict[str, Any]] | None

			pipeline = await self._mk_pipeline_tank_stats_latest(account=account, release=release)
			if pipeline is None:
				raise ValueError(f'{self.backend}: could not create pipeline for get latest tank stats')

			async for data in self.objs_export(BSTableType.TankStats, pipeline):
				if len(tank_stats := WGTankStat.transform_objs(data, self.model_tank_stats)) > 0:
					yield tank_stats
				# if (tank_stat := WGTankStat.transform_obj(data)) is not None:
				# 	yield tank_stat
				# else:
				# 	error(f'could not transform data to WGTankStat: {data}')
		except Exception as err:
			error(f'Error fetching tank stats from {self.table_uri(BSTableType.TankStats)}: {err}')


	async def tank_stats_count(self, release: BSBlitzRelease | None = None,
								regions: 	set[Region] = Region.API_regions(),
								accounts: 	Sequence[Account] | None = None,
								tanks: 		Sequence[Tank] | None = None,
								since:  	int = 0,
								sample : 	float = 0) -> int:
		assert sample >= 0, f"'sample' must be >= 0, was {sample}"
		try:
			debug('starting')
			dbc : AsyncIOMotorCollection = self.collection_tank_stats

			if release is None and regions == Region.has_stats():
				total : int = cast(int, await dbc.estimated_document_count())
				if sample == 0:
					return total
				if sample < 1:
					return int(total * sample)
				else:
					return int(min(total, sample))
			else:
				pipeline : list[dict[str, Any]] | None
				pipeline = await self._mk_pipeline_tank_stats(release=release, regions=regions,
																tanks=tanks, accounts=accounts,
																since=since, sample=sample)

				if pipeline is None:
					raise ValueError(f'could not create pipeline for tank stats {self.table_uri(BSTableType.TankStats)}')
				return await self._datas_count(BSTableType.TankStats, pipeline)
		except Exception as err:
			error(f'counting tank stats failed: {err}')
		return -1


	async def tank_stats_update(self, tank_stats: list[WGTankStat],
								upsert: bool = False) -> tuple[int, int]:
		"""Store tank stats to the backend. Returns number of stats inserted and not inserted"""
		debug('starting')
		return await self._datas_update(BSTableType.TankStats,
										objs=tank_stats, upsert=upsert)


	async def tank_stat_export(self, 
								sample: float = 0) -> AsyncGenerator[WGTankStat, None]:
		"""Export tank stats from Mongo DB"""
		debug('starting')
		async for tank_stat in self._datas_export(BSTableType.TankStats,
												in_type=self.model_tank_stats,
												out_type=WGTankStat,
												sample=sample):
			yield tank_stat


	async def tank_stats_export(self, sample: float = 0, batch: int = 0
									) -> AsyncGenerator[list[WGTankStat], None]:
		"""Export tank stats as list from Mongo DB"""
		debug('starting')
		if batch == 0:
			batch = MONGO_BATCH_SIZE
		async for objs in self.objs_export(BSTableType.TankStats,
											sample=sample,
											batch=batch):
			yield WGTankStat.transform_objs(objs=objs, in_type=self.model_tank_stats)


	async def tank_stats_duplicates(self,
									tank: Tank,
									release: BSBlitzRelease,
									regions: set[Region] = Region.API_regions(),
									sample : int = 0) -> AsyncGenerator[WGTankStat, None]:
		"""Find duplicate tank stats from the backend"""
		debug('starting')
		try:
			a = AliasMapper(self.model_tank_stats)
			alias : Callable = a.alias			
			pipeline : list[dict[str, Any]] | None
			if (pipeline := await self._mk_pipeline_tank_stats(release = release,
																regions = regions, 
																tanks = [tank])) is None:
				raise ValueError('Could not create $match pipeline')

			pipeline.append({ '$sort': 		{ alias('last_battle_time'): DESCENDING } })
			pipeline.append({ '$group': 	{ '_id': '$' + alias('account_id'),
												'all_ids': {'$push': '$_id' },
												'len': { "$sum": 1 } } 
							})
			pipeline.append({ '$match': 	{ 'len': { '$gt': 1 } } })
			pipeline.append({ '$project':	{ 'ids': {  '$slice': [  '$all_ids', 1, '$len' ] } } })										
		
			if sample > 0:
				pipeline.append({ '$sample' : { 'size': sample } })

			async for idxs in self.collection_tank_stats.aggregate(pipeline, allowDiskUse=True):
				try:
					for idx in idxs['ids']:
						if (obj := await self._data_get(BSTableType.TankStats, idx)) is not None:
							if( tank_stat := WGTankStat.transform_obj(obj)) is not None:
								# debug(f'tank stat duplicate: {tank_stat}')
								yield tank_stat
				except Exception as err:
					error(f'{err}')

		except Exception as err:
			debug(f'Could not find duplicates from {self.table_uri(BSTableType.TankStats)}: {err}')


	async def tank_stats_unique(self,
								field	: str,
								field_type: type[A], 
								release	: BSBlitzRelease | None = None,
								regions	: set[Region] = Region.API_regions(),
								account	: BSAccount | None = None, 
								tank	: Tank | None = None, 
								randomize: bool = True
								) -> AsyncGenerator[A, None]:
		"""Return unique values of field"""
		debug('starting')
		try:
			a 		: AliasMapper 	= AliasMapper(self.model_tank_stats)
			alias 	: Callable 		= a.alias
			db_field: str = alias(field)
			dbc 	: AsyncIOMotorCollection = self.collection_tank_stats
			query 	: dict[str, Any] = dict()

			if release is not None:
				query[alias('release')] = release.release
			query[alias('region')] = { '$in': list(regions)}
			if tank is not None:
				query[alias('tank_id')] = tank.tank_id
			if account is not None:
				query[alias('account_id')] = account.id
			values : list[A] = await dbc.distinct(key = db_field, filter=query)
			if randomize:
				random.shuffle(values)
			for item in values:		# type: ignore
				yield item
		except Exception as err:
			error(f'{err}')
		
		
	async def tank_stats_unique_count(self,
								field	: str,
								field_type: type[A], 
								release	: BSBlitzRelease | None = None,
								regions	: set[Region] = Region.API_regions(),
								account	: BSAccount | None = None, 
								tank	: Tank | None = None
								) -> int:
		"""Return count of unique values of field"""
		debug('starting')
		count : int = 0
		try:
			a 		: AliasMapper 	= AliasMapper(self.model_tank_stats)
			alias 	: Callable 		= a.alias
			db_field: str = alias(field)
			dbc 	: AsyncIOMotorCollection = self.collection_tank_stats
			query 	: dict[str, Any] = dict()

			if release is not None:
				query[alias('release')] = release.release
			query[alias('region')] = { '$in': list(regions)}
			if tank is not None:
				query[alias('tank_id')] = tank.tank_id
			if account is not None:
				query[alias('account_id')] = account.id
			
			return len(await dbc.distinct(key = db_field, filter=query))		# type: ignore
				
		except Exception as err:
			error(f'{err}')
		return 0


	########################################################
	#
	# MongoBackend(): tankopedia
	#
	########################################################


	def _mk_tankopedia_pipeline(self,
								tanks 		: list[Tank] | None 		= None,
								tier		: EnumVehicleTier | None 	= None,
								tank_type	: EnumVehicleTypeStr | None = None,
								nation		: EnumNation | None 		= None,
								is_premium	: bool | None 				= None
								) -> list[dict[str, Any]] | None:
		debug('starting')
		try:
			a 		: AliasMapper 	= AliasMapper(self.model_tankopedia)
			alias 	: Callable 		= a.alias
			match : list[dict[str, str|int|float|dict|list]] = list()
			pipeline : list[dict[str, Any]] = list()
			if is_premium is not None:
				match.append({ alias('is_premium'): is_premium  })				
			if tier is not None:				
				match.append({ alias('tier'): tier.value })
			if tank_type is not None:
				match.append( { alias('type'): tank_type.value })				
			if nation is not None:
				match.append( { alias('nation'): nation.name })				
			if tanks is not None and len(tanks) > 0:
				match.append( { alias('tank_id'): { '$in': [ t.tank_id for t in tanks ] } })
			if len(match) > 0:
				pipeline.append( { '$match' : { '$and' : match } })

			return pipeline
		except Exception as err:
			error(f'could not create query: {err}')
		return None


	async def tankopedia_get(self, tank_id 	: int) -> Tank | None:
		debug('starting')
		try:			
			if (obj := await self._data_get(BSTableType.Tankopedia, idx = tank_id)) is not None:
				return Tank.transform_obj(obj, self.model_tankopedia)
		except Exception as err:
			error(f'{err}')
		return None
		

	async def tankopedia_get_many(self,
							tanks 		: list[Tank] | None 		= None,
							tier		: EnumVehicleTier | None 	= None,
							tank_type	: EnumVehicleTypeStr | None = None,
							nation		: EnumNation | None 		= None,
							is_premium	: bool | None 				= None) -> AsyncGenerator[Tank, None]:
		debug('starting')
		try:
			pipeline : list[dict[str, Any]] | None
			if (pipeline := self._mk_tankopedia_pipeline(tanks=tanks, tier=tier, 
														tank_type=tank_type, 
														nation=nation, 
														is_premium=is_premium)) is None:
				raise ValueError('Could not create Tankopedia pipeline')
			# debug(f'pipeline={pipeline}')
			async for data in self._datas_get(BSTableType.Tankopedia, pipeline):
				if (tank := Tank.transform_obj(data)) is not None:
					yield tank
		except Exception as err:
			error(f'Could get Tankopedia from {self.table_uri(BSTableType.Tankopedia)}: {err}')


	async def tankopedia_count(self, 
								tanks 		: list[Tank] | None 		= None, 
								tier		: EnumVehicleTier | None 	= None,
								tank_type	: EnumVehicleTypeStr | None = None,
								nation		: EnumNation | None 		= None,							
								is_premium	: bool | None 				= None,
								) -> int:
		"""Count tanks in Tankopedia"""
		try:
			pipeline : list[dict[str, Any]] | None
			if (pipeline := self._mk_tankopedia_pipeline(tanks=tanks, tier=tier, 
														tank_type=tank_type, 
														nation=nation, 
														is_premium=is_premium)) is None:
				raise ValueError('Could not create Tankopedia pipeline')
			return await self._datas_count(BSTableType.Tankopedia, pipeline)			
		except Exception as err:
			debug(f'Could get Tankopedia from {self.table_uri(BSTableType.Tankopedia)}: {err}')
		return -1


	async def tankopedia_insert(self, tank: Tank) -> bool:
		""""insert tank into Tankopedia"""
		debug('starting')
		return await self._data_insert(BSTableType.Tankopedia, obj=tank)


	async def tankopedia_replace(self, tank: Tank, upsert : bool = True) -> bool:
		""""Replace tank in Tankopedia"""
		debug('starting')
		return await self._data_replace(BSTableType.Tankopedia, obj=tank, upsert=upsert)


	async def tankopedia_update(self, tank: Tank,
								update: dict[str, Any] | None = None,
								fields: list[str] | None= None) -> bool:
		"""Update a tank in the backend's tankopedia. Returns False
			if the tank was not updated"""
		try:
			debug('starting')
			return await self._data_update(BSTableType.Tankopedia, obj=tank,
											update=update, fields=fields)
		except Exception as err:
			debug(f"Could't update tank {tank} in {self.table_uri(BSTableType.Tankopedia)}: {err}")
		return False



	async def tankopedia_export(self, 
								sample: float = 0) -> AsyncGenerator[Tank, None]:
		"""Export tankopedia"""
		debug(f'starting: model={self.model_tankopedia} ')
		async for tank in self._datas_export(BSTableType.Tankopedia,
												in_type=self.model_tankopedia,
												out_type=Tank,
												sample=sample):
			yield tank


	async def tankopedia_delete(self, tank: Tank) -> bool:
		"""Delete a tank from Tankopedia"""
		return await self._data_delete(BSTableType.Tankopedia, idx=tank.tank_id)


	########################################################
	#
	# MongoBackend(): tank_string_
	#
	########################################################


	async def tank_string_insert(self, tank_str: WoTBlitzTankString) -> bool:
		""""insert a tank string"""
		return await self._data_insert(BSTableType.TankStrings, tank_str)


	async def tank_string_get(self, code: str) -> WoTBlitzTankString | None:
		"""Get a tank string from the backend"""
		debug('starting')
		if (tank_str := await self._data_get(BSTableType.TankStrings, idx=code)) is not None:
			return WoTBlitzTankString.transform_obj(tank_str, self.model_tank_strings)
		return None
	

	async def tank_strings_get(self, search: str | None) -> AsyncGenerator[WoTBlitzTankString, None]:
		debug('starting')
		try:
			model : type[JSONExportable] = self.model_tank_strings
			mapper : AliasMapper = AliasMapper(model)
			alias : Callable = mapper.alias
			if search is not None and bool(re.match(r'^[a-zA-Z\s]+$', search)):
				async for obj in self.collection_tank_strings.find({ alias('name') : { '$regex': search, 
																						'$options': 'i' } }):
					if (res:= WoTBlitzTankString.transform_obj(obj, model)) is not None:
						yield res
			else:
				async for obj in self.collection_tank_strings.find():
					if (res:= WoTBlitzTankString.transform_obj(obj, model)) is not None:
						yield res		
		except Exception as err:
			error(f"Could't get tank strings for search string: {search}: {err}")


	async def tank_string_replace(self, tank_str: WoTBlitzTankString, upsert : bool = True) -> bool:
		""""Replace tank in Tankopedia"""
		return await self._data_replace(BSTableType.TankStrings, obj=tank_str, upsert=upsert)


	########################################################
	#
	# MongoBackend(): error_
	#
	########################################################


	async def error_log(self, error: ErrorLog) -> bool:
		"""Log an error into the backend's ErrorLog"""
		try:
			debug('starting')
			debug(f'Logging error: {error.table}: {error.msg}')
			dbc : AsyncIOMotorCollection = self.collection_error_log
			await dbc.insert_one(error.obj_db())
			return True
		except Exception as err:
			debug(f'Could not log error: {error.table}: "{error.msg}" into {self.table_uri(BSTableType.ErrorLog)}: {err}')
		return False


	async def errors_get(self, table_type: BSTableType | None = None, doc_id : Any | None = None,
							after: datetime | None = None) -> AsyncGenerator[ErrorLog, None]:
		"""Return errors from backend ErrorLog"""
		try:
			debug('starting')
			dbc : AsyncIOMotorCollection = self.collection_error_log
			query : dict[str, Any] = dict()

			if after is not None:
				query['d'] = { '$gte': after }
			if table_type is not None:
				query['t'] = self.get_table(table_type)
			if doc_id is not None:
				query['did'] = doc_id

			err	: MongoErrorLog
			async for error_obj in dbc.find(query).sort('d', ASCENDING):
				try:
					err = MongoErrorLog.parse_obj(error_obj)
					debug(f'Read "{err.msg}" from {self.table_uri(BSTableType.ErrorLog)}')

					yield err
				except Exception as e:
					error(f'{e}')
					continue
		except Exception as e:
			error(f'Error getting errors from {self.table_uri(BSTableType.ErrorLog)}: {e}')


	async def errors_clear(self, table_type: BSTableType, doc_id : Any | None = None,
							after: datetime | None = None) -> int:
		"""Clear errors from backend ErrorLog"""
		try:
			debug('starting')

			dbc : AsyncIOMotorCollection = self.collection_error_log
			query : dict[str, Any] = dict()

			query['t'] = self.get_table(table_type)
			if after is not None:
				query['d'] = { '$gte': after }
			if doc_id is not None:
				query['did'] = doc_id

			res : DeleteResult = await dbc.delete_many(query)
			return res.deleted_count
		except Exception as e:
			error(f'Error clearing errors from {self.table_uri(BSTableType.ErrorLog)}: {e}')
		return 0

# Register backend

debug('Registering mongodb')
Backend.register(driver=MongoBackend.driver, backend=MongoBackend)