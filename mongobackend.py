from configparser import ConfigParser
from argparse import Namespace, ArgumentParser
from datetime import date, datetime
from os.path import isfile
from typing import Optional, Any, Iterable, AsyncGenerator, TypeVar, ClassVar, cast, Generic, Callable
import logging

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCursor, AsyncIOMotorCollection # type: ignore
from pymongo.results import InsertManyResult, InsertOneResult, UpdateResult, DeleteResult
from pymongo.errors import BulkWriteError, CollectionInvalid
from pymongo import DESCENDING, ASCENDING
from pydantic import BaseModel, ValidationError, Field

from backend import Backend, OptAccountsDistributed, OptAccountsInactive, BSTableType, \
					MAX_UPDATE_INTERVAL, WG_ACCOUNT_ID_MAX, MIN_UPDATE_INTERVAL, ErrorLog, ErrorLogType
from models import BSAccount, BSBlitzRelease, StatsTypes
from pyutils import epoch_now, JSONExportable, AliasMapper, alias_mapper
from blitzutils.models import Region, Account, Tank, WoTBlitzReplayJSON, WoTBlitzReplaySummary, \
								WGtankStat, WGBlitzRelease, WGplayerAchievementsMaxSeries

# Setup logging
logger	= logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug

# Constants
TANK_STATS_BATCH	: int = 1000

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

I = TypeVar('I', str, int, ObjectId)
D = TypeVar('D', bound=JSONExportable)
O = TypeVar('O', bound=JSONExportable)

class MongoBackend(Backend):

	driver : str = 'mongodb'
	# default_db : str = 'BlitzStats'	

	def __init__(self, config: ConfigParser | None = None, **kwargs):
		"""Init MongoDB backend from config file and CLI args
			CLI arguments overide settings in the config file"""

		debug('starting')
		super().__init__(config=config, **kwargs)

		mongodb_rc 		: dict[str, Any] = dict()
		self._config 	: dict[str, Any]
		self._client 	: AsyncIOMotorClient

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

		if 'database' in kwargs:
			self._database = kwargs['database']
			del kwargs['database']

		try:			
			self.db 	: AsyncIOMotorDatabase

			if config is not None:
				if 'GENERAL' in config.sections():
					configGeneral = config['GENERAL']
					self._cache_valid 	= configGeneral.getint('cache_valid', MIN_UPDATE_INTERVAL) 
				if 'MONGODB' in config.sections():
					configMongo = config['MONGODB']
					self._database		= configMongo.get('database', self._database)
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

					self.set_table(BSTableType.Accounts, 	configMongo.get('c_accounts', 	self.table_accounts))
					self.set_table(BSTableType.Tankopedia, 	configMongo.get('c_tankopedia', self.table_tankopedia))
					self.set_table(BSTableType.Releases, 	configMongo.get('c_releases', 	self.table_releases))
					self.set_table(BSTableType.Replays, 	configMongo.get('c_replays', 	self.table_replays))
					self.set_table(BSTableType.TankStats, 	configMongo.get('c_tank_stats', self.table_tank_stats))
					self.set_table(BSTableType.PlayerAchievements, configMongo.get('c_player_achievements', 
																				self.table_player_achievements))
					self.set_table(BSTableType.AccountLog, 	configMongo.get('c_account_log',self.table_account_log))
					self.set_table(BSTableType.ErrorLog,	configMongo.get('c_error_log', 	self.table_error_log))

				else:					
					debug(f'"MONGODB" section not found from config file')

			for param, value in kwargs.items():
				mongodb_rc[param] = value

			mongodb_rc = {k: v for k, v in mongodb_rc.items() if v is not None} 	# remove unset kwargs
		
			self._client  =  AsyncIOMotorClient(**mongodb_rc)
						
			# assert self._client is not None, "Failed to initialize Mongo DB connection"
			self._config = mongodb_rc
			self.db = self._client[self._database]
	
			debug('Mongo DB connection succeeded')
		except FileNotFoundError as err:
			error(f'{err}')
			raise err
		except Exception as err:
			error(f'Error connecting Mongo DB: {err}')
			raise err


	def copy(self, config : ConfigParser | None = None,**kwargs) -> Optional['Backend']:
		"""Create a copy of the backend"""
		try:
			debug('starting')
			for param, value in kwargs.items():
				self._config[param] = value
			return MongoBackend(config=config, **self._config)
		except Exception as err:
			error(f'Error creating copy: {err}')
		return None


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



	@classmethod
	def add_args_import(cls, parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
		"""Add argument parser for import backend"""
		try:
			debug('starting')
			super().add_args_import(parser=parser, config=config)

			parser.add_argument('--server-url', metavar='SERVER-URL', type=str, default=None, dest='import_host',
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
	def read_args(cls, args : Namespace, backend: str) -> dict[str, Any]:
		debug('starting')
		if backend != cls.driver:
			raise ValueError(f'calling {cls}.read_args() for {backend} backend')
		kwargs : dict[str, Any] = dict()
		if args.import_host is not None:
			kwargs['host'] = args.import_host
		if args.import_database is not None:
			kwargs['database'] = args.import_database
		debug(f'args={kwargs}')
		return kwargs	
	

	@property
	def backend(self) -> str:
		host : str = 'UNKNOWN'
		try:
			host, port = self._client.address
			return f'{self.driver}://{host}:{port}/{self.database}'
		except Exception as err:
			error(f'Error determing host')
		return f'{self.driver}://{host}/{self.database}'


	def __eq__(self, __o: object) -> bool:
		return __o is not None and isinstance(__o, MongoBackend) and \
					self._client.address == __o._client.address and \
					self.database == __o.database


	async def init(self) -> bool:
		"""Init MongoDB backend: create collections and set indexes"""
		try:
			debug('starting')

			DBC : str = ''
			dbc : AsyncIOMotorCollection

			try:
				DBC = self.table_accounts
				try:
					self.db.create_collection('DBC')
					debug(f'Collection created: {DBC}')
				except CollectionInvalid:
					debug(f'Collection exists: {DBC}')
				dbc = self.db[DBC]

				verbose(f'Adding index: {DBC}: [ region, inactive, disabled]')
				await dbc.create_index([ ('r', DESCENDING), ('i', DESCENDING), 
										 ('d', DESCENDING) ], background=True)
			except Exception as err:
				error(f'{self.backend}: Could not init collection {DBC} for accounts: {err}')	
			
			try:
				DBC = self.table_releases
				try:
					self.db.create_collection('DBC')
					debug(f'Collection created: {DBC}')
				except CollectionInvalid:
					debug(f'Collection exists: {DBC}')
				dbc = self.db[DBC]

				verbose(f'Adding index: {DBC}: [ release: -1, launch_date: -1]')
				await dbc.create_index([ ('_id', DESCENDING), ('launch_date', DESCENDING)], background=True)
			except Exception as err:
				error(f'{self.backend}: Could not init collection {DBC} for releases: {err}')

			try:
				DBC = self.table_replays
				try:
					self.db.create_collection('DBC')
					debug(f'Collection created: {DBC}')
				except CollectionInvalid:
					debug(f'Collection exists: {DBC}')
				dbc = self.db[DBC]

				verbose(f'Adding index: {DBC}: [ data.summary.protagonist, data.summary.room_type, data.summary.vehicle_tier]')
				await dbc.create_index([ ('d.s.p', DESCENDING), ('d.s.rt', DESCENDING), 
										 ('d.s.vx', DESCENDING) ], background=True)
			except Exception as err:
				error(f'{self.backend}: Could not init collection {DBC} for replays: {err}')

			try:
				DBC = self.table_tank_stats
				try:
					self.db.create_collection('DBC')
					debug(f'Collection created: {DBC}')
				except CollectionInvalid:
					debug(f'Collection exists: {DBC}')
				dbc = self.db[DBC]

				verbose(f'Adding index: {DBC}: [ account_id, tank_id, last_battle_time]')
				await dbc.create_index([ ('a', DESCENDING), ('t', DESCENDING),
										 ('lb', DESCENDING) ], background=True)
				verbose(f'Adding index: {DBC}: [ region, account_id, last_battle_time]')
				await dbc.create_index([ ('r', DESCENDING), ('a', DESCENDING), 
										 ('lb', DESCENDING) ], background=True)
			except Exception as err:
				error(f'{self.backend}: Could not init collection {DBC} for tank_stats: {err}')

			try:
				DBC = self.table_player_achievements
				try:
					self.db.create_collection('DBC')
					debug(f'Collection created: {DBC}')
				except CollectionInvalid:
					debug(f'Collection exists: {DBC}')
				dbc = self.db[DBC]

				verbose(f'Adding index: {DBC}: [ account_id, updated]')
				# await dbc.create_index([ ('a', DESCENDING), ('u', DESCENDING)], background=True)
			except Exception as err:
				error(f'{self.backend}: Could not init collection {DBC} for player_achievements: {err}')


		except Exception as err:
			error(f'Error initializing {self.backend}: {err}')
		return False


########################################################
# 
# MongoBackend(): generic data_funcs
#
########################################################


	# async def _data_get1(self, dbc : AsyncIOMotorCollection, 
	# 					data_type: type[BaseModel], 
	# 					id: Any) -> Optional[BaseModel]:
	# 	"""Generic method to get one object of data_type. Requires cast() on the caller side."""
	# 	try:
	# 		debug('starting')			
	# 		return data_type.parse_obj(await dbc.find_one({ '_id': id}))
	# 	except ValidationError as err:
	# 		error(f'Could not validate document from {self.backend}.{dbc.name}: {err}')
	# 		await self.error_log(MongoErrorLog(table=dbc.name, doc_id=id, type=ErrorLogType.ValidationError))
	# 	except Exception as err:
	# 		error(f'{self.backend}: {err}')
	# 	return None

	
	async def _data_insert(self, dbc : AsyncIOMotorCollection, data: D) -> bool:  		# type: ignore
		"""Generic method to get one object of data_type"""
		try:
			debug('starting')
			res : InsertOneResult = await dbc.insert_one(data.obj_db())
			debug(f'Inserted {type(data)} (_id={res.inserted_id}) into {self.backend}.{dbc.name}: {data}')
			return True			
		except Exception as err:
			debug(f'Failed to insert {type(data)}={data} into {self.backend}.{dbc.name}: {err}')	
		return False

	
	async def _data_get(self, dbc : AsyncIOMotorCollection, 
						data_type: type[D], 
						id: I) -> Optional[D]:
		"""Generic method to get one object of data_type"""
		try:
			debug('starting')
			res : Any = await dbc.find_one({ '_id': id})
			if res is not None:
				return data_type.parse_obj(res)
			else: 
				return None
		except ValidationError as err:
			error(f'Could not validate {type(data_type)} _id={id} from {self.backend}.{dbc.name}: {err}')
			await self.error_log(MongoErrorLog(table=dbc.name, doc_id=id, type=ErrorLogType.ValidationError))
		except Exception as err:
			error(f'Error getting {type(data_type)} _id={id} from {self.backend}.{dbc.name}: {err}')
		return None


	async def _data_update(self, dbc : AsyncIOMotorCollection, id: I, update: dict) -> bool:
		"""Generic method to update an object of data_type"""
		try:
			debug('starting')
			model = self.get_model(dbc.name)			
			alias_fields : dict[str, Any] = alias_mapper(model, update)
			if (res := await dbc.find_one_and_update({ '_id': id}, { '$set': alias_fields})) is None:
				debug(f'Failed to update _id={id} into {self.backend}.{dbc.name}')
				return False
			debug(f'Updated (_id={id}) into {self.backend}.{dbc.name}')
			return True			
		except Exception as err:
			debug(f'Error while updating _id={id} in {self.backend}.{dbc.name}: {err}')	
		return False

	
	async def _data_replace(self, dbc : AsyncIOMotorCollection, data: D, 	# type: ignore
							id: I, upsert : bool = False) -> bool: 
		"""Generic method to update an object of data_type"""		
		try:
			debug('starting')			
			if (res := await dbc.find_one_and_replace({ '_id': id}, data.obj_db(), upsert=upsert)) is None:
				debug(f'Failed to replace _id={id} into {self.backend}.{dbc.name}')
				return False
			debug(f'Replaced (_id={id}) into {self.backend}.{dbc.name}')
			return True			
		except Exception as err:
			debug(f'Error while replacing _id={id} in {self.backend}.{dbc.name}: {err}')	
		return False


	async def _data_delete(self, dbc : AsyncIOMotorCollection, id: I) -> bool:
		"""Generic method to delete an object of data_type"""
		try:
			debug('starting')
			if (res := await dbc.delete_one({ '_id': id})) == 1:
				debug(f'Delete (_id={id}) from {self.backend}.{dbc.name}')
				return True
			else:
				debug(f'Failed to delete _id={id} from {self.backend}.{dbc.name}')
		except Exception as err:
			debug(f'Error while deleting _id={id} from {self.backend}.{dbc.name}: {err}')	
		return False


	async def _datas_insert(self, dbc : AsyncIOMotorCollection, datas: Iterable[D]) -> tuple[int, int]:
		"""Store data to the backend. Returns the number of added and not added"""
		debug('starting')
		added		: int = 0
		not_added 	: int = 0
		try:
			res : InsertManyResult = await dbc.insert_many( (data.obj_db() for data in datas), 
															ordered=False)
			added = len(res.inserted_ids)
		except BulkWriteError as err:
			if err.details is not None:
				added = err.details['nInserted']
				not_added = len(err.details["writeErrors"])
				debug(f'Added {added}, could not add {not_added} entries to {self.backend}.{dbc.name}')
			else:
				error('BulkWriteError.details is None')
		except Exception as err:
			error(f'Unknown error when adding  entries to {self.backend}.{dbc.name}: {err}')
		return added, not_added


	async def _datas_update(self, dbc : AsyncIOMotorCollection, 
							datas: list[D], upsert: bool = False) -> tuple[int, int]:
		"""Store data to the backend. Returns number of documents inserted and not inserted"""
		debug('starting')
		updated			: int = 0
		not_updated 	: int = len(datas)
		try:
			res : UpdateResult
			res = await dbc.update_many( (d.obj_db() for d in datas), 
										  upsert=upsert, ordered=False)
			updated = res.modified_count
			not_updated -= updated
		except Exception as err:
			error(f'Unknown error when updating tank stats: {err}')
		return updated, not_updated
	

	async def _datas_get(self, dbc : AsyncIOMotorCollection, 
						data_type: type[D], 
						pipeline : list[dict[str, Any]]) -> AsyncGenerator[D, None]:
		try:
			debug('starting')
			async for obj in dbc.aggregate(pipeline, allowDiskUse=True):
				try:
					yield data_type.parse_obj(obj)
				except ValidationError as err:
					error(f'Could not validate {data_type} ob={obj} from {self.backend}.{dbc.name}: {err}')
				except Exception as err:
					error(f'{err}')
		except Exception as err:
			error(f'Error fetching {data_type} from {self.backend}.{dbc.name}: {err}')


	async def _datas_count(self, dbc : AsyncIOMotorCollection, 
							pipeline : list[dict[str, Any]]) -> int:
		try:
			debug('starting')
			pipeline.append({ '$count': 'total' })
			async for res in dbc.aggregate(pipeline, allowDiskUse=True):
				return int(res['total'])
		except Exception as err:
			error(f'Error counting documents in {self.backend}.{dbc.name}: {err}')
		return -1

	
	async def _datas_export(self, dbc : AsyncIOMotorCollection,
							in_type: type[D],
							out_type: type[O], 
							sample : float = 0) -> AsyncGenerator[O, None]:
		"""Export data from Mongo DB"""
		try:
			debug('starting')
			pipeline : list[dict[str, Any]] = list()
			if sample > 0 and sample < 1:
				N : int = await dbc.estimated_document_count()
				pipeline.append({ '$sample' : { 'size' : int(N * sample) }})
			elif sample >= 1:
				pipeline.append({ '$sample' : { 'size' : int(sample) }})
						
			async for obj in dbc.aggregate(pipeline, allowDiskUse=True):
				try:
					obj_in = in_type.parse_obj(obj)
					debug(f'Read {obj_in} from {self.backend}.{dbc.name}')   # comment out
					yield out_type.parse_obj(obj_in.obj_db())
				except Exception as err:
					error(f'{err}')
					continue
		except Exception as err:
			error(f'Error fetching data from {self.backend}.{dbc.name}: {err}')	


########################################################
# 
# MongoBackend(): account
#
########################################################
		

	# Original method
	# async def account_get(self, account_id: int) -> BSAccount | None:
	# 	"""Get account from backend"""
	# 	try:
	# 		debug('starting')
	# 		# DBC : str = self.table_accounts
	# 		dbc : AsyncIOMotorCollection = self.collection_accounts]
	# 		return BSAccount.parse_obj(await dbc.find_one({'_id': account_id}))
	# 	except ValidationError as err:
	# 		error(f'Could not validate account_id={account_id} from {self.name}: {self}')
	# 		await self.error_log(MongoErrorLog(table=self.table_accounts, doc_id=account_id, type=ErrorLogType.ValidationError))
		
	# 	except Exception as err:
	# 		error(f'Error fetching account_id: {account_id}) from {self.backend}.{self.table_accounts}: {err}')
				
	# 	return None

	# async def account_get1(self, account_id: int) -> BSAccount | None:
	# 	"""Get account from backend"""
		
	# 	try:
	# 		debug('starting')
	# 		return cast(BSAccount | None, await self._data_get1(self.collection_accounts], BSAccount, id=account_id))
	# 	except Exception as err:
	# 		error(f'{self.backend}: {err}')
	# 	return None


	async def account_insert(self, account: BSAccount) -> bool:
		"""Store account to the backend. Returns False 
			if the account was not added"""
		debug('starting')
		return await self._data_insert(self.collection_accounts, account)
		

	async def account_get(self, account_id: int) -> BSAccount | None:
		"""Get account from backend"""
		debug('starting')
		return await self._data_get(self.collection_accounts, BSAccount, id=account_id)


	async def account_update(self, account: BSAccount, 
							 update: dict[str, Any] | None = None, 
							 fields: list[str] | None = None) -> bool:
		"""Update an account in the backend. Returns False 
			if the account was not updated"""
		try: 
			debug('starting')
			if update is not None:
				pass
			elif fields is not None:
				update = account.obj_db(fields=fields)
			else:
				return False
			return await self._data_update(self.collection_accounts, id=account.id, update=update)
		except Exception as err:
			debug(f'Error while updating account (id={account.id}) into {self.backend}.{self.table_accounts}: {err}')	
		return False


	async def account_replace(self, account: BSAccount, upsert: bool = True) -> bool:
		"""Update an account in the backend. Returns False 
			if the account was not updated"""
		debug('starting')
		return await self._data_replace(self.collection_accounts, data=account,
										id=account.id, upsert=upsert)


	async def account_delete(self, account_id: int) -> bool:
		"""Deleta account from MongoDB backend"""
		debug('starting')
		return await self._data_delete(self.collection_accounts, id=account_id)


	

	# async def accounts_get(self, stats_type : StatsTypes | None = None, 
	## 						regions: set[Region] = Region.API_regions(), 
	# 						inactive : OptAccountsInactive = OptAccountsInactive.default(), 	
	# 						disabled: bool = False, 
	# 						dist : OptAccountsDistributed | None = None, sample : float = 0, 
	# 						cache_valid: int | None = None ) -> AsyncGenerator[BSAccount, None]:
	# 	"""Get accounts from Mongo DB
	# 		inactive: true = only inactive, false = not inactive, none = AUTO"""
	# 	try:
	# 		debug('starting')
	# 		NOW = epoch_now()	
	# 		dbc : AsyncIOMotorCollection = self.collection_accounts]
	# 		pipeline : list[dict[str, Any]] | None 
	# 		pipeline = await self._accounts_mk_pipeline(stats_type=stats_type, regions=regions, 
	# 													inactive=inactive, disabled=disabled, 
	# 													dist=dist, sample=sample, 
	# 													cache_valid=cache_valid)

	# 		update_field : str | None = None
	# 		if stats_type is not None:
	# 			update_field = stats_type.value

	# 		if pipeline is None:
	# 			raise ValueError(f'could not create get-accounts {self.name} cursor')
						
	# 		async for account_obj in dbc.aggregate(pipeline, allowDiskUse=True):
	# 			try:
	# 				player = BSAccount.parse_obj(account_obj)
	# 				# if not force and not disabled and inactive is None and player.inactive:
	# 				if not disabled and inactive == OptAccountsInactive.auto and player.inactive:
	# 					assert update_field is not None, "automatic inactivity detection requires stat_type"
	# 					updated = getattr(player, update_field)
	# 					if (NOW - updated) < min(MAX_UPDATE_INTERVAL, (updated - player.last_battle_time)/2):
	# 						continue
	# 				yield player
	# 			except Exception as err:
	# 				error(f'{err}')
	# 	except Exception as err:
	# 		error(f'Error fetching accounts from {self.backend}.{self.table_accounts}: {err}')	

	async def _mk_pipeline_accounts(self, stats_type : StatsTypes | None = None, 
							regions: set[Region] = Region.API_regions(), 
							inactive : OptAccountsInactive = OptAccountsInactive.default(), 
							dist : OptAccountsDistributed | None = None,
							disabled: bool|None = False, sample : float = 0, 
							cache_valid: int | None = None) -> list[dict[str, Any]] | None:
		assert sample >= 0, f"'sample' must be >= 0, was {sample}"
		try:
			debug('starting')
			a = AliasMapper(BSAccount)
			alias : Callable = a.alias

			if cache_valid is None:
				cache_valid = self._cache_valid
			update_field : str | None = None
			if stats_type is not None:
				update_field = alias(stats_type.value)
			dbc : AsyncIOMotorCollection = self.collection_accounts
			match : list[dict[str, str|int|float|dict|list]] = list()
			pipeline : list[dict[str, Any]] = list()
			
			# Pipeline build based on ESR rule
			# https://www.mongodb.com/docs/manual/tutorial/equality-sort-range-rule/#std-label-esr-indexing-rule



			if disabled:
				match.append({ alias('disabled') : True })
			elif inactive == OptAccountsInactive.yes:
				match.append({ alias('inactive'): True })
			
			if regions != Region.has_stats():
				match.append({ alias('region') : { '$in' : [ r.value for r in regions ]} })

			match.append({ alias('id') : {  '$lt' : WG_ACCOUNT_ID_MAX}})  # exclude Chinese account ids
			
			if dist is not None:
				match.append({ alias('id') : {  '$mod' :  [ dist.div, dist.mod ]}})			
	
			if disabled is not None and not disabled:
				match.append({ alias('disabled'): { '$ne': True }})
				# check inactive only if disabled == False
				if inactive == OptAccountsInactive.auto:
					assert update_field is not None, "automatic inactivity detection requires stat_type"
					match.append({ '$or': [ { update_field: None}, { update_field: { '$lt': epoch_now() - cache_valid }} ] })				
				elif inactive == OptAccountsInactive.no:
					match.append({ alias('inactive'): { '$ne': True }})

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


	async def accounts_get(self, stats_type : StatsTypes | None = None, 
							regions: set[Region] = Region.API_regions(), 
							inactive : OptAccountsInactive = OptAccountsInactive.default(), 	
							disabled : bool = False, 
							dist : OptAccountsDistributed | None = None, sample : float = 0, 
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
				raise ValueError(f'could not create get-accounts {self.backend}.{self.table_accounts} cursor')

			async for player in self._datas_get(self.collection_accounts, BSAccount, pipeline):
				try:					
					# if not force and not disabled and inactive is None and player.inactive:
					if not disabled and inactive == OptAccountsInactive.auto and player.inactive:
						assert update_field is not None, "automatic inactivity detection requires stat_type"
						updated = getattr(player, update_field)
						if (NOW - updated) < min(MAX_UPDATE_INTERVAL, (updated - player.last_battle_time)/2):
							continue
					yield player
				except Exception as err:
					error(f'{err}')
		except Exception as err:
			error(f'Error fetching accounts from {self.backend}.{self.table_accounts}: {err}')	


	async def accounts_count(self, stats_type : StatsTypes | None = None, 
							regions: set[Region] = Region.API_regions(), 
							inactive : OptAccountsInactive = OptAccountsInactive.default(), 	
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
					raise ValueError(f'Could not create pipeline for accounts {self.backend}.{dbc.name}')
				return await self._datas_count(dbc, pipeline)
		except Exception as err:
			error(f'counting accounts failed: {err}')
		return -1


	async def accounts_export(self, data_type: type[Account] = BSAccount, 
								sample : float = 0) -> AsyncGenerator[BSAccount, None]:
		"""Import accounts from Mongo DB"""
		debug('starting')
		async for account in self._datas_export(self.collection_accounts, 
												in_type=data_type, 
												out_type=BSAccount, 
												sample=sample):
			yield account


	# async def accounts_insert(self, accounts: Iterable[BSAccount]) -> tuple[int, int]:
	# 	"""Store account to the backend. Returns False 
	# 		if the account was not added"""
	# 	debug('starting')
	# 	added		: int = 0
	# 	not_added 	: int = 0
	# 	try:
	# 		# DBC : str = self.table_accounts
	# 		dbc : AsyncIOMotorCollection = self.collection_accounts]
	# 		res : InsertManyResult
			
	# 		# for account in accounts:
	# 		# 	# modifying Iterable items is OK since the item object ref stays the sam
	# 		# 	account.added = epoch_now()   

	# 		res = await dbc.insert_many( (account.obj_db() for account in accounts), 
	# 									  ordered=False)
	# 		added = len(res.inserted_ids)
	# 	except BulkWriteError as err:
	# 		if err.details is not None:
	# 			added = err.details['nInserted']
	# 			not_added = len(err.details["writeErrors"])
	# 			debug(f'Added {added}, could not add {not_added} accounts')
	# 		else:
	# 			error('BulkWriteError.details is None')
	# 	except Exception as err:
	# 		error(f'Unknown error when adding acconts to {self.name}:{self.database}.{self.table_accounts}: {err}')
	# 	return added, not_added


	async def accounts_insert(self, accounts: Iterable[BSAccount]) -> tuple[int, int]:
		"""Store account to the backend. Returns the number of added and not added"""
		debug('starting')
		return await self._datas_insert(self.collection_accounts, accounts)

########################################################
# 
# MongoBackend(): player_achievements
#
########################################################


	async def player_achievement_insert(self, player_achievement: WGplayerAchievementsMaxSeries) -> bool:		
		"""Insert a single player achievement"""
		debug('starting')
		return await self._data_insert(self.collection_player_achievements, player_achievement)


	async def player_achievement_get(self, account: BSAccount, added: int) -> WGplayerAchievementsMaxSeries | None:
		"""Return a player_achievement from the backend"""
		try:
			debug('starting')
			_id : ObjectId = WGplayerAchievementsMaxSeries.mk_id(account.id, region=account.region, added=added)
			return await self._data_get(self.collection_player_achievements, WGplayerAchievementsMaxSeries, id=_id)
		except Exception as err:
			error(f'Unknown error: {err}')
		return None


	async def player_achievement_delete(self, account: BSAccount, added: int) -> bool:
		"""Delete a player achievements from the backend"""
		try:
			debug('starting')
			_id : ObjectId = WGplayerAchievementsMaxSeries.mk_id(account.id, region=account.region, added=added)
			return await self._data_delete(self.collection_player_achievements, id=_id)
		except Exception as err:
			error(f'Unknown error: {err}')
		return False


	async def player_achievements_insert(self, player_achievements: Iterable[WGplayerAchievementsMaxSeries]) -> tuple[int, int]:
		"""Store player achievements to the backend. Returns number of stats inserted and not inserted"""
		debug('starting')
		return await self._datas_insert(self.collection_player_achievements, player_achievements)


	async def _mk_pipeline_player_achievements(self, release: BSBlitzRelease|None = None, 
										regions: set[Region] = Region.API_regions(), 
										accounts: Iterable[Account] | None = None,
										sample: float = 0) -> list[dict[str, Any]] | None:
		assert sample >= 0, f"'sample' must be >= 0, was {sample}"
		try:
			debug('starting')
			
			# class WGplayerAchievementsMaxSeries(JSONExportable):
			# 	id 			: ObjectId | None	= Field(default=None, alias='_id')
			# 	jointVictory: int 				= Field(default=0, alias='jv')
			# 	account_id	: int		 		= Field(default=0, alias='a')	
			## 	region		: Region | None 	= Field(default=None, alias='r')
			# 	release 	: str  | None 		= Field(default=None, alias='u')
			# 	added		: int 				= Field(default=epoch_now(), alias='t')

			a = AliasMapper(WGplayerAchievementsMaxSeries)
			alias : Callable = a.alias

			dbc : AsyncIOMotorCollection = self.collection_player_achievements
			pipeline : list[dict[str, Any]] = list()
			match : list[dict[str, str|int|float|dict|list]] = list()
			
			# Pipeline build based on ESR rule
			# https://www.mongodb.com/docs/manual/tutorial/equality-sort-range-rule/#std-label-esr-indexing-rule

			if regions != Region.has_stats():
				match.append({ alias('region') : { '$in' : [ r.value for r in regions ]} })
			if accounts is not None:
				match.append({ alias('account_id'): { '$in': [ a.id for a in accounts ]}})	
			if release is not None:
				match.append({ alias('release'): release.release })

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
							sample : float = 0) -> AsyncGenerator[WGplayerAchievementsMaxSeries, None]:
		"""Return player achievements from the backend"""
		try:
			debug('starting')
			pipeline : list[dict[str, Any]] | None 
			pipeline = await self._mk_pipeline_player_achievements(release=release, regions=regions, 
														accounts=accounts, 
														sample=sample)			
			if pipeline is None:
				raise ValueError(f'could not create pipeline for get player achievements {self.backend}')

			async for pa in self._datas_get(self.collection_player_achievements, WGplayerAchievementsMaxSeries, pipeline):
				yield pa
		except Exception as err:
			error(f'Error fetching player achievements from {self.backend}.{self.table_player_achievements}: {err}')	


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
					raise ValueError(f'could not create pipeline for player achievements {self.backend}.{dbc.name}')
				return await self._datas_count(dbc, pipeline)
		except Exception as err:
			error(f'counting player achievements failed: {err}')
		return -1


	async def player_achievements_update(self, player_achievements: list[WGplayerAchievementsMaxSeries], upsert: bool = False) -> tuple[int, int]:
		"""Update or upsert player achievements to the backend. Returns number of stats updated and not updated"""
		debug('starting')
		return await self._datas_update(self.collection_player_achievements, 
										datas=player_achievements, upsert=upsert)


	async def player_achievements_export(self, data_type: type[WGplayerAchievementsMaxSeries] = WGplayerAchievementsMaxSeries, 
								sample: float = 0) -> AsyncGenerator[WGplayerAchievementsMaxSeries, None]:
		"""Export player achievements from Mongo DB"""
		async for pa in self._datas_export(self.collection_tank_stats, 
												in_type=data_type, 
												out_type=WGplayerAchievementsMaxSeries, 
												sample=sample):
			yield pa


########################################################
# 
# MongoBackend(): releases
#
########################################################


	async def release_get(self, release : str) -> BSBlitzRelease | None:
		"""Get release from backend"""
		debug('starting')
		release = WGBlitzRelease.validate_release(release)
		return await self._data_get(self.collection_releases, BSBlitzRelease, id=release)


	async def release_get_latest(self) -> BSBlitzRelease | None:
		"""Get the latest release in the backend"""
		debug('starting')
		rel : BSBlitzRelease | None = None
		try:
			dbc : AsyncIOMotorCollection = self.collection_releases
			async for r in dbc.find().sort('launch_date', DESCENDING):
				rel = BSBlitzRelease.parse_obj(r)
				if rel is not None:
					return rel			
		except ValidationError as err:
			error(f'Incorrect data format: {err}')
		except Exception as err:
			error(f'Could not find the latest release from {self.backend}.{self.table_releases}: {err}')
		return None


	async def release_get_current(self) -> BSBlitzRelease | None:
		"""Get the latest release in the backend"""
		debug('starting')		
		try:
			
			dbc : AsyncIOMotorCollection = self.collection_releases
			async for r in dbc.find({ 'launch_date': { '$lte': date.today() } }).sort('launch_date', ASCENDING):
				return BSBlitzRelease.parse_obj(r)
		except ValidationError as err:
			error(f'Incorrect data format: {err}')
		except Exception as err:
			error(f'Could not find the latest release: {err}')
		return None


	async def release_insert(self, release: BSBlitzRelease) -> bool:
		"""Insert new release to the backend"""
		debug('starting')
		return await self._data_insert(self.collection_releases, data=release)


	async def release_delete(self, release: str) -> bool:
		"""Delete a release from backend"""
		debug('starting')
		release = WGBlitzRelease.validate_release(release)
		return await self._data_delete(self.collection_releases, id=release)


	async def release_update(self, release: BSBlitzRelease, 
								update: dict[str, Any] | None = None, 
								fields: list[str] | None= None) -> bool:
		"""Update an release in the backend. Returns False 
			if the release was not updated"""
		try: 
			debug('starting')
			if update is not None:
				pass
			elif fields is not None:
				update = release.obj_db(fields=fields)
			else:
				return False
			return await self._data_update(self.collection_releases, id=release.release, update=update)
		except Exception as err:
			debug(f'Error while updating release {release.release} into {self.backend}.{self.table_accounts}: {err}')	
		return False



	async def release_replace(self, release: BSBlitzRelease, upsert=False) -> bool:
		"""Update an account in the backend. Returns False 
			if the account was not updated"""
		debug('starting')
		return await self._data_replace(self.collection_releases, data=release, 
										id=release.release, upsert=upsert)	


	# async def release_replace(self, release: BSBlitzRelease, upsert=False) -> bool:
	# 	"""Update an release in the backend. Returns False 
	# 		if the release was not updated"""
	# 	try:
	# 		debug('starting')
			
	# 		dbc : AsyncIOMotorCollection = self.collection_releases]
	# 		release : BSBlitzRelease | None
			
	# 		if (release := await self.release_get(release=update.release)) is None:
	# 			raise ValueError(f'Could not find release {update.release} from {self.name}')
	# 		release_dict : dict[str, Any] 	= release.dict(by_alias=False)
	# 		debug(f'release={release_dict}')
	# 		update_dict : dict[str, Any] 	= update.dict(by_alias=False, exclude_unset=True)
	# 		debug(f'update={update_dict}')
			
	# 		for key in update_dict.keys():
	# 			if key == 'release':
	# 				continue
	# 			release_dict[key] = update_dict[key]
			
	# 		await dbc.find_one_and_replace({ 'release': release.release }, BSBlitzRelease(**release_dict).obj_db(), upsert=upsert)
	# 		debug(f'Updated: {release}')
	# 		return True			
	# 	except Exception as err:
	# 		debug(f'Failed to update release={update.release} to {self.name}: {err}')	
	# 	return False
	

	async def _mk_pipeline_releases(self, release_match: str | None = None, 
							since : date | None = None, 
							first : BSBlitzRelease | None = None) -> list[dict[str, Any]]:
		"""Build aggregation pipeline for releases"""
		try:
			debug('starting')
			match : list[dict[str, str|int|float|datetime|dict|list]] = list()
			pipeline : list[dict[str, Any]] = list()
			a = AliasMapper(BSBlitzRelease)
			alias : Callable = a.alias
			if since is not None:
				match.append( { alias('launch_date'):  { '$gte': datetime.combine(since, datetime.min.time()) }})
			if first is not None:
				match.append( { alias('launch_date'):  { '$gte': first.launch_date }})
			if release_match is not None:
				match.append( { alias('release'): { '$regex' : '^' + release_match } } )
			
			if len(match) > 0:
				pipeline.append( { '$match' : { '$and' : match } })

			pipeline.append( { '$sort': { alias('launch_date'): ASCENDING } })
			debug(f'pipeline: {pipeline}')
			return pipeline
		except Exception as err:
			error(f'Error creating pipeline: {err}')
			raise err


	async def releases_get(self, release_match: str | None = None, 
							since : date | None = None, 
							first : BSBlitzRelease | None = None) -> AsyncGenerator[BSBlitzRelease, None]:
		assert since is None or first is None, 'Only one can be defined: since, first'
		debug('starting')
		
		try:
			pipeline : list[dict[str, Any]]
			pipeline = await self._mk_pipeline_releases(release_match=release_match, since=since, first=first)
			
			async for release in self._datas_get(self.collection_releases, 
													data_type=BSBlitzRelease, 
													pipeline=pipeline):
				yield release

		except Exception as err:
			error(f'Error getting releases: {err}')



	# async def releases_export(self, release_type: type[WGBlitzRelease] = BSBlitzRelease, 
	# 							sample : float = 0) -> AsyncGenerator[BSBlitzRelease, None]:
	# 	"""Import relaseses from Mongo DB"""
	# 	try:
	# 		debug('starting')
	# 		dbc : AsyncIOMotorCollection = self.collection_releases]
						
	# 		rel	: BSBlitzRelease
	# 		async for release_obj in dbc.find():
	# 			try:
	# 				release_in = release_type.parse_obj(release_obj)
	# 				debug(f'Read {release_in} from {self.database}.{self.table_releases}')
	# 				rel = BSBlitzRelease.parse_obj(release_in.obj_db())
	# 				yield rel
	# 			except Exception as err:
	# 				error(f'{err}')
	# 				continue			
	# 	except Exception as err:
	# 		error(f'Error exporting releases from {self.name}: {err}')	


	async def releases_export(self, data_type: type[WGBlitzRelease] = BSBlitzRelease, 
							sample : float = 0) -> AsyncGenerator[BSBlitzRelease, None]:
		"""Import relaseses from Mongo DB"""
		debug('starting')
		async for release in self._datas_export(self.collection_releases, 
												in_type=data_type, 
												out_type=BSBlitzRelease, 
												sample=sample):
			# debug(f'Exporting release {release}: {release.json_src()}')
			yield release





########################################################
# 
# MongoBackend(): replay
#
########################################################

	async def replay_insert(self, replay: WoTBlitzReplayJSON) -> bool:
		"""Store replay into backend"""
		debug('starting')
		return await self._data_insert(self.collection_replays, replay)


	# async def replay_get(self, replay_id: str | ObjectId) -> WoTBlitzReplayJSON | None:
	# 	"""Get a replay from backend based on replayID"""
	# 	try:
	# 		debug(f'Getting replay (id={replay_id} from {self.name})')

	# 		dbc : AsyncIOMotorCollection = self.collection_replays]
	# 		res : Any | None = await dbc.find_one({'_id': str(replay_id)})
	# 		if res is not None:
	# 			# replay : WoTBlitzReplayJSON  = WoTBlitzReplayJSON.parse_obj(res) 
	# 			# debug(replay.json_src())
	# 			return WoTBlitzReplayJSON.parse_obj(res)   # returns None if not found
	# 	except Exception as err:
	# 		debug(f'Error reading replay (id_: {replay_id}) from {self.name}:{self.database}.{self.table_replays}: {err}')	
	# 	return None
	


	async def replay_get(self, replay_id: str) -> WoTBlitzReplayJSON | None:
		"""Get replay from backend"""
		debug('starting')			
		return await self._data_get(self.collection_replays, WoTBlitzReplayJSON, id=replay_id)
		

	async def replay_delete(self, replay_id: str) -> bool:
		"""Delete a replay from backend"""
		debug('starting')
		return await self._data_delete(self.collection_replays, id=replay_id)	


	async def replays_insert(self, replays: Iterable[WoTBlitzReplayJSON]) ->  tuple[int, int]:
		"""Insert replays to MongoDB backend"""
		debug('starting')
		return await self._datas_insert(self.collection_replays, replays)


	async def _mk_pipeline_replays(self, since: date | None = None, sample : float = 0, 
									**summary_fields) -> list[dict[str, Any]]:
		"""Build pipeline for replays"""
		assert sample >= 0, f"'sample' must be >= 0, was {sample}"
		debug('starting')
		match : list[dict[str, str|int|float|dict|list]] = list()
		pipeline : list[dict[str, Any]] = list()
		dbc : AsyncIOMotorCollection = self.collection_replays
		a : AliasMapper = AliasMapper(WoTBlitzReplaySummary)
		alias : Callable = a.alias

		if since is not None:
			match.append( {'d.s.bts': { '$gte': datetime.combine(since, datetime.min.time()).timestamp() }})
		
		for sf, value in summary_fields.items():
			try:				
				match.append( { f'd.s.{alias(sf)}': value })
			except KeyError:
				error(f'No such a key in WoTBlitzReplaySummary(): {alias(sf)}')
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


	async def replays_get(self, since: date | None = None, sample : float = 0,
							**summary_fields) ->  AsyncGenerator[WoTBlitzReplayJSON, None]:
		"""Get replays from mongodb backend"""
		debug('starting')
		try:
			debug('starting')
			pipeline : list[dict[str, Any]] 
			pipeline = await self._mk_pipeline_replays(since=since, sample=sample, **summary_fields)
			async for replay in self._datas_get(self.collection_replays, WoTBlitzReplayJSON, pipeline):
				yield replay
		except Exception as err:
			error(f'Error exporting replays from {self.backend}.{self.table_replays}: {err}')	


	async def replays_count(self, since: date | None = None, sample : float = 0,
							**summary_fields) -> int:
		"""Count replays in backed"""
		try:
			debug('starting')
			pipeline : list[dict[str, Any]] = await self._mk_pipeline_replays(since=since, sample=sample, **summary_fields)
			return await self._datas_count(self.collection_replays, pipeline)

		except Exception as err:
			error(f'Error exporting replays from {self.backend}.{self.table_replays}: {err}')
		return -1

	# # replay fields that can be searched: protagonist, battle_start_timestamp, account_id, vehicle_tier
	# async def replays_export(self, replay_type: type[WoTBlitzReplayJSON] = WoTBlitzReplayJSON) -> AsyncGenerator[WoTBlitzReplayJSON, None]:
	# 	"""Export replays from Mongo DB"""
	# 	try:
	# 		debug('starting')			
	# 		dbc : AsyncIOMotorCollection = self.collection_replays]
			
	# 		async for replay_obj in dbc.find():
	# 			try:
	# 				replay_in = replay_type.parse_obj(replay_obj)
	# 				debug(f'Read {replay_in} from {self.database}.{self.table_replays}')
	# 				yield WoTBlitzReplayJSON.parse_obj(replay_in.obj_db())
	# 			except Exception as err:
	# 				error(f'{err}')
	# 				continue			
	# 	except Exception as err:
	# 		error(f'Error exporting replays from {self.name}: {err}')	


	async def replays_export(self, data_type: type[WoTBlitzReplayJSON] = WoTBlitzReplayJSON,
								sample: float = 0) -> AsyncGenerator[WoTBlitzReplayJSON, None]:
		"""Export replays from Mongo DB"""
		debug('starting')
		async for replay in self._datas_export(self.collection_replays, 
												in_type=data_type, 
												out_type=WoTBlitzReplayJSON, 
												sample=sample):
			yield replay


########################################################
# 
# MongoBackend(): tank_stats
#
########################################################

	async def tank_stat_insert(self, tank_stat: WGtankStat) -> bool:
		"""Insert a single tank stat"""
		debug('starting')
		return await self._data_insert(self.collection_tank_stats, tank_stat)


	async def tank_stat_get(self, account: BSAccount, tank_id: int, 
							last_battle_time: int) -> WGtankStat | None:
		"""Return tank stats from the backend"""
		try:
			debug('starting')
			_id : ObjectId = WGtankStat.mk_id(account.id, last_battle_time, tank_id)
			return await self._data_get(self.collection_tank_stats, WGtankStat, id=_id)
		except Exception as err:
			error(f'Unknown error: {err}')
		return None

	
	async def tank_stat_update(self, tank_stat: WGtankStat, 
							 update: dict[str, Any] | None = None, 
							 fields: list[str] | None = None) -> bool:
		"""Update an tank stat in the backend. Returns False 
			if the tank stat was not updated"""
		try: 
			debug('starting')
			if update is not None:
				pass
			elif fields is not None:
				# by_alias=False since _data_update remaps alias fields
				update = tank_stat.obj_db(by_alias=False, fields=fields)
			else:
				return False
			return await self._data_update(self.collection_tank_stats, id=tank_stat.id, update=update)
		except Exception as err:
			debug(f'Error while updating tank stat (id={tank_stat.id}) into {self.backend}.{self.table_tank_stats}: {err}')	
		return False


	async def tank_stat_delete(self, account: BSAccount, tank_id: int, 
								last_battle_time: int) -> bool:
		try:
			debug('starting')
			_id : ObjectId = WGtankStat.mk_id(account.id, last_battle_time, tank_id)						
			return await self._data_delete(self.collection_tank_stats, id=_id)
		except Exception as err:
			error(f'Unknown error: {err}')
		return False


	async def tank_stats_insert(self, tank_stats: Iterable[WGtankStat]) -> tuple[int, int]:
		"""Store tank stats to the backend. Returns the number of added and not added"""
		debug('starting')
		return await self._datas_insert(self.collection_tank_stats, tank_stats)


	async def _mk_pipeline_tank_stats(self, release: BSBlitzRelease|None = None, 
										regions: set[Region] = Region.API_regions(), 
										accounts: Iterable[Account] | None = None,
										tanks: Iterable[Tank] | None = None, 
										since:  datetime | None = None,
										sample: float = 0) -> list[dict[str, Any]] | None:
		assert sample >= 0, f"'sample' must be >= 0, was {sample}"
		try:
			debug('starting')

			# class WGtankStat(JSONExportable, JSONImportable):
			# id					: ObjectId | None = Field(None, alias='_id')
			# _region				: Region | None = Field(None, alias='r')
			# all					: WGtankStatAll = Field(..., alias='s')
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
			
			a = AliasMapper(WGtankStat)
			alias : Callable = a.alias
			dbc : AsyncIOMotorCollection = self.collection_tank_stats
			pipeline : list[dict[str, Any]] = list()
			match : list[dict[str, str|int|float|dict|list]] = list()
			
			# Pipeline build based on ESR rule
			# https://www.mongodb.com/docs/manual/tutorial/equality-sort-range-rule/#std-label-esr-indexing-rule

			if regions != Region.has_stats():
				match.append({ alias('region') : { '$in' : [ r.value for r in regions ]} })
			if accounts is not None:
				match.append({ alias('account_id'): { '$in': [ a.id for a in accounts ]}})
			if tanks is not None:
				match.append({ alias('tank_id'): { '$in': [ t.tank_id for t in tanks ]}})
			if release is not None:
				match.append({ alias('release'): release.release })
			if since is not None:
				match.append({ alias('last_battle_time'): { '$gte': since.timestamp() } })

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

	
	async def tank_stats_get(self, release: BSBlitzRelease | None = None,
							regions : set[Region] = Region.API_regions(), 
							accounts: Iterable[Account] | None = None,
							tanks 	: Iterable[Tank] | None = None, 
							since:  datetime | None = None,
							sample 	: float = 0) -> AsyncGenerator[WGtankStat, None]:
		"""Return tank stats from the backend"""
		try:
			debug('starting')
			pipeline : list[dict[str, Any]] | None 
			pipeline = await self._mk_pipeline_tank_stats(release=release, regions=regions, 
														tanks=tanks, accounts=accounts, 
														since=since, sample=sample)
			
			if pipeline is None:
				raise ValueError(f'could not create pipeline for get tank stats {self.backend}')

			async for tank_stat in self._datas_get(self.collection_tank_stats, 
													WGtankStat, pipeline):
				yield tank_stat
		except Exception as err:
			error(f'Error fetching tank stats from {self.backend}.{self.table_tank_stats}: {err}')	


	async def tank_stats_count(self, release: BSBlitzRelease | None = None,
							regions: set[Region] = Region.API_regions(), 
							accounts: Iterable[Account] | None = None,
							tanks: Iterable[Tank] | None = None, 
							since:  datetime | None = None,
							sample : float = 0) -> int:
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
					raise ValueError(f'could not create pipeline for tank stats {self.backend}.{dbc.name}')
				return await self._datas_count(dbc, pipeline)
		except Exception as err:
			error(f'counting tank stats failed: {err}')
		return -1


	# async def tank_stats_insert(self, tank_stats: Iterable[WGtankStat]) -> tuple[int, int]:
	# 	"""Store tank stats to the backend. Returns number of stats inserted and not inserted"""
	# 	debug('starting')
	# 	added			: int = 0
	# 	not_added 		: int = 0
	# 	# last_battle_time: int = -1

	# 	try:
	# 		dbc : AsyncIOMotorCollection = self.collection_tank_stats]
	# 		res : InsertManyResult
	# 		# last_battle_time = max( [ ts.last_battle_time for ts in tank_stats] )	
	# 		res = await dbc.insert_many( (tank_stat.obj_db() for tank_stat in tank_stats), 
	# 									  ordered=False)
	# 		added = len(res.inserted_ids)
	# 	except BulkWriteError as err:
	# 		if err.details is not None:
	# 			added = err.details['nInserted']
	# 			not_added = len(err.details["writeErrors"])
	# 			debug(f'Added {added}, could not add {not_added} tank stats')
	# 		else:
	# 			error('BulkWriteError.details is None')
	# 	except Exception as err:
	# 		error(f'Unknown error when adding tank stats: {err}')
	# 	return added, not_added  # , last_battle_time


	# async def tank_stats_update(self, tank_stats: list[WGtankStat], upsert: bool = False) -> tuple[int, int]:
	# 	"""Store tank stats to the backend. Returns number of stats inserted and not inserted"""
	# 	debug('starting')
	# 	updated			: int = 0
	# 	not_updated 	: int = 0
	# 	# last_battle_time: int = -1

	# 	try:
	# 		dbc : AsyncIOMotorCollection = self.collection_tank_stats]
	# 		res : UpdateResult
	# 		# last_battle_time = max( [ ts.last_battle_time for ts in tank_stats] )	
	# 		res = await dbc.update_many( (ts.obj_db() for ts in tank_stats), 
	# 									  upsert=upsert, ordered=False)
	# 		updated = res.modified_count
	# 		not_updated = len(tank_stats) - updated
		
	# 	except Exception as err:
	# 		error(f'Unknown error when updating tank stats: {err}')
	# 	return updated, not_updated


	async def tank_stats_update(self, tank_stats: list[WGtankStat], 
								upsert: bool = False) -> tuple[int, int]:
		"""Store tank stats to the backend. Returns number of stats inserted and not inserted"""
		debug('starting')
		return await self._datas_update(self.collection_tank_stats, 
										datas=tank_stats, upsert=upsert)



	async def tank_stats_export(self, data_type: type[WGtankStat] = WGtankStat, 
								sample: float = 0) -> AsyncGenerator[WGtankStat, None]:
		"""Export tank stats from Mongo DB"""
		debug('starting')
		async for tank_stat in self._datas_export(self.collection_tank_stats, 
												in_type=data_type, 
												out_type=WGtankStat, 
												sample=sample):
			yield tank_stat


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
			debug(f'Could not log error: {error.table}: "{error.msg}" into {self.backend}.{self.table_error_log}: {err}')	
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
					debug(f'Read "{err.msg}" from {self.backend}.{self.table_error_log}')
					
					yield err
				except Exception as e:
					error(f'{e}')
					continue			
		except Exception as e:
			error(f'Error getting errors from {self.backend}.{self.table_error_log}: {e}')	


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
			error(f'Error clearing errors from {self.backend}.{self.table_error_log}: {e}')
		return 0

# Register backend

debug('Registering mongodb')
Backend.register(name=MongoBackend.driver, backend=MongoBackend)