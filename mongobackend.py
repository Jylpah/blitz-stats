from configparser import ConfigParser
from argparse import Namespace, ArgumentParser
from datetime import date
from os.path import isfile
from typing import Optional, Any, Iterable, AsyncGenerator, TypeVar, cast
import logging

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCursor, AsyncIOMotorCollection # type: ignore
from pymongo.results import InsertManyResult, InsertOneResult, UpdateResult
from pymongo.errors import BulkWriteError
from pymongo import DESCENDING, ASCENDING
from pydantic import ValidationError

from backend import Backend, OptAccountsDistributed, OptAccountsInactive, \
					MAX_UPDATE_INTERVAL, WG_ACCOUNT_ID_MAX, CACHE_VALID
from models import BSAccount, BSBlitzRelease, StatsTypes
from pyutils.utils import epoch_now
from blitzutils.models import Region, WoTBlitzReplayJSON, WGtankStat, Account, Tank, WGBlitzRelease

# Setup logging
logger	= logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug

# Constants
TANK_STATS_BATCH	: int = 1000

##############################################
#
## class MongoBackend(Backend)
#
##############################################

class MongoBackend(Backend):

	name : str = 'mongodb'
	default_db : str = 'BlitzStats'
	

	def __init__(self, config: ConfigParser | None = None, 
					**kwargs):
		"""Init MongoDB backend from config file and CLI args
			CLI arguments overide settings in the config file"""	
		mongodb_rc 	: dict[str, Any] = dict()
		self._config : dict[str, Any]
		self._database 	: str 	= 'BlitzStats'
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
			# client 		: AsyncIOMotorClient | None = None
			self.db 	: AsyncIOMotorDatabase
			self.C 		: dict[str,str] = dict()

			# default collections
			self.C['ACCOUNTS'] 			= 'Accounts'
			self.C['TANKOPEDIA'] 		= 'Tankopedia'
			self.C['RELEASES'] 			= 'Releases'
			self.C['REPLAYS'] 			= 'Replays'
			self.C['TANK_STATS'] 		= 'TankStats'
			self.C['PLAYER_ACHIEVEMENTS'] = 'PlayerAchievements'

			if config is not None:
				if 'GENERAL' in config.sections():
					configGeneral = config['GENERAL']
					self._cache_valid 	= configGeneral.getint('cache_valid', CACHE_VALID) 
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

					self.C['ACCOUNTS'] 		= configMongo.get('c_accounts', 	self.C['ACCOUNTS'])
					self.C['TANKOPEDIA'] 	= configMongo.get('c_tankopedia', 	self.C['TANKOPEDIA'])
					self.C['RELEASES']		= configMongo.get('c_releases', 	self.C['RELEASES'])	
					self.C['REPLAYS'] 		= configMongo.get('c_replays', 		self.C['REPLAYS'])
					self.C['TANK_STATS']	= configMongo.get('c_tank_stats', 	self.C['TANK_STATS'])
					self.C['PLAYER_ACHIEVEMENTS'] 	= configMongo.get('c_player_achievements', 
																				self.C['PLAYER_ACHIEVEMENTS'])
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
			for param, value in kwargs.items():
				self._config[param] = value
			return MongoBackend(config=config, **self._config)
		except Exception as err:
			error(f'Error creating copy: {err}')
		return None


	@classmethod
	def add_args_import(cls, parser: ArgumentParser, config: Optional[ConfigParser] = None, 
							import_types: list[str] = list()) -> bool:
		"""Add argument parser for import backend"""
		try:
			debug('starting')
			super().add_args_import(parser=parser, config=config, import_types=import_types)

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
		if backend != cls.name:
			raise ValueError(f'calling {cls}.read_args() for {backend} backend')
		kwargs : dict[str, Any] = dict()
		if args.server_url is not None:
			kwargs['host'] = args.server_url
		if args.database is not None:
			kwargs['database'] = args.database
		return kwargs	
	

	def set_database(self, database : str) -> bool:
		"""Set database"""
		try:
			self.db = self._client[database]
			self._database = database
			return True
		except Exception as err:
			error(f'Error creating copy: {err}')
		return False

	@property
	def database(self) -> str:
		return self._database


	@property
	def table_accounts(self) -> str:
		return self.C['ACCOUNTS']


	@property
	def table_tank_stats(self) -> str:
		return self.C['TANK_STATS']


	@property
	def table_player_achievements(self) -> str:
		return self.C['PLAYER_ACHIEVEMENTS']


	@property
	def table_releases(self) -> str:
		return self.C['RELEASES']

	
	@property
	def table_replays(self) -> str:
		return self.C['REPLAYS']


	@property
	def table_tankopedia(self) -> str:
		return self.C['TANKOPEDIA']


	def set_table(self, table: str, new: str) -> bool:
		"""Set database"""
		try:
			assert table in self.C.keys(), f'Unknown collection {table}'
			self.C[table] = new	
			return True
		except Exception as err:
			error(f'Error creating copy: {err}')
		return False


	def __eq__(self, __o: object) -> bool:
		return __o is not None and isinstance(__o, MongoBackend) and \
					self._client.address == __o._client.address and \
					self.database == __o.database


	async def init(self) -> bool:
		"""Init MongoDB backend: create collections and set indexes"""
		try:
			# self.C['ACCOUNTS'] 			= 'Accounts'
			# self.C['TANKOPEDIA'] 			= 'Tankopedia'
			# self.C['RELEASES'] 			= 'Releases'
			# self.C['REPLAYS'] 			= 'Replays'
			# self.C['TANK_STATS'] 			= 'TankStats'
			# self.C['PLAYER_ACHIEVEMENTS'] = 'PlayerAchievements'

			DBC : str = 'NOT DEFINED'
			dbc : AsyncIOMotorCollection

			try:
				DBC = self.C['ACCOUNTS']
				dbc = self.db[DBC]

				verbose(f'Adding index: {DBC}: [ region, inactive, disabled]')
				await dbc.create_index([ ('r', DESCENDING), ('i', DESCENDING), 
										 ('d', DESCENDING) ], background=True)
			except Exception as err:
				error(f'{self.name}: Could not init collection {DBC} for accounts: {err}')	
			
			try:
				DBC = self.C['RELEASES']
				dbc = self.db[DBC]

				verbose(f'Adding index: {DBC}: [ release: -1, launch_date: -1]')
				await dbc.create_index([ ('release', DESCENDING), ('launch_date', DESCENDING)], background=True)
			except Exception as err:
				error(f'{self.name}: Could not init collection {DBC} for releases: {err}')

			try:
				DBC = self.C['REPLAYS']
				dbc = self.db[DBC]

				verbose(f'Adding index: {DBC}: [ data.summary.protagonist, data.summary.room_type, data.summary.vehicle_tier]')
				await dbc.create_index([ ('d.s.p', DESCENDING), ('d.s.rt', DESCENDING), 
										 ('d.s.vx', DESCENDING) ], background=True)
			except Exception as err:
				error(f'{self.name}: Could not init collection {DBC} for replays: {err}')

			try:
				DBC = self.C['TANK_STATS']
				dbc = self.db[DBC]

				verbose(f'Adding index: {DBC}: [ tank_id, account_id, last_battle_time]')
				await dbc.create_index([ ('a', DESCENDING), ('t', DESCENDING),
										 ('lb', DESCENDING) ], background=True)
			except Exception as err:
				error(f'{self.name}: Could not init collection {DBC} for tank_stats: {err}')

			try:
				DBC = self.C['PLAYER_ACHIEVEMENTS']
				dbc = self.db[DBC]

				verbose(f'Adding index: {DBC}: [ account_id, updated]')
				# await dbc.create_index([ ('a', DESCENDING), ('u', DESCENDING)], background=True)
			except Exception as err:
				error(f'{self.name}: Could not init collection {DBC} for player_achievements: {err}')


		except Exception as err:
			error(f'Error initializing {self.name}: {err}')
		return False


########################################################
# 
# MongoBackend(): account
#
########################################################


	async def account_get(self, account_id: int) -> BSAccount | None:
		"""Get account from backend"""
		try:
			DBC : str = self.C['ACCOUNTS']
			dbc : AsyncIOMotorCollection = self.db[DBC]
			return BSAccount.parse_obj(await dbc.find_one({'_id': account_id}))
		except Exception as err:
			error(f'Error fetching account_id: {account_id}) from {self.name}: {err}')	
		return None


	async def accounts_count(self, stats_type : StatsTypes | None = None, 
							regions: set[Region] = Region.API_regions(), 
							inactive : OptAccountsInactive = OptAccountsInactive.default(), 	
							disabled: bool = False, 
							dist : OptAccountsDistributed | None = None, sample : float = 0, 
							force : bool = False, cache_valid: int | None = None ) -> int:
		assert sample >= 0, f"'sample' must be >= 0, was {sample}"
		try:
			DBC : str = self.C['ACCOUNTS']
			dbc : AsyncIOMotorCollection = self.db[DBC]
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
				pipeline : list[dict[str, Any]] | None = await self._accounts_mk_pipeline(stats_type=stats_type, regions=regions, 
																		inactive=inactive, disabled=disabled, 
																		dist=dist, sample=sample, 
																		force=force, cache_valid=cache_valid)

				if pipeline is None:
					raise ValueError(f'could not create get-accounts {self.name} cursor')
				pipeline.append({ '$count': 'accounts' })
				cursor : AsyncIOMotorCursor = dbc.aggregate(pipeline, allowDiskUse=True)
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
							force : bool = False, cache_valid: int | None = None ) -> AsyncGenerator[BSAccount, None]:
		"""Get accounts from Mongo DB
			inactive: true = only inactive, false = not inactive, none = AUTO
		"""
		try:
			NOW = epoch_now()	
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
						
			async for account_obj in dbc.aggregate(pipeline, allowDiskUse=True):
				try:
					player = BSAccount.parse_obj(account_obj)
					if not force and not disabled and inactive is None and player.inactive:
						assert update_field is not None, "automatic inactivity detection requires stat_type"
						updated = dict(player)[update_field]
						if (NOW - updated) < min(MAX_UPDATE_INTERVAL, (updated - player.last_battle_time)/2):
							continue
					yield player
				except Exception as err:
					error(f'{err}')
					continue
		except Exception as err:
			error(f'Error fetching accounts from Mongo DB: {err}')	


	async def accounts_export(self, account_type: type[Account] = BSAccount, 
								regions: set[Region] = Region.has_stats(), 
								sample : float = 0) -> AsyncGenerator[BSAccount, None]:
		"""Import accounts from Mongo DB"""
		try:
			DBC : str = self.C['ACCOUNTS']
			dbc : AsyncIOMotorCollection = self.db[DBC]

			pipeline : list[dict[str, Any]] = list()
			if regions != Region.has_stats():
				pipeline.append({ '$match': { 'r': { '$in':  [ r.value for r in regions ] } }})
			
			if sample > 0 and sample < 1:
				N : int = await dbc.estimated_document_count()
				pipeline.append({ '$sample' : N * sample })
			elif sample >= 1:
				pipeline.append({ '$sample' : sample })
			
			account 	: BSAccount
			async for account_obj in dbc.aggregate(pipeline, allowDiskUse=True):
				try:
					account_in = account_type.parse_obj(account_obj)
					debug(f'Read {account_in} from {self.database}.{self.table_accounts}')
					account = BSAccount.parse_obj(account_in.obj_db())
					yield account
				except Exception as err:
					error(f'{err}')
					continue
		except Exception as err:
			error(f'Error fetching accounts from Mongo DB: {err}')	


	async def _accounts_mk_pipeline(self, stats_type : StatsTypes | None = None, 
							regions: set[Region] = Region.API_regions(), 
							inactive : OptAccountsInactive = OptAccountsInactive.default(), 
							dist : OptAccountsDistributed | None = None,
							disabled: bool|None = False, sample : float = 0, 
							force : bool = False, cache_valid: int | None = None) -> list[dict[str, Any]] | None:
		try:
			# id						: int		= Field(default=..., alias='_id')
			# region 					: Region | None= Field(default=None, alias='r')
			# last_battle_time			: int | None = Field(default=None, alias='l')
			# updated_tank_stats 		: int | None = Field(default=None, alias='ut')
			# updated_player_achievements : int | None = Field(default=None, alias='up')
			# added 					: int | None = Field(default=None, alias='a')
			# inactive					: bool | None = Field(default=None, alias='i')
			# disabled					: bool | None = Field(default=None, alias='d')
			
			NOW = epoch_now()
			DBC : str = self.C['ACCOUNTS']
			if cache_valid is None:
				cache_valid = self.cache_valid
			update_field : str | None = None
			if stats_type is not None:
				update_field = stats_type.value
			dbc : AsyncIOMotorCollection = self.db[DBC]
			match : list[dict[str, str|int|float|dict|list]] = list()
			
			# Pipeline build based on ESR rule
			# https://www.mongodb.com/docs/manual/tutorial/equality-sort-range-rule/#std-label-esr-indexing-rule

			if disabled:
				match.append({ 'd': True })
			elif inactive == OptAccountsInactive.yes:
				match.append({ 'i': True })
			
			if regions != Region.has_stats():
				match.append({ 'r' : { '$in' : [ r.value for r in regions ]} })

			match.append({ '_id' : {  '$lt' : WG_ACCOUNT_ID_MAX}})  # exclude Chinese account ids
			
			if dist is not None:
				match.append({ '_id' : {  '$mod' :  [ dist.div, dist.mod ]}})			
	
			if disabled is not None and not disabled:
				match.append({ 'd': { '$ne': True }})
				# check inactive only if disabled == False
				if inactive == OptAccountsInactive.auto:
					if not force:
						assert update_field is not None, "automatic inactivity detection requires stat_type"
						match.append({ '$or': [ { update_field: None}, { update_field: { '$lt': NOW - cache_valid }} ] })				
				elif inactive == OptAccountsInactive.no:
					match.append({ 'i': { '$ne': True }})

			pipeline : list[dict[str, Any]] = [ { '$match' : { '$and' : match } }]

			if sample >= 1:				
				pipeline.append({'$sample': {'size' : int(sample) } })
			elif sample > 0:
				n : int = cast(int, await dbc.estimated_document_count())
				pipeline.append({'$sample': {'size' : int(n * sample) } })
			return pipeline		
		except Exception as err:
			error(f'{err}')
		return None


	async def account_insert(self, account: BSAccount) -> bool:
		"""Store account to the backend. Returns False 
			if the account was not added"""
		try:
			DBC : str = self.C['ACCOUNTS']
			dbc : AsyncIOMotorCollection = self.db[DBC]
			account.added = epoch_now()
			res : InsertOneResult = await dbc.insert_one(account.obj_db())
			debug(f'Account add to {self.name}: {account.id}')
			return True			
		except Exception as err:
			debug(f'Failed to add account_id={account.id} to {self.name}: {err}')	
		return False
	
	
	async def account_update(self, account: BSAccount, upsert: bool = True) -> bool:
		"""Update an account in the backend. Returns False 
			if the account was not updated"""
		try:
			DBC : str = self.C['ACCOUNTS']
			dbc : AsyncIOMotorCollection = self.db[DBC]
			
			await dbc.find_one_and_replace({ '_id': account.id }, account.obj_db(), upsert=upsert)
			debug(f'Updated: {account}')
			return True			
		except Exception as err:
			debug(f'Failed to update account_id={account.id} to {self.name}: {err}')	
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

			res = await dbc.insert_many( (account.obj_db() for account in accounts), 
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
			error(f'Unknown error when adding acconts: {err}')
		return added, not_added

########################################################
# 
# MongoBackend(): releases
#
########################################################


	async def release_get(self, release : str) -> BSBlitzRelease | None:
		res : BSBlitzRelease | None = None
		try:
			DBC : str = self.C['RELEASES']
			dbc : AsyncIOMotorCollection = self.db[DBC]			
			res = BSBlitzRelease.parse_obj(await dbc.find({ 'release' : release }))
			if res is None:
				raise ValueError()
		except ValidationError as err:
			error(f'Incorrect data format: {err}')
		except Exception as err:
			error(f'Could not find release {release}: {err}')
		return res


	async def release_get_latest(self) -> BSBlitzRelease | None:
		"""Get the latest release in the backend"""
		rel : BSBlitzRelease | None = None
		try:
			DBC : str = self.C['RELEASES']
			dbc : AsyncIOMotorCollection = self.db[DBC]
			async for r in dbc.find().sort('launch_date', DESCENDING):
				rel = BSBlitzRelease.parse_obj(r)
				if rel is not None:
					return rel			
		except ValidationError as err:
			error(f'Incorrect data format: {err}')
		except Exception as err:
			error(f'Could not find the latest release: {err}')
		return None


	async def release_get_current(self) -> BSBlitzRelease | None:
		"""Get the latest release in the backend"""
		rel : BSBlitzRelease | None = None
		try:
			DBC : str = self.C['RELEASES']
			dbc : AsyncIOMotorCollection = self.db[DBC]
			async for r in dbc.find({ 'launch_date': { '$lte': date.today() } }).sort('launch_date', DESCENDING):
				rel = BSBlitzRelease.parse_obj(r)
				if rel is not None:
					if rel.cut_off == 0:
						return rel
					else:
						return None
		except ValidationError as err:
			error(f'Incorrect data format: {err}')
		except Exception as err:
			error(f'Could not find the latest release: {err}')
		return None


	async def releases_get(self, release: str | None = None, since : date | None = None, 
							first : BSBlitzRelease | None = None) -> list[BSBlitzRelease]:
		assert since is None or first is None, 'Only one can be defined: since, first'
		debug(f'release={release}, since={since}, first={first}')
		releases : list[BSBlitzRelease] = list()
		try:
			DBC : str = self.C['RELEASES']
			dbc : AsyncIOMotorCollection = self.db[DBC]
			query : dict[str, Any] = dict()
			if since is not None:
				query['launch_date'] = { '$gte': since }
			elif first is not None:
				query['launch_date'] = { '$gte': first.launch_date}
			
			if release is not None:
				query['release'] = { '$regex' : '^' + release }
			
			async for r in dbc.find(query).sort('launch_date', ASCENDING):
				rel : BSBlitzRelease|None = BSBlitzRelease.parse_obj(r)
				if rel is not None:
					releases.append(rel)
				else:
					error(f'Could not parse release: {r}')
		except Exception as err:
			error(f'Error getting releases: {err}')
		return releases


	async def release_insert(self, release: BSBlitzRelease) -> bool:
		"""Insert new release to the backend"""
		try:
			DBC : str = self.C['RELEASES']
			dbc : AsyncIOMotorCollection = self.db[DBC]
			res : InsertOneResult= dbc.insert_one(release)
			if res.inserted_id is None:
				error(f'Could not add release {release}')
				return False
			return True
		except Exception as err:
			error(f'Could not add release {release}: {err}')
		return False


	async def release_update(self, release: BSBlitzRelease, upsert: bool = True) -> bool:
		"""Update an release in the backend. Returns False 
			if the release was not updated"""
		try:
			DBC : str = self.C['RELEASES']
			dbc : AsyncIOMotorCollection = self.db[DBC]
			
			await dbc.find_one_and_replace({ 'release': release.release }, release.obj_db(), upsert=upsert)
			debug(f'Updated: {release}')
			return True			
		except Exception as err:
			debug(f'Failed to update release={release} to {self.name}: {err}')	
		return False

########################################################
# 
# MongoBackend(): replay
#
########################################################

	async def replay_insert(self, replay: WoTBlitzReplayJSON) -> bool:
		"""Store replay into backend"""
		try:
			DBC : str = self.C['REPLAYS']
			dbc : AsyncIOMotorCollection = self.db[DBC]
			await dbc.insert_one(replay.obj_db())
			return True
		except Exception as err:
			debug(f'Could not insert replay (_id: {replay.id}) into {self.name}: {err}')	
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
			debug(f'Error reading replay (id_: {replay_id}) from {self.name}: {err}')	
		return None
	

	# replay fields that can be searched: protagonist, battle_start_timestamp, account_id, vehicle_tier
	async def replay_find(self, **kwargs) -> AsyncGenerator[WoTBlitzReplayJSON, None]:
		"""Find a replay from backend based on search string"""
		raise NotImplementedError



########################################################
# 
# MongoBackend(): tank_stats
#
########################################################


	async def _tank_stats_mk_pipeline(self, release: BSBlitzRelease|None = None, 
										regions: set[Region] = Region.API_regions(), 
										tanks: list[Tank]| None = None, 
										accounts : list[Account] |None = None,
										sample: float = 0) -> list[dict[str, Any]] | None:
		assert sample >= 0, f"'sample' must be >= 0, was {sample}"
		try:
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
			
			NOW = epoch_now()
			DBC : str = self.C['TANK_STATS']
			dbc : AsyncIOMotorCollection = self.db[DBC]
			pipeline : list[dict[str, Any]] = list()
			match : list[dict[str, str|int|float|dict|list]] = list()
			
			# Pipeline build based on ESR rule
			# https://www.mongodb.com/docs/manual/tutorial/equality-sort-range-rule/#std-label-esr-indexing-rule

			if regions != Region.has_stats():
				match.append({ 'r' : { '$in' : [ r.value for r in regions ]} })
			if tanks is not None:
				match.append({ 't': { '$in': [ t.tank_id for t in tanks ]}})
			if accounts is not None:
				match.append({ 'a': { '$in': [ a.id for a in accounts ]}})

			if len(match) > 0:
				pipeline.append( { '$match' : { '$and' : match } })

			if sample >= 1:				
				pipeline.append({'$sample': {'size' : int(sample) } })
			elif sample > 0:
				n : int = cast(int, await dbc.estimated_document_count())
				pipeline.append({'$sample': {'size' : int(n * sample) } })
			return pipeline		
		except Exception as err:
			error(f'{err}')
		return None


	async def tank_stats_count(self, release: BSBlitzRelease | None = None,
							regions: set[Region] = Region.API_regions(), 
							sample : float = 0, 
							force : bool = False) -> int:
		assert sample >= 0, f"'sample' must be >= 0, was {sample}"
		try:
			DBC : str = self.C['TANK_STATS']
			dbc : AsyncIOMotorCollection = self.db[DBC]

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
				pipeline = await self._tank_stats_mk_pipeline(release=release, regions=regions, sample=sample)

				if pipeline is None:
					raise ValueError(f'could not create get-accounts {self.name} cursor')
				pipeline.append({ '$count': 'accounts' })
				cursor : AsyncIOMotorCursor = dbc.aggregate(pipeline, allowDiskUse=True)
				res : Any =  (await cursor.to_list(length=100))[0]
				if type(res) is dict and 'accounts' in res:
					return int(res['accounts'])
				else:
					raise ValueError('pipeline returned malformed data')
		except Exception as err:
			error(f'counting accounts failed: {err}')
		return -1
	

	async def tank_stats_insert(self, tank_stats: Iterable[WGtankStat]) -> tuple[int, int, int]:
		"""Store tank stats to the backend. Returns number of stats inserted and not inserted"""
		added			: int = 0
		not_added 		: int = 0
		last_battle_time: int = -1

		try:
			DBC : str = self.C['TANK_STATS']
			dbc : AsyncIOMotorCollection = self.db[DBC]
			res : InsertManyResult
			last_battle_time = max( [ ts.last_battle_time for ts in tank_stats] )	
			res = await dbc.insert_many( (tank_stat.obj_db() for tank_stat in tank_stats), 
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
			error(f'Unknown error when adding tank stats: {err}')
		return added, not_added, last_battle_time


	async def tank_stats_update(self, tank_stats: list[WGtankStat], upsert: bool = False) -> tuple[int, int, int]:
		"""Store tank stats to the backend. Returns number of stats inserted and not inserted"""
		updated			: int = 0
		not_updated 	: int = 0
		last_battle_time: int = -1

		try:
			DBC : str = self.C['TANK_STATS']
			dbc : AsyncIOMotorCollection = self.db[DBC]
			res : UpdateResult
			last_battle_time = max( [ ts.last_battle_time for ts in tank_stats] )	
			res = await dbc.update_many( (ts.obj_db() for ts in tank_stats), 
										  upsert=upsert, ordered=False)
			updated = res.modified_count
			not_updated = len(tank_stats) - updated
		
		except Exception as err:
			error(f'Unknown error when updating tank stats: {err}')
		return updated, not_updated, last_battle_time


	async def tank_stats_get(self, account: BSAccount, tank_id: int | None = None, 
								last_battle_time: int | None = None) -> AsyncGenerator[WGtankStat, None]:
		"""Return tank stats from the backend"""
		raise NotImplementedError


	async def tank_stats_export(self, tank_stats_type: type[WGtankStat] = WGtankStat, 
								# regions: set[Region] = Region.has_stats(), 
								accounts: list[BSAccount] | None = None,
								tanks: list[Tank] | None = None,  
								sample : float = 0) -> AsyncGenerator[list[WGtankStat], None]:
		"""Import accounts from Mongo DB"""
		try:
			DBC : str = self.C['TANK_STATS']
			dbc : AsyncIOMotorCollection = self.db[DBC]

			pipeline : list[dict[str, Any]] = list()
			match : list[dict[str, Any]] = list()
			if accounts is not None:
				match.append({ 'a': { '$in' : [ a.id for a in accounts ]}})
			if tanks is not None:
				match.append({ 't': { '$in' : [ t.tank_id for t in tanks ]}})
			if len(match) > 0:
				pipeline.append({ '$match': { '$and' : match }})
			
			if sample > 0 and sample < 1:
				N : int = await dbc.estimated_document_count()
				pipeline.append({ '$sample' : N * sample })
			elif sample >= 1:
				pipeline.append({ '$sample' : sample })
			
			tank_stats	: list[WGtankStat] = list()
			async for tank_stat_obj in dbc.aggregate(pipeline, allowDiskUse=True):
				try:
					tank_stat_in = tank_stats_type.parse_obj(tank_stat_obj)
					debug(f'Read account={tank_stat_in.account_id} tank_id={tank_stat_in.tank_id} from {self.database}.{self.table_tank_stats}')
					tank_stats.append(WGtankStat.parse_obj(tank_stat_in.obj_db()))
					if len(tank_stats) >= TANK_STATS_BATCH:
						yield tank_stats
						tank_stats = list()
				except Exception as err:
					error(f'{err}')
					continue
			if len(tank_stats) > 0:
				yield tank_stats

		except Exception as err:
			error(f'Error fetching accounts from {self.name}: {err}')

# Register backend

Backend.register(name=MongoBackend.name, backend=MongoBackend)