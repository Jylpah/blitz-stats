from configparser import ConfigParser
from argparse import Namespace, ArgumentParser
from datetime import date, datetime
from os.path import isfile
from typing import Optional, Any, Iterable, AsyncGenerator, TypeVar, cast
import logging

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCursor, AsyncIOMotorCollection # type: ignore
from pymongo.results import InsertManyResult, InsertOneResult, UpdateResult, DeleteResult
from pymongo.errors import BulkWriteError
from pymongo import DESCENDING, ASCENDING
from pydantic import ValidationError, Field

from backend import Backend, OptAccountsDistributed, OptAccountsInactive, BSTableType, \
					MAX_UPDATE_INTERVAL, WG_ACCOUNT_ID_MAX, CACHE_VALID, ErrorLog, ErrorLogType
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

class MongoBackend(Backend):

	name : str = 'mongodb'
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
			# self.C 		: dict[str,str] = dict()

			# default collections
			# self.C['ACCOUNTS'] 			= 'Accounts'
			# self.C['TANKOPEDIA'] 		= 'Tankopedia'
			# self.C['RELEASES'] 			= 'Releases'
			# self.C['REPLAYS'] 			= 'Replays'
			# self.table_tank_stats 		= 'TankStats'
			# self.C['PLAYER_ACHIEVEMENTS'] = 'PlayerAchievements'

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

					self.set_table(BSTableType.Accounts, 	configMongo.get('c_accounts', 	self.table_accounts))
					self.set_table(BSTableType.Tankopedia, 	configMongo.get('c_tankopedia', 	self.table_tankopedia))
					self.set_table(BSTableType.Releases, 	configMongo.get('c_releases', 	self.table_releases))
					self.set_table(BSTableType.Replays, 	configMongo.get('c_replays', 		self.table_replays))
					self.set_table(BSTableType.TankStats, 	configMongo.get('c_tank_stats', 	self.table_tank_stats))
					self.set_table(BSTableType.PlayerAchievements, configMongo.get('c_player_achievements', 
																				self.table_player_achievements))
					self.set_table(BSTableType.AccountLog, 	configMongo.get('c_account_log', 	self.table_account_log))
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
		if backend != cls.name:
			raise ValueError(f'calling {cls}.read_args() for {backend} backend')
		kwargs : dict[str, Any] = dict()
		if args.import_host is not None:
			kwargs['host'] = args.import_host
		if args.import_database is not None:
			kwargs['database'] = args.import_database
		debug(f'args={kwargs}')
		return kwargs	
	

	# def set_database(self, database : str) -> bool:
	# 	"""Set database"""
	# 	try:
	# 		debug('starting')
	# 		self.db = self._client[database]
	# 		self._database = database
	# 		return True
	# 	except Exception as err:
	# 		error(f'Error creating copy: {err}')
	# 	return False


	@property
	def backend(self) -> str:
		return f'{self.driver}://{self._client.HOST}/{self.database}'


	def __eq__(self, __o: object) -> bool:
		return __o is not None and isinstance(__o, MongoBackend) and \
					self._client.address == __o._client.address and \
					self.database == __o.database


	async def init(self) -> bool:
		"""Init MongoDB backend: create collections and set indexes"""
		try:
			debug('starting')

			DBC : str = 'NOT DEFINED'
			dbc : AsyncIOMotorCollection

			try:
				DBC = self.table_accounts
				dbc = self.db[DBC]

				verbose(f'Adding index: {DBC}: [ region, inactive, disabled]')
				await dbc.create_index([ ('r', DESCENDING), ('i', DESCENDING), 
										 ('d', DESCENDING) ], background=True)
			except Exception as err:
				error(f'{self.name}: Could not init collection {DBC} for accounts: {err}')	
			
			try:
				DBC = self.table_releases
				dbc = self.db[DBC]

				verbose(f'Adding index: {DBC}: [ release: -1, launch_date: -1]')
				await dbc.create_index([ ('release', DESCENDING), ('launch_date', DESCENDING)], background=True)
			except Exception as err:
				error(f'{self.name}: Could not init collection {DBC} for releases: {err}')

			try:
				DBC = self.table_replays
				dbc = self.db[DBC]

				verbose(f'Adding index: {DBC}: [ data.summary.protagonist, data.summary.room_type, data.summary.vehicle_tier]')
				await dbc.create_index([ ('d.s.p', DESCENDING), ('d.s.rt', DESCENDING), 
										 ('d.s.vx', DESCENDING) ], background=True)
			except Exception as err:
				error(f'{self.name}: Could not init collection {DBC} for replays: {err}')

			try:
				DBC = self.table_tank_stats
				dbc = self.db[DBC]

				verbose(f'Adding index: {DBC}: [ tank_id, account_id, last_battle_time]')
				await dbc.create_index([ ('a', DESCENDING), ('t', DESCENDING),
										 ('lb', DESCENDING) ], background=True)
			except Exception as err:
				error(f'{self.name}: Could not init collection {DBC} for tank_stats: {err}')

			try:
				DBC = self.table_player_achievements
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
			debug('starting')
			# DBC : str = self.table_accounts
			dbc : AsyncIOMotorCollection = self.db[self.table_accounts]
			return BSAccount.parse_obj(await dbc.find_one({'_id': account_id}))
		except ValidationError as err:
			error(f'Could not validate account_id={account_id} from {self.name}: {self}')
			await self.error_log(MongoErrorLog(table=self.table_accounts, doc_id=account_id, type=ErrorLogType.ValidationError))
		
		except Exception as err:
			error(f'Error fetching account_id: {account_id}) from {self.name}.{self.table_accounts}: {err}')
				
		return None


	async def accounts_count(self, stats_type : StatsTypes | None = None, 
							regions: set[Region] = Region.API_regions(), 
							inactive : OptAccountsInactive = OptAccountsInactive.default(), 	
							disabled: bool = False, 
							dist : OptAccountsDistributed | None = None, sample : float = 0, 
							cache_valid: int | None = None ) -> int:
		assert sample >= 0, f"'sample' must be >= 0, was {sample}"
		try:
			debug('starting')
			# DBC : str = self.table_accounts
			dbc : AsyncIOMotorCollection = self.db[self.table_accounts]
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
																		cache_valid=cache_valid)

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
							cache_valid: int | None = None ) -> AsyncGenerator[BSAccount, None]:
		"""Get accounts from Mongo DB
			inactive: true = only inactive, false = not inactive, none = AUTO
		"""
		try:
			debug('starting')
			NOW = epoch_now()	
			# DBC : str = self.table_accounts
			dbc : AsyncIOMotorCollection = self.db[self.table_accounts]
			pipeline : list[dict[str, Any]] | None = await self._accounts_mk_pipeline(stats_type=stats_type, regions=regions, 
																	inactive=inactive, disabled=disabled, 
																	dist=dist, sample=sample, 
																	cache_valid=cache_valid)

			update_field : str | None = None
			if stats_type is not None:
				update_field = stats_type.value

			if pipeline is None:
				raise ValueError(f'could not create get-accounts {self.name} cursor')
						
			async for account_obj in dbc.aggregate(pipeline, allowDiskUse=True):
				try:
					player = BSAccount.parse_obj(account_obj)
					# if not force and not disabled and inactive is None and player.inactive:
					if not disabled and inactive == OptAccountsInactive.auto and player.inactive:
						assert update_field is not None, "automatic inactivity detection requires stat_type"
						updated = dict(player)[update_field]
						if (NOW - updated) < min(MAX_UPDATE_INTERVAL, (updated - player.last_battle_time)/2):
							continue
					yield player
				except Exception as err:
					error(f'{err}')
					continue
		except Exception as err:
			error(f'Error fetching accounts from {self.name}:{self.database}.{self.table_accounts}: {err}')	


	async def accounts_export(self, account_type: type[Account] = BSAccount, 
								regions: set[Region] = Region.has_stats(), 
								sample : float = 0) -> AsyncGenerator[BSAccount, None]:
		"""Import accounts from Mongo DB"""
		try:
			debug('starting')
			# DBC : str = self.table_accounts
			dbc : AsyncIOMotorCollection = self.db[self.table_accounts]

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
			error(f'Error fetching accounts from {self.name}:{self.database}.{self.table_accounts}: {err}')	


	async def _accounts_mk_pipeline(self, stats_type : StatsTypes | None = None, 
							regions: set[Region] = Region.API_regions(), 
							inactive : OptAccountsInactive = OptAccountsInactive.default(), 
							dist : OptAccountsDistributed | None = None,
							disabled: bool|None = False, sample : float = 0, 
							cache_valid: int | None = None) -> list[dict[str, Any]] | None:
		try:
			debug('starting')
			
			NOW = epoch_now()
			# DBC : str = self.table_accounts
			if cache_valid is None:
				cache_valid = self._cache_valid
			update_field : str | None = None
			if stats_type is not None:
				update_field = stats_type.value
			dbc : AsyncIOMotorCollection = self.db[self.table_accounts]
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
			debug('starting')
			# DBC : str = self.table_accounts
			dbc : AsyncIOMotorCollection = self.db[self.table_accounts]
			account.added = epoch_now()
			res : InsertOneResult = await dbc.insert_one(account.obj_db())
			debug(f'Account add to {self.name}: {account.id}')
			return True			
		except Exception as err:
			debug(f'Failed to add account_id={account.id} to {self.name}:{self.database}.{self.table_accounts}: {err}')	
		return False
	
	
	async def account_update(self, account: BSAccount, upsert: bool = True) -> bool:
		"""Update an account in the backend. Returns False 
			if the account was not updated"""
		try:
			debug('starting')
			# DBC : str = self.table_accounts
			dbc : AsyncIOMotorCollection = self.db[self.table_accounts]
			
			await dbc.find_one_and_replace({ '_id': account.id }, account.obj_db(), upsert=upsert)
			debug(f'Updated: {account}')
			return True			
		except Exception as err:
			debug(f'Failed to update account_id={account.id} to {self.name}:{self.database}.{self.table_accounts}: {err}')	
		return False


	async def accounts_insert(self, accounts: Iterable[BSAccount]) -> tuple[int, int]:
		"""Store account to the backend. Returns False 
			if the account was not added"""
		debug('starting')
		added		: int = 0
		not_added 	: int = 0
		try:
			# DBC : str = self.table_accounts
			dbc : AsyncIOMotorCollection = self.db[self.table_accounts]
			res : InsertManyResult
			
			# for account in accounts:
			# 	# modifying Iterable items is OK since the item object ref stays the sam
			# 	account.added = epoch_now()   

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
			error(f'Unknown error when adding acconts to {self.name}:{self.database}.{self.table_accounts}: {err}')
		return added, not_added

########################################################
# 
# MongoBackend(): releases
#
########################################################


	async def release_get(self, release : str) -> BSBlitzRelease | None:
		res : BSBlitzRelease | None = None
		try:
			debug('starting')
			# DBC : str = self.table_releases
			dbc : AsyncIOMotorCollection = self.db[self.table_releases]			
			res = BSBlitzRelease.parse_obj(await dbc.find_one({ 'release' : release }))
			if res is None:
				raise ValueError()
		except ValidationError as err:
			error(f'Incorrect data format: {err}')
		except Exception as err:
			error(f'Could not find release {release} from {self.name}:{self.database}.{self.table_releases}: {err}')
		return res


	async def release_get_latest(self) -> BSBlitzRelease | None:
		"""Get the latest release in the backend"""
		rel : BSBlitzRelease | None = None
		try:
			debug('starting')
			# DBC : str = self.table_releases
			dbc : AsyncIOMotorCollection = self.db[self.table_releases]
			async for r in dbc.find().sort('launch_date', DESCENDING):
				rel = BSBlitzRelease.parse_obj(r)
				if rel is not None:
					return rel			
		except ValidationError as err:
			error(f'Incorrect data format: {err}')
		except Exception as err:
			error(f'Could not find the latest release from {self.name}:{self.database}.{self.table_releases}: {err}')
		return None


	async def release_get_current(self) -> BSBlitzRelease | None:
		"""Get the latest release in the backend"""
		rel : BSBlitzRelease | None = None
		try:
			debug('starting')
			dbc : AsyncIOMotorCollection = self.db[self.table_releases]
			async for r in dbc.find({ 'launch_date': { '$lte': date.today() } }).sort('launch_date', ASCENDING):
				return BSBlitzRelease.parse_obj(r)
		except ValidationError as err:
			error(f'Incorrect data format: {err}')
		except Exception as err:
			error(f'Could not find the latest release: {err}')
		return None


	async def releases_get(self, release: str | None = None, since : date | None = None, 
							first : BSBlitzRelease | None = None) -> list[BSBlitzRelease]:
		assert since is None or first is None, 'Only one can be defined: since, first'
		debug('starting')
		releases : list[BSBlitzRelease] = list()
		try:
			async for rel in self.releases_export(release=release, since=since, first=first):				
				releases.append(rel)				
		except Exception as err:
			error(f'Error getting releases: {err}')
		return releases


	async def releases_export(self, release_type: type[WGBlitzRelease] = BSBlitzRelease,
								release: str | None = None, since : date | None = None, 
								first : BSBlitzRelease | None = None) -> AsyncGenerator[BSBlitzRelease, None]:
		"""Import relaseses from Mongo DB"""
		try:
			debug('starting')
			debug(f'release={release}, since={since}, first={first}')

			dbc : AsyncIOMotorCollection = self.db[self.table_releases]
			query : dict[str, Any] = dict()

			if since is not None:
				query['launch_date'] = { '$gte': since }
			elif first is not None:
				query['launch_date'] = { '$gte': first.launch_date}
			
			if release is not None:
				query['release'] = { '$regex' : '^' + release }
			
			rel	: BSBlitzRelease
			async for release_obj in dbc.find(query).sort('launch_date', ASCENDING):
				try:
					release_in = release_type.parse_obj(release_obj)
					debug(f'Read {release_in} from {self.database}.{self.table_releases}')
					rel = BSBlitzRelease.parse_obj(release_in.obj_db())
					yield rel
				except Exception as err:
					error(f'{err}')
					continue			
		except Exception as err:
			error(f'Error exporting releases from {self.name}: {err}')	


	async def release_insert(self, release: BSBlitzRelease) -> bool:
		"""Insert new release to the backend"""
		try:
			debug(f'starting. release={release.obj_db()}')
			
			dbc : AsyncIOMotorCollection = self.db[self.table_releases]
			res : InsertOneResult= await dbc.insert_one(release.obj_db())
			
			if res.inserted_id is None:
				error(f'Could not add release {release}')
				return False
			return True
		except Exception as err:
			error(f'Could not add release {release}: {err}')
		return False


	async def release_delete(self, release: BSBlitzRelease) -> bool:
		"""Delete a release from backend"""
		try:
			debug('starting')
			dbc : AsyncIOMotorCollection = self.db[self.table_releases]
			res : DeleteResult = await dbc.delete_one( {'release': release.release })
			return res.deleted_count == 1
		except Exception as err:
			error(f'Could not add release {release}: {err}')
		return False


	async def release_update(self, update: BSBlitzRelease, upsert=False) -> bool:
		"""Update an release in the backend. Returns False 
			if the release was not updated"""
		try:
			debug('starting')
			
			dbc : AsyncIOMotorCollection = self.db[self.table_releases]
			release : BSBlitzRelease | None
			
			if (release := await self.release_get(release=update.release)) is None:
				raise ValueError(f'Could not find release {update.release} from {self.name}')
			release_dict : dict[str, Any] 	= release.dict(by_alias=False)
			debug(f'release={release_dict}')
			update_dict : dict[str, Any] 	= update.dict(by_alias=False, exclude_unset=True)
			debug(f'update={update_dict}')
			
			for key in update_dict.keys():
				if key == 'release':
					continue
				release_dict[key] = update_dict[key]
			
			await dbc.find_one_and_replace({ 'release': release.release }, BSBlitzRelease(**release_dict).obj_db(), upsert=upsert)
			debug(f'Updated: {release}')
			return True			
		except Exception as err:
			debug(f'Failed to update release={update.release} to {self.name}: {err}')	
		return False

########################################################
# 
# MongoBackend(): replay
#
########################################################

	async def replay_insert(self, replay: WoTBlitzReplayJSON) -> bool:
		"""Store replay into backend"""
		try:
			debug('starting')
			dbc : AsyncIOMotorCollection = self.db[self.table_replays]
			await dbc.insert_one(replay.obj_db())
			return True
		except Exception as err:
			debug(f'Could not insert replay (_id: {replay.id}) into {self.name}:{self.database}.{self.table_replays}: {err}')	
		return False


	async def replay_get(self, replay_id: str | ObjectId) -> WoTBlitzReplayJSON | None:
		"""Get a replay from backend based on replayID"""
		try:
			debug(f'Getting replay (id={replay_id} from {self.name})')

			dbc : AsyncIOMotorCollection = self.db[self.table_replays]
			res : Any | None = await dbc.find_one({'_id': str(replay_id)})
			if res is not None:
				# replay : WoTBlitzReplayJSON  = WoTBlitzReplayJSON.parse_obj(res) 
				# debug(replay.json_src())
				return WoTBlitzReplayJSON.parse_obj(res)   # returns None if not found
		except Exception as err:
			debug(f'Error reading replay (id_: {replay_id}) from {self.name}:{self.database}.{self.table_replays}: {err}')	
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
			
			dbc : AsyncIOMotorCollection = self.db[self.table_tank_stats]
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
							sample : float = 0) -> int:
		assert sample >= 0, f"'sample' must be >= 0, was {sample}"
		try:
			debug('starting')
			dbc : AsyncIOMotorCollection = self.db[self.table_tank_stats]

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


	async def tank_stats_insert(self, tank_stats: Iterable[WGtankStat]) -> tuple[int, int]:
		"""Store tank stats to the backend. Returns number of stats inserted and not inserted"""
		debug('starting')
		added			: int = 0
		not_added 		: int = 0
		# last_battle_time: int = -1

		try:
			dbc : AsyncIOMotorCollection = self.db[self.table_tank_stats]
			res : InsertManyResult
			# last_battle_time = max( [ ts.last_battle_time for ts in tank_stats] )	
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
		return added, not_added  # , last_battle_time


	async def tank_stats_update(self, tank_stats: list[WGtankStat], upsert: bool = False) -> tuple[int, int]:
		"""Store tank stats to the backend. Returns number of stats inserted and not inserted"""
		debug('starting')
		updated			: int = 0
		not_updated 	: int = 0
		# last_battle_time: int = -1

		try:
			dbc : AsyncIOMotorCollection = self.db[self.table_tank_stats]
			res : UpdateResult
			# last_battle_time = max( [ ts.last_battle_time for ts in tank_stats] )	
			res = await dbc.update_many( (ts.obj_db() for ts in tank_stats), 
										  upsert=upsert, ordered=False)
			updated = res.modified_count
			not_updated = len(tank_stats) - updated
		
		except Exception as err:
			error(f'Unknown error when updating tank stats: {err}')
		return updated, not_updated


	async def tank_stats_get(self, account: BSAccount, tank_id: int | None = None, 
								last_battle_time: int | None = None) -> AsyncGenerator[WGtankStat, None]:
		"""Return tank stats from the backend"""
		debug('starting')
		raise NotImplementedError


	async def tank_stats_export(self, tank_stats_type: type[WGtankStat] = WGtankStat, 
								regions: set[Region] = Region.has_stats(), 
								accounts: list[BSAccount] | None = None,
								tanks: list[Tank] | None = None,  
								sample : float = 0) -> AsyncGenerator[list[WGtankStat], None]:
		"""Import accounts from Mongo DB"""
		try:
			debug('starting')

			dbc : AsyncIOMotorCollection = self.db[self.table_tank_stats]
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
			dbc : AsyncIOMotorCollection = self.db[self.table_error_log]
			await dbc.insert_one(error.obj_db())
			return True
		except Exception as err:
			debug(f'Could not log error: {error.table}: "{error.msg}" into {self.name}:{self.database}.{self.table_error_log}: {err}')	
		return False


	async def errors_get(self, table_type: BSTableType | None = None, doc_id : Any | None = None, 
							after: datetime | None = None) -> AsyncGenerator[ErrorLog, None]:
		"""Return errors from backend ErrorLog"""
		try:
			debug('starting')
			
			dbc : AsyncIOMotorCollection = self.db[self.table_error_log]
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
					debug(f'Read "{err.msg}" from {self.name} {self.database}.{self.table_error_log}')
					
					yield err
				except Exception as e:
					error(f'{e}')
					continue			
		except Exception as e:
			error(f'Error getting errors from {self.name} {self.database}.{self.table_error_log}: {e}')	


	async def errors_clear(self, table_type: BSTableType, doc_id : Any | None = None, 
							after: datetime | None = None) -> int:
		"""Clear errors from backend ErrorLog"""
		try:
			debug('starting')
			
			dbc : AsyncIOMotorCollection = self.db[self.table_error_log]
			query : dict[str, Any] = dict()

			query['t'] = self.get_table(table_type)
			if after is not None:
				query['d'] = { '$gte': after }
			if doc_id is not None:
				query['did'] = doc_id
			
			res : DeleteResult = await dbc.delete_many(query)
			return res.deleted_count	
		except Exception as e:
			error(f'Error clearing errors from {self.name} {self.database}.{self.table_error_log}: {e}')
		return 0

# Register backend

debug('Registering mongodb')
Backend.register(name=MongoBackend.name, backend=MongoBackend)