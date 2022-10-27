from datetime import datetime
from typing import Any, Union
from bson.objectid import ObjectId
from pydantic import BaseModel, root_validator, validator, Field, ValidationError
from enum import Enum, IntEnum
from os.path import basename
import logging

logger = logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug

class Region(str, Enum):
	eu 		= 'eu'
	com 	= 'com'
	asia 	= 'asia'
	ru 		= 'ru'
	china 	= 'china'

class BaseJsonModel(BaseModel):
	def json_src(self, exclude :  set |dict | None = None):
		return self.json(by_alias=False, exclude_none=False, exclude=exclude )

	def json_db(self, exclude :  set |dict | None = None):
		return self.json(by_alias=True, exclude_none=True, exclude=exclude)


class Account(BaseJsonModel):
	id		: int 			= Field(default=..., alias='_id')
	region 	: Region | None = Field(default=None, alias='r')
	last_battle_time			: int | None = Field(default=None, alias='l')
	updated_tank_stats 			: int | None = Field(default=None, alias='uts')
	updated_player_achievements : int | None = Field(default=None, alias='upa')
	added 						: int | None = Field(default=None, alias='a')
	inactive					: bool | None = Field(default=None, alias='i')
	disabled					: bool | None = Field(default=None, alias='d')
	datestr 					: str | None = Field(default=None, alias='dstr')

	class Config:
		allow_population_by_field_name = True
		allow_mutation 			= True
		validate_assignment 	= True
		
		        
	# @validator('datestr')
	# def check_dstr(cls, v):
	# 	return None

	@validator('id')
	def check_id(cls, v):
		if v < 0:
			raise ValueError('account_id must be >= 0')
		return v

	
	@validator('last_battle_time', 'updated_tank_stats', 'updated_player_achievements', 'added')
	def check_epoch_ge_zero(cls, v):
		if v is None:
			return None
		elif v >= 0:
			return v
		else:
			raise ValueError('time field must be >= 0')

	
	@root_validator(skip_on_failure=True)
	def set_region(cls, values):
		i = values.get('id')
		if i >= 31e8:
			values['region'] = Region.china
		elif i >= 20e8:
			values['region'] = Region.asia
		elif i >= 10e8:
			values['region'] = Region.com
		elif i >= 5e8:
			values['region'] = Region.eu
		else:			
			values['region'] = Region.ru
		if values['last_battle_time'] is not None:
			values['datestr'] = datetime.fromtimestamp(values['last_battle_time']).strftime("%Y-%m-%d %H:%M:%S")
		# remove null values
		# values = {k: v for k, v in values.items() if v is not None}		
		return values
	
	def json_db(self):
		return super().json_db(exclude={'datestr'})


class EnumWinnerTeam(IntEnum):
	draw = 0
	one = 1
	two = 2

class EnumBattleResult(IntEnum):
	incomplete = -1
	not_win = 0
	win = 1


class EnumVehicleType(IntEnum):
	light_tank = 0
	medium_tank = 1
	heavy_tank = 2
	tank_destroyer = 3

class WoTBlitzReplayAchievement(BaseJsonModel):
	t: int
	v: int


WoTBlitzReplayDetail = dict[str, Union[str, int, list[WoTBlitzReplayAchievement], None]]


class WoTBlitzReplaySummary(BaseJsonModel):
	winner_team 	: EnumWinnerTeam 	| None 	= Field(default=..., alias='wt')
	battle_result 	: EnumBattleResult 	| None 	= Field(default=..., alias='br')
	room_type		: int | None 	= Field(default=None, alias='rt')
	battle_type		: int | None 	= Field(default=None, alias='bt')
	uploaded_by 	: int 			= Field(default=0, alias='ul')
	title 			: str | None 	= Field(default=..., alias='t')
	player_name		: str			= Field(default=..., alias='pn')
	protagonist		: int 			= Field(default=..., alias='p')
	protagonist_team: int | None	= Field(default=..., alias='pt')
	map_name		: str			= Field(default=..., alias='mn')
	vehicle			: str			= Field(default=..., alias='v')
	vehicle_tier	: int | None	= Field(default=..., alias='vx')
	vehicle_type 	: EnumVehicleType | None = Field(default=..., alias='vt')
	credits_total	: int | None 	= Field(default=None, alias='ct')
	credits_base	: int | None 	= Field(default=None, alias='cb')
	exp_base		: int | None	= Field(default=..., alias='eb')
	exp_total		: int | None	= Field(default=None, repr=False)	
	battle_start_timestamp : int 		= Field(default=..., alias='bts')
	battle_start_time : str | None	= Field(default=None, repr=False)	# duplicate of 'bts'
	battle_duration : float			= Field(default=..., alias='bd')	
	description		: None			= Field(default=None, repr=False)
	arena_unique_id	: int			= Field(default=..., alias='aid')
	allies 			: list[int]		= Field(default=..., alias='a')
	enemies 		: list[int]		= Field(default=..., alias='e')
	mastery_badge	: int | None 	= Field(default=None, alias='mb')
	details 		: list[WoTBlitzReplayDetail] = Field(default=..., alias='d')
	TimestampFormat : str = "%Y-%m-%d %H:%M:%S"

	@validator('vehicle_tier')
	def check_tier(cls, v):
		if v > 10 or v < 0:
			raise ValueError('Tier has to be within [1, 10]')
		else: 
			return v

	@validator('protagonist_team')
	def check_protagonist_team(cls, v):
		if v == 1 or v == 2:
			return v
		else:
			raise ValueError('protagonist_team has to be within 1 or 2')

	@validator('battle_start_time', 'description', 'exp_total')
	def return_none(cls, v):
		return None

	# @root_validator(skip_on_failure=True)
	# def remove_none(cls, values):
	# 	# remove null values
	# 	values = {k: v for k, v in values.items() if v is not None}		
	# 	return values

	def json_src(self):
		self.battle_start_time = datetime.fromtimestamp(self.battle_start_timestamp).strftime(self.TimestampFormat)
		return super().json_src()

	
	def json_db(self):
		self.battle_start_time = None
		return super().json_db()


class WoTBlitzReplayData(BaseJsonModel):
	view_url	: str = Field(default=..., alias='v')
	download_url: str | None = Field(default=None)
	summary		: WoTBlitzReplaySummary  = Field(default=..., alias='s') 
	ViewUrlBase : str = 'https://replays.wotinspector.com/en/view/'
	DLurlBase	: str = 'https://replays.wotinspector.com/en/download/'

	@validator('download_url')
	def check_download_url(cls, v):
		return None

	@validator('view_url')
	def check_view_url(cls, v):
		try:
			return v.split('/')[-1:][0]
		except Exception as err:
			raise ValueError('could not parse view_url')


	def json_src(self):
		export : WoTBlitzReplayData = self.copy(deep=True)
		export.download_url = f"{self.DLurlBase}{self.view_url}"
		export.view_url		= f"{self.ViewUrlBase}{self.view_url}"
		return super(WoTBlitzReplayData, export).json_src()


	def json_db(self):
		# self.download_url 	= None
		# if self.view_url.find('/') != -1:
		# 	self.view_url = self.check_view_url(self.view_url)
		return super().json_db()
	

class WoTBlitzReplay(BaseJsonModel):
	id : ObjectId = Field(default=..., alias='_id')
	status: str
	data: WoTBlitzReplayData 
	error: dict
	class Config:
		arbitrary_types_allowed = True
		json_encoders = { ObjectId: str }


