from datetime import datetime
from time import time
from typing import Any, Mapping
from bson.objectid import ObjectId
from pydantic import BaseModel, Extra, root_validator, validator, Field, HttpUrl, ValidationError
import json
from enum import Enum, IntEnum
from os.path import basename
import logging

TYPE_CHECKING = True
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

TypeExcludeDict = Mapping[int | str, Any]

class BaseJsonModel(BaseModel):
	_exclude_alias : TypeExcludeDict = { str(True): {}, str(False): {} }

	def json_src(self, exclude :  set |dict | None = None):
		return self.json(by_alias=False, exclude_none=False, exclude=exclude )

	def json_db(self, exclude :  set |dict | None = None):
		return self.json(by_alias=True, exclude_none=True, exclude=exclude)

	def json(self, by_alias: bool = False, **kwargs):
		return super().json(by_alias=by_alias, exclude_unset=True, 
							exclude=self._exclude_alias[str(by_alias)], **kwargs)
		

class Account(BaseJsonModel):
	# _exclude_alias : TypeExcludeDict = { 	str(True):  { 'region' }, 
	# 										str(False): {'last_battle_time'} 
	# 										}	
	id		: int 				= Field(default=..., alias='_id')
	region 	: Region | None 	= Field(default=None, alias='r')
	last_battle_time			: int | None = Field(default=None, alias='l')
	updated_tank_stats 			: int | None = Field(default=None, alias='uts')
	updated_player_achievements : int | None = Field(default=None, alias='upa')
	added 						: int | None = Field(default=None, alias='a')
	inactive					: bool | None = Field(default=None, alias='i')
	disabled					: bool | None = Field(default=None, alias='d')

	class Config:
		allow_population_by_field_name = True
		allow_mutation 			= True
		validate_assignment 	= True


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

	@validator('added')
	def set_current_time(cls, v):
		if v is None:
			return int(time())
		else:
			return v

	TypeAccountDict = dict[str, int |bool |Region |None]

	@root_validator(skip_on_failure=True)
	def set_region(cls, values: TypeAccountDict) -> TypeAccountDict:
		i = values.get('id')
		
		assert type(i) is int, 'i has to be int'

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
		return values
	
# REMOVE
class TestAccount(BaseJsonModel):
	_exclude_alias : TypeExcludeDict = { 	str(True):  { 'a' }, 
													str(False): {'b'} 
												}
	a : Account = Field(default=..., alias='Aaa')
	b : Account = Field(default=..., alias='Bee')


class EnumWinnerTeam(IntEnum):
	draw = 0
	one = 1
	two = 2

class EnumBattleResult(IntEnum):
	incomplete = -1
	not_win = 0
	win = 1


class EnumVehicleType(IntEnum):
	light_tank 	= 0
	medium_tank = 1
	heavy_tank 	= 2
	tank_destroyer = 3

class WoTBlitzReplayAchievement(BaseJsonModel):
	t: int
	v: int


# WoTBlitzReplayDetail = dict[str, Union[str, int, list[WoTBlitzReplayAchievement], None]]
class WoTBlitzReplayDetail(BaseJsonModel):
	achievements : WoTBlitzReplayAchievement | None = Field(default=None, alias='a')
	base_capture_points	: int | None = Field(default=None, alias='bc')
	base_defend_points	: int | None = Field(default=None, alias='bd')
	chassis_id			: int | None = Field(default=None, alias='ch')
	clan_tag			: str | None = Field(default=None, alias='ct')
	clanid				: int | None = Field(default=None, alias='ci')
	credits				: int | None = Field(default=None, alias='cr')
	damage_assisted		: int | None = Field(default=None, alias='da')
	damage_assisted_track: int | None = Field(default=None, alias='dat')
	damage_blocked		: int | None = Field(default=None, alias='db')
	damage_made			: int | None = Field(default=None, alias='dm')
	damage_received		: int | None = Field(default=None, alias='dr')
	dbid				: int | None = Field(default=None, alias='ai')
	death_reason		: int | None = Field(default=None, alias='de')
	distance_travelled	: int | None = Field(default=None, alias='dt')
	enemies_damaged		: int | None = Field(default=None, alias='ed')
	enemies_destroyed	: int | None = Field(default=None, alias='ek')
	enemies_spotted		: int | None = Field(default=None, alias='es')
	exp					: int | None = Field(default=None, alias='ex')
	exp_for_assist		: int | None = Field(default=None, alias='exa')
	exp_for_damage		: int | None = Field(default=None, alias='exd')
	exp_team_bonus		: int | None = Field(default=None, alias='et')
	gun_id				: int | None = Field(default=None, alias='gi')
	hero_bonus_credits	: int | None = Field(default=None, alias='hc')
	hero_bonus_exp		: int | None = Field(default=None, alias='he')
	hitpoints_left		: int | None = Field(default=None, alias='hl')
	hits_bounced		: int | None = Field(default=None, alias='hb')
	hits_pen			: int | None = Field(default=None, alias='hp')
	hits_received		: int | None = Field(default=None, alias='hr')
	hits_splash			: int | None = Field(default=None, alias='hs')
	killed_by			: int | None = Field(default=None, alias='ki')
	shots_hit			: int | None = Field(default=None, alias='sh')
	shots_made			: int | None = Field(default=None, alias='sm')
	shots_pen			: int | None = Field(default=None, alias='sp')
	shots_splash		: int | None = Field(default=None, alias='ss')
	squad_index			: int | None = Field(default=None, alias='sq')
	time_alive			: int | None = Field(default=None, alias='t')
	turret_id			: int | None = Field(default=None, alias='ti')
	vehicle_descr		: int | None = Field(default=None, alias='vi')
	wp_points_earned	: int | None = Field(default=None, alias='we')
	wp_points_stolen	: int | None = Field(default=None, alias='ws')

	class Config:
		extra 				= Extra.allow
		allow_mutation 		= True
		validate_assignment = True


class WoTBlitzReplaySummary(BaseJsonModel):
	_exclude_alias : TypeExcludeDict = { 	str(True):  { 'description', 'battle_start_time' }, 
											str(False): {'description'} 
										}
	TimestampFormat : str = "%Y-%m-%d %H:%M:%S"

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


	class Config:
		extra 				= Extra.allow
		allow_mutation 		= True
		validate_assignment = True

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

	@root_validator(skip_on_failure=True)
	def root(cls, values):
		# remove null values
		del values['description']		
		values['battle_start_time'] = datetime.fromtimestamp(values['battle_start_timestamp']).strftime(cls.TimestampFormat)
		return values


class WoTBlitzReplayData(BaseJsonModel):
	_exclude_alias : TypeExcludeDict = { 	str(True):  { 'view_url', 'download_url'}, 
											str(False): {'id'} 
										}
	ViewUrlBase : str = 'https://replays.wotinspector.com/en/view/'
	DLurlBase	: str = 'https://replays.wotinspector.com/en/download/'

	# fields
	view_url	: HttpUrl 		= Field(default=..., alias='v')
	download_url: HttpUrl 		= Field(default=..., alias='d')
	id 			: str | None	= Field(default=None, repr=False)
	summary		: WoTBlitzReplaySummary  = Field(default=..., alias='s') 
	
	class Config:
		allow_mutation 			= True
		validate_assignment 	= True


	@root_validator
	def store_id(cls, values):
		try:
			if values['view_url'] is not None:
				values['id'] = values['view_url'].split('/')[-1:][0]
			elif values['download_url'] is not None:
				values['id'] = values['download_url'].split('/')[-1:][0]
			else:
				raise ValueError('Replay is missing ID')
			values['view_url'] 		= f"{cls.ViewUrlBase}{values['id']}"
			values['download_url'] 	= f"{cls.DLurlBase}{values['id']}"
			return values
		except Exception as err:
			raise ValueError('could not replay url')
	
	
class WoTBlitzReplay(BaseJsonModel):
	id : ObjectId = Field(default=..., alias='_id')
	status: str
	data: WoTBlitzReplayData 
	error: dict
	class Config:
		arbitrary_types_allowed = True
		json_encoders = { ObjectId: str }
		allow_mutation 			= True
		validate_assignment 	= True


