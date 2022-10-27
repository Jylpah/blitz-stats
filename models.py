from datetime import datetime
import json
from typing import Union
from pydantic import BaseModel, Json, root_validator, validator, Field, ValidationError
from pydantic.json import pydantic_encoder

from enum import Enum, IntEnum

class Region(str, Enum):
	eu 		= 'eu'
	com 	= 'com'
	asia 	= 'asia'
	ru 		= 'ru'
	china 	= 'china'

class Account(BaseModel):
	id		: int = Field(default=..., alias='_id')
	region 	: Region | None = None
	last_battle_time			: int | None = None
	updated_tank_stats 			: int | None = None
	updated_player_achievements : int | None = None
	added 						: int | None = None
	inactive: bool | None = None
	disabled: bool | None = None

	class Config:
		allow_population_by_field_name = True
		allow_mutation = True
		validate_assignment = True
		        

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
		# remove null values
		values = {k: v for k, v in values.items() if v is not None}
		
		return values

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

class WoTBlitzReplayAchievement(BaseModel):
	t: int
	v: int


WoTBlitzReplayDetail = dict[str, Union[str, int, list[WoTBlitzReplayAchievement], None]]


class WoTBlitzReplaySummary(BaseModel):
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
	battle_start_time : datetime | None	= Field(default=None, repr=False)	# duplicate of 'bts'
	battle_duration : float			= Field(default=..., alias='bd')	
	description		: None			= Field(default=None, repr=False)
	arena_unique_id	: int			= Field(default=..., alias='aid')
	allies 			: list[int]		= Field(default=..., alias='a')
	enemies 		: list[int]		= Field(default=..., alias='e')
	mastery_badge	: int | None 	= Field(default=None, alias='mb')
	details 		: list[WoTBlitzReplayDetail] = Field(default=..., alias='d')

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

	@validator('battle_start_timestamp', 'description', 'exp_total')
	def return_none(cls, v):
		return None

	@root_validator(skip_on_failure=True)
	def remove_none(cls, values):
		# remove null values
		values = {k: v for k, v in values.items() if v is not None}		
		return values
	


