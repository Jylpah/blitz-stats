import json
from pydantic import BaseModel, Json, root_validator, validator, Field
from pydantic.json import pydantic_encoder

from enum import Enum

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
		values = {k: v for k, v in values.items() if v is not None}
		
		return values

# class WoTBlitzReplay(BaseModel):
# 	pass