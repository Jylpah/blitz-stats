from pydantic import root_validator, validator
from pydantic.dataclasses import dataclass
from enum import Enum

class Region(str, Enum):
	eu 		= 'eu'
	com 	= 'com'
	asia 	= 'asia'
	ru 		= 'ru'
	china 	= 'china'


@dataclass
class Account:
	id		: int
	region 	: Region | None = None
	last_battle_time			: int | None = None
	updated_tank_stats 			: int | None = None
	updated_player_achievements : int | None = None
	added 						: int | None = None
	inactive: bool = False
	disabled: bool = False


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
		_id = values.get('id')
		if _id >= 31e8:
			values['region'] = Region.china
		elif _id >= 20e8:
			values['region'] = Region.asia
		elif _id >= 10e8:
			values['region'] = Region.com
		elif _id >= 5e8:
			values['region'] = Region.eu
		else:			
			values['region'] = Region.ru
		return values


	



