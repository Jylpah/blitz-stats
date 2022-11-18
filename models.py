from datetime import datetime
from time import time
from typing import Any, Mapping, Optional, Tuple
from bson.objectid import ObjectId
from bson.int64 import Int64
from isort import place_module
from pydantic import BaseModel, Extra, root_validator, validator, Field, HttpUrl
from pydantic.utils import ValueItems
import json
from enum import Enum, IntEnum
from os.path import basename
import logging
import aiofiles
from collections import defaultdict
from blitzutils.models import Region	# type: ignore

TYPE_CHECKING = True
logger 	= logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug


class StatsTypes(str, Enum):
		tank_stats 			= 'updated_tank_stats'
		player_achievements = 'updated_player_achievements'


class Account(BaseModel):	
	id							: int 		 = Field(default=..., alias='_id')
	region 						: Region | None= Field(default=None, alias='r')
	last_battle_time			: int | None = Field(default=None, alias='l')
	updated_tank_stats 			: int | None = Field(default=None, alias='ut')
	updated_player_achievements : int | None = Field(default=None, alias='up')
	added 						: int | None = Field(default=None, alias='a')
	inactive					: bool 		 = Field(default=False, alias='i')
	disabled					: bool		 = Field(default=False, alias='d')

	
	class Config:
		allow_population_by_field_name = True
		allow_mutation 			= True
		validate_assignment 	= True

	
	@classmethod
	def get_update_field(cls, stats_type : str | None ) -> str | None:
		UPDATED : str = 'updated_'
		try:
			if stats_type is not None:
				return StatsTypes(stats_type).value
		except Exception as err:
			error(f'Unknown stats_type: {stats_type}')
		return None


	@validator('id')
	def check_id(cls, v):
		assert v is not None, "id cannot be None"
		assert type(v) is int or type(v) is Int64, "id has to be int"
		if type(v) is Int64:
			v = int(v)
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
		
		assert type(i) is int, f'_id has to be int, was: {i} : {type(i)}'

		if values['region'] is None:
			# set default regions, but do not change region if set
			values['region'] = Region.from_id(i)
			
		return values
	
	def json_src(self) -> str:
		# exclude_src : TypeExcludeDict = { } 
		return self.json(exclude_unset=True, by_alias=False)


	def export_db(self) -> dict:
		# exclude_src : TypeExcludeDict = { } 
		return self.dict(exclude_defaults=True, by_alias=True)
	