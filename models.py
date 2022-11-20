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
from blitzutils.models import Region, Account	# type: ignore

TYPE_CHECKING = True
logger 	= logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug


class StatsTypes(str, Enum):
	tank_stats 			= 'updated_tank_stats'
	player_achievements = 'updated_player_achievements'


class BSAccount(Account):	
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


	@validator('updated_tank_stats', 'updated_player_achievements', 'added')
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

			
	# def json_src(self) -> str:
	# 	# exclude_src : TypeExcludeDict = { } 
	# 	return self.json(exclude_unset=True, by_alias=False)


	# JSONexportable()
	@classmethod
	def json_formats(cls) -> list[str]:
		return ['db'] + super().json_formats()


	def json_str(self, format: str = 'src') -> str:
		# exclude_src : TypeExcludeDict = { } 
		if format == 'db':
			return self.json(exclude_defaults=True, by_alias=True)
		else:
			return super().json_str(format=format)


	def json_obj(self, format: str = 'src') -> Any:
		# exclude_src : TypeExcludeDict = { } 
		if format == 'db':
			return self.dict(exclude_defaults=True, by_alias=True)
		else:
			return super().json_obj(format=format)


	# def dict_src(self) -> dict[str, int|bool|Region|None]:
	# 	return self.dict(exclude_unset=False, by_alias=False)
	