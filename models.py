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

	_exclude_export_DB_fields = None
	_exclude_export_src_fields = None
	
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

	@classmethod
	def from_csv(cls, row: dict[str, Any]) -> 'BSAccount':
		"""Provide CSV row as a dict for csv.DictWriter"""
		row = super().from_csv(row).dict()
		# field type conversion
		for field in ['inactive', 'disabled']:
			if field in row:
				if row[field] == '':
					del row[field]
				else:
					row[field] = int(row[field])
		
		for field in []:
			if field in row:
				if row[field] == '':
					del row[field]
				else:
					row[field] = bool(row[field])
		
		return BSAccount(**row)

