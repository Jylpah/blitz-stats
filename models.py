from enum import StrEnum
from sys import maxsize
from time import time
from typing import Optional, ClassVar
from math import ceil
from pydantic import validator, Field, HttpUrl


import logging


from blitzutils.models import Region, Account, WGBlitzRelease
from pyutils.utils import epoch_now, TypeExcludeDict, I, D, Idx, \
						BackendIndexType, BackendIndex, DESCENDING, ASCENDING, TEXT

TYPE_CHECKING = True
logger 	= logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug

MIN_INACTIVITY_DAYS : int = 30 # days

class StatsTypes(StrEnum):
	tank_stats 			= 'updated_tank_stats'
	player_achievements = 'updated_player_achievements'


class BSAccount(Account):	
	updated_tank_stats 			: int | None = Field(default=None, alias='ut')
	updated_player_achievements : int | None = Field(default=None, alias='up')
	
	added 						: int 		= Field(default_factory=epoch_now, alias='a')
	inactive					: bool 		= Field(default=False, alias='i')
	disabled					: bool		= Field(default=False, alias='d')

	_min_inactivity_days: int = MIN_INACTIVITY_DAYS

	_exclude_export_DB_fields = None
	_exclude_export_src_fields = None
	_include_export_DB_fields = None
	_include_export_src_fields= None
	
	class Config:
		allow_population_by_field_name = True
		allow_mutation 			= True
		validate_assignment 	= True


	@classmethod
	def backend_indexes(cls) -> list[list[tuple[str, BackendIndexType]]]:
		"""return backend search indexes"""
		indexes : list[list[tuple[str, BackendIndexType]]] = list()
		indexes = list()
		indexes.append([ 	('disabled', ASCENDING), 
							('inactive', ASCENDING), 									 
							('region', 	ASCENDING), 
							('id', 		ASCENDING)
						])
		# indexes.append([ 	('disabled', ASCENDING), 
		# 					('inactive', ASCENDING), 
		# 					('id', 		ASCENDING), 
		# 					('updated_tank_stats', ASCENDING)
		# 				])
		return indexes
	
	@classmethod	
	def inactivity_limit(cls) -> int:
		return cls._min_inactivity_days

	
	@classmethod
	def set_inactivity_limit(cls, days: int) -> None:
		cls._min_inactivity_days = days
	

	@classmethod
	def get_update_field(cls, stats_type : str | None ) -> str | None:
		UPDATED : str = 'updated_'
		try:
			if stats_type is not None:
				return StatsTypes(stats_type).value
		except Exception:
			error(f'Unknown stats_type: {stats_type}')
		return None


	@validator('updated_tank_stats', 'updated_player_achievements')
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
			return epoch_now()
		elif v >= 0:
			return v
		else:
			ValueError('time field must be >= 0')


	def stats_updated(self, stats: StatsTypes) -> None:
		assert type(stats) is StatsTypes, "'stats' need to be type(StatsTypes)"
		setattr(self, stats.value, epoch_now())

	
	def is_inactive(self, stats_type: StatsTypes | None = None) -> bool:
		stats_updated : int | None
		if stats_type is None:
			stats_updated = epoch_now()
		else:
			stats_updated = getattr(self, stats_type.value)

		if stats_updated is None:
			return False
		elif self.last_battle_time is not None:
			return stats_updated - self.last_battle_time > self._min_inactivity_days*24*3600
		else:
			# cannot tell, but assumption is that yes
			return True


class BSBlitzRelease(WGBlitzRelease):
	cut_off: int 	= Field(default=maxsize)

	# _excluide_export_DB_fields	: ClassVar[Optional[TypeExcludeDict]] = { 'cut_off': False }

	# class Config:		
	# 	allow_mutation 			= True
	# 	validate_assignment 	= True


	@validator('cut_off', pre=True)
	def check_cut_off_now(cls, v):
		if v is None:
			return maxsize
		elif isinstance(v, str) and v == 'now':
			return epoch_now()
		else:
			return int(v)
		

	@validator('cut_off')
	def validate_cut_off(cls, v: int) -> int:
		ROUND_TO : int = 15*60
		if v >= 0:
			return ceil(v / ROUND_TO) * ROUND_TO
		raise ValueError('cut_off has to be >= 0')


	def cut_off_now(self) -> None:		
		self.cut_off = epoch_now()


	def txt_row(self, format: str = '') -> str:
		extra : str = ''
		if format == 'rich':
			extra = f"\t{self.cut_off}"
		return super().txt_row(format) + extra