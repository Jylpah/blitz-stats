from pydantic import Field, root_validator, validator
from datetime import datetime, date
from sys import maxsize
import logging

from blitzutils.models import Account
from models import BSBlitzRelease

logger 	= logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug

class WG_Account(Account):	
	updated_WGtankstats 		 : int | None = Field(default=None, alias='ut')
	updated_WGplayerachievements : int | None = Field(default=None, alias='up')
	inactive					: bool 		 = Field(default=False, alias='i')
	invalid						: bool		 = Field(default=False, alias='d')

	_exclude_export_DB_fields = None
	_exclude_export_src_fields = None
	_include_export_DB_fields = None
	_include_export_src_fields= None
	
	class Config:
		allow_population_by_field_name = True
		allow_mutation 			= True
		validate_assignment 	= True

	@validator('updated_WGtankstats', 'updated_WGplayerachievements')
	def check_epoch_ge_zero(cls, v):
		if v is None:
			return None
		elif v >= 0:
			return v
		else:
			raise ValueError('time field must be >= 0')


class WG_Release(BSBlitzRelease):
	release		: str 				= Field(default=..., alias='Release')
	launch_date	: datetime | None	= Field(default=None, alias='Date')
	cut_off		: int 				= Field(default=maxsize, alias='Cut-off')
	
	_export_DB_by_alias = False

	class Config:
		allow_population_by_field_name = True
		allow_mutation 			= True
		validate_assignment 	= True
		
	# @validator('launch_date', pre=True)
	# def to_date(cls, v) -> date | None:
	# 	print(f'DEBUG: {v}')
	# 	if v is None:
	# 		return None
	# 	return datetime.fromisoformat(v).date()

