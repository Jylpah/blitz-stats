from pydantic import Field, field_validator
import logging

from blitzmodels import Account

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
	
	class Config:
		allow_population_by_field_name = True
		allow_mutation 			= True
		validate_assignment 	= True

	@field_validator('updated_WGtankstats', 'updated_WGplayerachievements')
	def check_epoch_ge_zero(cls, v):
		if v is None:
			return None
		elif v >= 0:
			return v
		else:
			raise ValueError('time field must be >= 0')


# class WG_Release(BSBlitzRelease):
# 	"""For import purposes only"""
# 	id 			: ObjectId | str    = Field(default=..., alias='_id')
# 	release		: str 				= Field(default=..., alias='Release')
# 	launch_date	: datetime | None	= Field(default=None, alias='Date')
# 	cut_off		: int 				= Field(default=maxsize, alias='Cut-off')
	
# 	_export_DB_by_alias = False

# 	class Config:
# 		arbitrary_types_allowed = True
# 		json_encoders 			= { ObjectId: str }
# 		allow_mutation 			= True
# 		validate_assignment 	= True
# 		allow_population_by_field_name = True
		

# 	@model_validator
# 	def transform_id(cls, values: Dict[str, Any]):
# 		if 'release' in values:
# 			del values['id']
# 		return values
