from pydantic import Field, root_validator, validator

from blitzutils.models import Account


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

