from argparse import ArgumentParser
from configparser import ConfigParser
from typing import Optional
import logging

error 	= logging.error
message	= logging.warning
verbose	= logging.info
debug	= logging.debug

def accounts_add_args(parser: ArgumentParser, config: Optional[ConfigParser]):
	try:
		if config is not None:
			if 'WI' in config.sections():
					configWI 		= config['WI']
					WI_RATE_LIMIT	= configWI.getfloat('rate_limit', 20/3600)
					WI_MAX_PAGES	= configWI.getint('max_pages', 10)
					WI_AUTH_TOKEN	= configWI.get('auth_token', None)
		
	except Exception as err:
		error(str(err))
	
