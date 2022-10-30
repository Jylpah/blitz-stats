from configparser import ConfigParser
from argparse import Namespace
import logging
from abc import ABCMeta, abstractmethod

from bson import ObjectId
from models import Account, Region, WoTBlitzReplayJSON

logger = logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug


class Backend(metaclass=ABCMeta):
	"""Abstract class for a backend (mongo, files, postgres)"""
	# def __init__(self, parser: Namespace, config: ConfigParser | None = None):
	# 	try:
	# 		if config is not None and 'BACKEND' in config.sections():

	# 	except Exception as err:
	# 		error(str(err))

	@abstractmethod
	async def replay_store(self, replay: WoTBlitzReplayJSON) -> bool:
		"""Store replay into backend"""
		raise NotImplementedError


	@abstractmethod
	async def replay_get(self, replayId: str | ObjectId) -> WoTBlitzReplayJSON | None:
		"""Get a replay from backend based on replayID"""
		raise NotImplementedError


	# replay fields that can be searched: protagonist, battle_start_timestamp, account_id, vehicle_tier
	@abstractmethod
	async def replay_find(self, **kwargs) -> WoTBlitzReplayJSON | None:
		"""Find a replay from backend based on search string"""
		raise NotImplementedError

	@abstractmethod
	async def account_get(self, account_id: int) -> Account | None:
		"""Get account from backend"""
		raise NotImplementedError

	@abstractmethod
	async def accounts_get(self, region: Region) -> Account | None:
		"""Get account from backend"""
		raise NotImplementedError