from aiohttp import ClientSession, ClientTimeout
import logging
from datetime import timedelta, datetime, timezone
from typing import AsyncGenerator 

from pyutils.utils import JSONExportable
from blitzutils.models import Region
from models import BSAccount

YS_BASE_URL : str = 'https://yastati.st'

logger 	= logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug


logging.basicConfig(level=logging.DEBUG)

class YastatistAccount(JSONExportable):
	id: int


async def get_accounts_since(region: Region, 
							days: int, 
							client_id: str, 
							secret: str) -> AsyncGenerator[BSAccount, None]:
	"""farm account_ids from Yastati.st"""
	debug(f'starting region={region}')
	assert region in [ Region.eu, Region.ru], "Yastati.st supports only EU and RU regions"
	assert days > 0, "days has to be positive integer"
	try:
		since : datetime = datetime.now(tz=timezone.utc) - timedelta(days=days)
		headers : dict[str, str] = {'connection': 'keep-alive', 
									'content-type': 'application/json', 
									'transfer-encoding': 'chunked', 
									'CF-Access-Client-Id': client_id,
									'CF-Access-Client-Secret': secret
									}
		url : str = f"/api/{region.value}/accounts/{since.isoformat(timespec='seconds')}/active-since"
		print(url)
		timeout = ClientTimeout(total=10)
		async with ClientSession(base_url=YS_BASE_URL, headers=headers, timeout=timeout) as session:
			print('session')
			async with session.get(url) as resp:
				print('response')
				async for line in resp.content:
					print(f'new line')
					try:
						account = YastatistAccount.parse_raw(line)
						yield BSAccount(id=account.id, region=region)
					except Exception as err:
						error(f'{err}')

	except Exception as err:
		error(f'{err}')




