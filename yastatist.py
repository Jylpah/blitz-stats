from aiohttp import ClientSession, ClientTimeout
import logging
from datetime import timedelta, datetime, timezone
from typing import AsyncGenerator 
from http import HTTPStatus

from pyutils import JSONExportable
from blitzutils.models import Region
from models import BSAccount

YS_BASE_URL : str = 'https://yastati.st'

logger 	= logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug

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
		headers : dict[str, str] = {'CF-Access-Client-Id': client_id,
									'CF-Access-Client-Secret': secret
									}
		url : str = f"/api/{region.value}/accounts/{since.isoformat(timespec='seconds')}/active-since"
		timeout = ClientTimeout(sock_read=20)
		async with ClientSession(base_url=YS_BASE_URL, headers=headers, timeout=timeout) as session:
			async with session.get(url) as resp:
				status : HTTPStatus = HTTPStatus(resp.status)
				if resp.ok:
					debug(f'HTTP GET: {status.value} / {status.phrase}')
					async for line in resp.content:
						try:
							account = YastatistAccount.parse_raw(line)
							yield BSAccount(id=account.id, region=region)
						except Exception as err:
							error(f'Could not read line: {err}')
				else:
					error(f'GET {url}: {status.value} / {status.phrase}')

	except Exception as err:
		error(f'{err}')




