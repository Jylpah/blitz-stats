#!/usr/bin/env python3
import sys
from asyncio import run, sleep
from typing import Any
from models import BSBlitzRelease
from releases import release_mapper
from multiprocessing import Manager
from multiprocessing.pool import Pool, AsyncResult 
from queue import Queue
from backend import Backend, BSTableType
from configparser import ConfigParser
from mongobackend import MongoBackend
import logging
from blitzutils.models import WGtankStat
from random import random
from pyutils import AsyncQueue, BucketMapper
from alive_progress import alive_bar  # type: ignore

logging.getLogger().setLevel(logging.DEBUG)

db 		: Backend
dbconfig: dict[str, Any]
Q  		: AsyncQueue
rm 		: BucketMapper

async def get_async(id: int) -> Any:
	global db, Q
	i = 0
	if await db.test():
		print(f'#{id}: connection test succeeded')
	else:
		print(f'#{id}: connection test FAILED')
	rm 		: BucketMapper[BSBlitzRelease] = await release_mapper(db)
	latest 	: BSBlitzRelease = rm.list()[-2]
	print(f'#{id} release={latest.release}')
	async for ts in db.tank_stats_get():		
		if (n := await Q.get()) is None:
			break
		print(f"#{id}: n={n} tank_id={ts.tank_id}")
		i += 1
		await sleep(random())
	return i


def init(dbconfig: dict[str, Any], Qin: Queue) -> None:
	global db, Q
	Q = AsyncQueue.from_queue(Qin)
	# rm = bm
	if (tmp := Backend.create(**dbconfig)):
		db = tmp
	else: 		
		assert False, 'ERROR: could not init backend'
	
def get(id:int) -> Any:
	return run(get_async(id), debug=True)


async def main(N: int):
	I : int = 20
	bm : BucketMapper
	with Manager() as manager:
		Q : Queue = manager.Queue(5)
		config : ConfigParser = ConfigParser()
		config.read('blitzstats.ini')
		if (db := Backend.create('mongodb', config=config)):
			dbconfig = db.config
		else: 
			return -1
		# bm = await release_mapper(db)
		with Pool(processes=N, initializer=init, initargs=[ dbconfig, Q ]) as pool:
			results : AsyncResult = pool.map_async(get, range(N))
			pool.close()
			with alive_bar(I) as bar:
				for i in range(I):				
					print(f'main(): put({i})')
					Q.put(i)
					bar()
			for _ in range(N):
				Q.put(None)
			for res in results.get():
				print(f'get(): {res}')
			pool.join()

if __name__ == "__main__":
	run(main(int(sys.argv[1])))