from tqdm import tqdm
from asyncio import sleep
from queutils import IterableQueue
import logging

logger = logging.getLogger(__name__)
error = logger.error


async def tqdm_monitorQ(
    Q: IterableQueue, bar: tqdm, batch: int = 1, freq: float = 0.5, close: bool = True
) -> None:
    """
    tqdm monitor for IterableQueue
    """
    try:
        while not Q.is_done:
            bar.update(Q.count * batch - bar.n)
            await sleep(freq)
        bar.update(Q.count * batch - bar.n)
    except KeyboardInterrupt:
        pass
    except Exception as err:
        error(f"{err}")
    finally:
        if close:
            bar.close()
