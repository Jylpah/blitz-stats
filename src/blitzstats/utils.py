from tqdm import tqdm
from asyncio import sleep, CancelledError
from queutils import IterableQueue
import logging

logger = logging.getLogger(__name__)
error = logger.error


async def tqdm_monitorQ(
    Q: IterableQueue, bar: tqdm, freq: float = 0.5, close: bool = True
) -> None:
    """
    tqdm monitor for IterableQueue
    """
    try:
        while not Q.is_done:
            bar.update(Q.count - bar.n)
            await sleep(freq)
        bar.update(Q.count - bar.n)
    except CancelledError:
        pass
    except Exception as err:
        error(f"{err}")
    finally:
        if close:
            bar.close()
