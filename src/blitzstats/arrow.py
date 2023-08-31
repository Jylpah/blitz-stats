#########################################################################
#
# arrow.py - Python Arrow based utils for dealing with Arrow data
#
#########################################################################

import logging

from os import makedirs
from os.path import isfile, dirname
from aiofiles import open

import os.path

from asyncio import CancelledError

# export data
import pandas as pd  # type: ignore
import pyarrow as pa  # type: ignore
import pyarrow.dataset as ds  # type: ignore
import pyarrow.parquet as pq  # type: ignore

from pyutils import AsyncQueue, EventCounter

logger = logging.getLogger()
error = logger.error
message = logger.warning
verbose = logger.info
debug = logger.debug

EXPORT_DATA_FORMATS: list[str] = ["parquet", "arrow"]
DEFAULT_EXPORT_DATA_FORMAT: str = EXPORT_DATA_FORMATS[0]

EXPORT_WRITE_BATCH: int = int(50e6)


async def data_writer(
    basedir: str,
    filename: str,
    dataQ: AsyncQueue[pd.DataFrame],
    export_format: str,
    schema: pa.Schema,
    force: bool = False,
) -> EventCounter:
    """Worker to write on data stats to a file in format"""
    debug("starting")
    # global export_total_rows
    assert (
        export_format in EXPORT_DATA_FORMATS
    ), f"export format has to be one of: {', '.join(EXPORT_DATA_FORMATS)}"
    stats: EventCounter = EventCounter(f"writer")

    try:
        makedirs(dirname(basedir), exist_ok=True)
        export_file: str = os.path.join(basedir, f"{filename}.{export_format}")
        if not force and isfile(export_file):
            raise FileExistsError(export_file)
        # schema: pa.Schema = WGTankStat.arrow_schema()
        if export_format == "parquet":
            with pq.ParquetWriter(export_file, schema, compression="lz4") as writer:
                while True:
                    batch = pa.RecordBatch.from_pandas(await dataQ.get(), schema)
                    try:
                        writer.write_batch(batch)
                        stats.log("rows written", batch.num_rows)
                    except Exception as err:
                        error(f"{err}")
                    dataQ.task_done()
        else:
            raise NotImplementedError(f"export format not implemented: {export_format}")

    except CancelledError:
        debug("cancelled")
    except Exception as err:
        error(f"{err}")

    return stats


async def dataset_writer(
    basedir: str,
    dataQ: AsyncQueue[pd.DataFrame],
    export_format: str,
    partioning: ds.Partitioning,
    schema: pa.schema,
    force: bool = False,
) -> EventCounter:
    """Worker to write on data stats to a file in format"""
    global EXPORT_WRITE_BATCH
    debug("starting")
    assert (
        export_format in EXPORT_DATA_FORMATS
    ), f"export format has to be one of: {', '.join(EXPORT_DATA_FORMATS)}"
    stats: EventCounter = EventCounter(f"writer")

    try:
        makedirs(dirname(basedir), exist_ok=True)

        # batch 	: pa.RecordBatch = pa.RecordBatch.from_pandas(await dataQ.get())
        # schema 	: pa.Schema 	 = batch.schema
        batch: pa.RecordBatch
        # part: ds.Partitioning = ds.partitioning(schema=schema)
        dfs: list[pa.RecordBatch] = list()
        rows: int = 0
        i: int = 0
        try:
            while True:
                batch = pa.RecordBatch.from_pandas(await dataQ.get(), schema)
                try:
                    rows += batch.num_rows
                    dfs.append(batch)
                    if rows > EXPORT_WRITE_BATCH:
                        debug(f"writing {rows} rows")
                        ds.write_dataset(
                            dfs,
                            base_dir=basedir,
                            basename_template=f"part-{i}"
                            + "-{i}."
                            + f"{export_format}",
                            format=export_format,
                            partitioning=partioning,
                            schema=schema,
                            existing_data_behavior="overwrite_or_ignore",
                        )
                        stats.log("stats written", rows)
                        rows = 0
                        i += 1
                        dfs = list()
                except Exception as err:
                    error(f"{err}")
                dataQ.task_done()
                # batch = pa.RecordBatch.from_pandas(await dataQ.get(), schema)

        except CancelledError:
            debug("cancelled")

        if len(dfs) > 0:
            ds.write_dataset(
                dfs,
                base_dir=basedir,
                basename_template=f"part-{i}" + "-{i}." + f"{export_format}",
                format=export_format,
                partitioning=partioning,
                schema=schema,
                existing_data_behavior="overwrite_or_ignore",
            )
            stats.log("stats written", rows)

    except Exception as err:
        error(f"{err}")
    return stats
