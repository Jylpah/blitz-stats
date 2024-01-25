#########################################################################
#
# arrow.py - Python Arrow based utils for dealing with Arrow data
#
#########################################################################

import logging

from os import makedirs
from os.path import isfile, dirname
from typing import Any
from enum import Enum, IntEnum
import os.path
from bson.objectid import ObjectId

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


def create_schema(
    obj: dict[str, Any], parent: str = "", default=pa.int32()
) -> list[tuple[str, Any]]:
    """Create Python Arrow schema as list of field, type tuples"""
    # obj_src: dict[str, Any] = obj.obj_src()
    schema: list[tuple[str, Any]] = list()
    for key, value in obj.items():
        schema_key: str = parent + key
        print(f"key={key}, value={value}, type={type(value)}")
        try:
            if isinstance(value, Enum):
                print(f"key={key} is Enum")
                if isinstance(value, IntEnum):
                    schema.append(
                        (schema_key, pa.dictionary(pa.int8(), pa.int32(), ordered=True))
                    )
                else:
                    schema.append(
                        (
                            schema_key,
                            pa.dictionary(pa.int8(), pa.string(), ordered=True),
                        )
                    )
            elif isinstance(value, float):
                schema.append((schema_key, pa.float32()))
            elif isinstance(value, int):
                if value > 10e6:
                    schema.append((schema_key, pa.int64()))
                else:
                    schema.append((schema_key, pa.int32()))
            elif isinstance(value, str):
                print(f"key={key} is string")
                schema.append((schema_key, pa.string()))
            elif isinstance(value, bool):
                schema.append((schema_key, pa.bool_()))
            elif isinstance(value, ObjectId):
                schema.append((schema_key, pa.string()))
            elif isinstance(value, dict):
                schema = schema + create_schema(value, f"{key}.")
            else:
                raise ValueError(
                    f"unsupported field type: field={schema_key}, type={type(value)}"
                )
        except Exception as err:
            error(err)
    return schema


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
    stats: EventCounter = EventCounter("writer")

    try:
        makedirs(dirname(basedir), exist_ok=True)
        export_file: str = os.path.join(basedir, f"{filename}.{export_format}")
        if not force and isfile(export_file):
            raise FileExistsError(export_file)
        # schema: pa.Schema = TankStat.arrow_schema()
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
    stats: EventCounter = EventCounter("writer")

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
