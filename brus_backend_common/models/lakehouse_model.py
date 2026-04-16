import importlib.util
import io
import logging
import os
import sys
import tempfile
from abc import ABC
from argparse import ArgumentTypeError
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Hashable, List

import deltalake
import pyarrow as pa
import pandas as pd
import polars as pl
from deltalake import DeltaTable, QueryBuilder  # Field, schema
from deltalake.writer import write_deltalake
from mypy_boto3_s3 import S3Client
from numpy.typing import DTypeLike
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType

from brus_backend_common.config import _SRC_ROOT_DIR, CONFIG
from brus_backend_common.helpers.aws import _get_boto3, get_storage_options
from brus_backend_common.helpers.pandas import convert_timestamp_df
from brus_backend_common.helpers.generic import step


logger = logging.getLogger(__name__)


class LakeHouseModelFormat(Enum):
    DELTA = "delta"
    CSV = "csv"


class LakeHouseDatabase(Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class LakeHouseModel(ABC):
    BUCKET_NAME: str
    RELATIVE_LAKEHOUSE_PATH: str = "data"
    DATABASE_NAME: LakeHouseDatabase
    TABLE_NAME: str
    DESCRIPTION: str
    CSV_NAME: str
    FORMAT: LakeHouseModelFormat
    PK: str
    UNIQUE_CONSTRAINTS: List[str | tuple[str]] | None = None
    MIGRATION_HISTORY: List[str] | None = None  # must be ordered by earliest to latest

    def __init__(self, spark: SparkSession | None = None) -> None:
        self._s3_client: S3Client = _get_boto3("client", "s3")
        self.RELATIVE_DATABASE_PATH: str = (
            f"{self.RELATIVE_LAKEHOUSE_PATH}/{self.FORMAT.value}/{self.DATABASE_NAME.value}"
        )
        self.DATABASE_PATH: str = f"s3://{self.BUCKET_NAME}/{self.RELATIVE_DATABASE_PATH}"
        self.DATABASE_PATH_HADOOP: str = f"s3a://{self.BUCKET_NAME}/{self.RELATIVE_DATABASE_PATH}"
        self.RELATIVE_TABLE_PATH: str = f"{self.RELATIVE_DATABASE_PATH}/{self.TABLE_NAME}"
        self.TABLE_PATH: str = f"{self.DATABASE_PATH}/{self.RELATIVE_TABLE_PATH}"
        self.TABLE_PATH_HADOOP: str = f"{self.DATABASE_PATH_HADOOP}/{self.RELATIVE_TABLE_PATH}"
        self.TABLE_REF: str = f"{self.DATABASE_NAME.value}.{self.TABLE_NAME}"

    def exists(self) -> bool:
        raise NotImplementedError()

    def count(self) -> int:
        raise NotImplementedError()

    def next_id(self) -> int:
        raise NotImplementedError()

    def to_pandas_df(self) -> pd.DataFrame | None:
        raise NotImplementedError()

    def to_polars_df(self) -> pl.DataFrame | pl.Series | None:
        raise NotImplementedError()

    def to_spark_df(self) -> DataFrame | None:
        raise NotImplementedError()

    def initialize(self, recreate: bool = False) -> None:
        raise NotImplementedError()

    def migrate(self, direction: int = 0) -> None:
        """
        direction (int): starting index of the migration list to run
            +1... - migrations
             0    - head/latest
            -1... - reverse migrations
        """
        migrations_dir = os.path.join(_SRC_ROOT_DIR, "models", "migrations")

        migration_model = LakeHouseCurrentMigration()
        mm_df = migration_model.to_pandas_df()
        if mm_df:
            current_migration = mm_df[mm_df.model == self.TABLE_REF].current_migration
            start = self.MIGRATION_HISTORY.index(current_migration) if current_migration else None

            # 0 - head/latest
            if direction == 0 and start is not None:
                # could drop the subtraction, just then relies on step working correctly
                direction = len(self.MIGRATION_HISTORY) - start

            # Reverse migrations always include the start, and for the first migration, include the start
            # Otherwise, skip the start up migration since we've already done it before
            include_start = direction < 0 or start == 0

            for index, migration in step(self.MIGRATION_HISTORY, start or None, direction, include_start=include_start):
                path = os.path.join(migrations_dir, f"{migration}.py")
                logger.info(f"Running migration {path} on {self.TABLE_REF}")
                if not os.path.exists(path):
                    raise FileNotFoundError(f"Migration {migration} not found.")

                spec = importlib.util.spec_from_file_location(migration, path)
                migration_module = importlib.util.module_from_spec(spec)
                sys.modules[migration] = migration_module
                spec.loader.exec_module(migration_module)

                if direction > 0:
                    if getattr(migration_module, "migrate"):
                        migration_module.migrate(self)
                    else:
                        raise NotImplementedError(f"No migrate function found in {path}.")

                    mm_df[mm_df.model == self.TABLE_REF].current_migration = migration
                    migration_model.save(mm_df)
                else:
                    if getattr(migration_module, "reverse_migrate"):
                        migration_module.reverse_migrate(self)
                    else:
                        raise NotImplementedError(f"No reverse_migrate function found in {path}.")

                    previous_migration = self.MIGRATION_HISTORY[index - 1] if index != 0 else None
                    mm_df[mm_df.model == self.TABLE_REF].current_migration = previous_migration
                    migration_model.save(mm_df)

    def save(self, df: pd.DataFrame | pl.DataFrame) -> None:
        # Easier to do the data manipulation/transformation on the pulled dataframes and simply save it instead of
        # implementing the merge/deletes for different data types (delta, parquet, csv).
        # TODO: how to update/delete a small portion of massive deltatable/csv via streaming?
        raise NotImplementedError()


class DeltaModel(LakeHouseModel):
    FORMAT = LakeHouseModelFormat.DELTA
    STRUCTURE: StructType

    # Used to repopulate the table from scratch
    # If the text is too large for the model, pull the text from a separate script.
    REPOPULATE_QUERIES: List[str | Callable[[SparkSession, str, str], None]]

    # Used to increment to the table
    # If the text is too large for the model, pull the text from a separate script.
    INCREMENT_QUERIES: List[str | Callable[[SparkSession, str, str], None]]

    def __init__(self, spark: SparkSession | None = None) -> None:
        super().__init__(spark=spark)

        self.spark: SparkSession | None = spark
        self.dt: DeltaTable | None = None

    def exists(self) -> bool:
        try:
            self.dt = DeltaTable(self.TABLE_PATH, storage_options=get_storage_options())
        except AnalysisException as e:
            if "DELTA_MISSING_DELTA_TABLE" in str(e):
                self.dt = None
            else:
                raise e

        return self.dt is not None

    def count(self) -> int:
        return self.dt.count() if self.dt is not None else -1

    def next_id(self) -> int:
        next_id = -1
        if self.exists() and self.dt is not None:
            # QueryBuilder needs a separate alias to the table
            table_ref_alias = self.TABLE_REF.replace(".", "_")
            max_id = (
                QueryBuilder()
                .register(table_ref_alias, self.dt)
                .execute(f"SELECT MAX({self.PK}) AS max_id FROM {table_ref_alias}")
                .read_all()["max_id"][0]
                .as_py()
            )
            next_id = max_id + 1
        return next_id

    def to_pandas_df(self) -> pd.DataFrame | None:
        df = None
        if self.exists() and self.dt is not None and self.dt.to_pyarrow_table() is not None:
            df = self.dt.to_pyarrow_table().to_pandas()
        return df

    def to_polars_df(self) -> pl.DataFrame | pl.Series | None:
        df = None
        if self.exists() and self.dt is not None:
            df = pl.from_arrow(self.dt.to_pyarrow_table())
        return df

    def to_spark_df(self) -> DataFrame | None:
        df = None
        if self.spark and self.exists():
            df = self.spark.read.table(self.TABLE_REF)
        return df

    def initialize(self, recreate: bool = False) -> None:
        logger.info(f"Initializing {self.TABLE_REF}")
        self._register_table_hive(recreate=recreate)

        if self.exists() and self.dt:
            # potentially, dt.alter.set_table_properties({"delta.minReaderVersion":"3","delta.minWriterVersion":"7"})
            self.dt.alter.add_feature(
                [deltalake.table.TableFeatures.TimestampWithoutTimezone],
                allow_protocol_versions_increase=True,
            )

    def _register_table_hive(self, recreate: bool = False) -> None:
        if self.spark:
            self.spark.sql(
                rf"""
                CREATE DATABASE IF NOT EXISTS {self.DATABASE_NAME.value}
                LOCATION '{self.DATABASE_PATH_HADOOP}'
            """
            ).show()

            structure = self.STRUCTURE
            if self.PK:
                # Drop from the initial structure, will be added afterward as a special id
                structure = StructType([field for field in structure.fields if field.name != self.PK])

            df = self.spark.createDataFrame([], structure)
            if self.PK:
                df = df.withColumn(self.PK, monotonically_increasing_id())
            if recreate:
                # Remove what's already there and start from scratch
                (
                    df.write.format(self.FORMAT.value)
                    .option("path", self.TABLE_PATH_HADOOP)
                    .option("overwriteSchema", "true")
                    .mode("overwrite")
                    .saveAsTable(self.TABLE_REF)
                )
            else:
                # Equivalent of create table if not exists
                (
                    df.write.format(self.FORMAT.value)
                    .option("path", self.TABLE_PATH_HADOOP)
                    .mode("ignore")
                    .saveAsTable(self.TABLE_REF)
                )
            # TODO: This *should* allow one to run `ALTER TABLE DROP COLUMN ...` commands
            #       but we ran into issues when trying it.
            # self.spark.sql(f"""
            #     ALTER TABLE {self.TABLE_REF} SET TBLPROPERTIES (
            #       'delta.minReaderVersion' = '2',
            #       'delta.minWriterVersion' = '5',
            #       'delta.columnMapping.mode' = 'name'
            #     )
            # """)

    def repopulate(self) -> None:
        if self.REPOPULATE_QUERIES:
            self.load_queries(self.REPOPULATE_QUERIES)
        else:
            raise NotImplementedError(f"No repopulate queries provided for {self.TABLE_REF}")

    def increment(self) -> None:
        if self.INCREMENT_QUERIES:
            self.load_queries(self.INCREMENT_QUERIES)
        else:
            raise NotImplementedError(f"No increment queries provided for {self.TABLE_REF}")

    def load_queries(self, queries: list[str | Callable[[SparkSession, str, str], None]]) -> None:
        for index, query in enumerate(queries):
            logger.info(f"Running query number: {index + 1}\n")
            self.load_query(query)

    def load_query(self, query: str | Callable[[SparkSession, str, str], None]) -> None:
        if isinstance(query, str) and self.spark:
            self.spark.sql(query)
        elif not isinstance(query, str) and self.spark:
            query(self.spark, self.DATABASE_NAME.value, self.TABLE_NAME)
        else:
            raise ArgumentTypeError(f"Invalid query. `{query}` must be a string or a Callable.")

    def save(self, df: pd.DataFrame | pl.DataFrame):
        if isinstance(df, pd.DataFrame):
            if self.PK and self.PK not in df.columns:
                df[self.PK] = df.index + 1

            # Convert to PyArrow
            # Type checker struggles with the concept
            df = pa.Table.from_pandas(df)  # type: ignore
        else:
            if self.PK and self.PK not in df.columns:
                df = df.with_row_index(name=self.PK, offset=1)
        write_deltalake(table_or_uri=self.TABLE_PATH, data=df, mode="overwrite", storage_options=get_storage_options())


class CSVModel(LakeHouseModel):
    FORMAT = LakeHouseModelFormat.CSV
    DTYPES: dict[Hashable, DTypeLike]
    CSV_NAME: str

    def __init__(self, spark: SparkSession | None = None) -> None:
        super().__init__(spark=spark)

        self._s3_object: bytes | None = None

        self.RELATIVE_CSV_PATH: str = f"{self.RELATIVE_TABLE_PATH}/{self.CSV_NAME}"
        self.CSV_PATH: str = f"{self.TABLE_PATH}/{self.CSV_NAME}"
        self.CSV_PATH_HADOOP: str = f"{self.TABLE_PATH_HADOOP}/{self.CSV_NAME}"

    def exists(self) -> bool:
        self._s3_object = None
        try:
            self._s3_object = self._s3_client.get_object(Bucket=self.BUCKET_NAME, Key=self.RELATIVE_CSV_PATH)[
                "Body"
            ].read()
        except self._s3_client.exceptions.NoSuchKey:
            logger.warning(f"CSV not found. Please run initialize recreate or upload the file to {self.CSV_PATH}")

        return self._s3_object is not None

    def count(self) -> int:
        count = -1
        df = self.to_pandas_df()
        if df is not None:
            count = len(df)
        return count

    def next_id(self) -> int:
        df = self.to_pandas_df()
        return df[self.PK].max() + 1 if df is not None else -1

    def initialize(self, recreate: bool = False) -> None:
        logger.info(f"Initializing {self.TABLE_REF}")
        if not recreate and not self.exists():
            raise FileNotFoundError(
                f"CSV not found. Please run initialize recreate or upload the file to {self.CSV_PATH}"
            )
        elif not recreate and self.exists():
            logger.info(f"{self.TABLE_REF} already initialized.")
        else:
            logger.info(f"Recreating {self.TABLE_REF} with a blank file.")
            self._recreate_blank_file()
            self.exists()

    def _recreate_blank_file(self):
        df = pd.DataFrame(columns=list(self.DTYPES))
        with tempfile.TemporaryDirectory() as temp_dir:
            blank_csv = os.path.join(temp_dir, self.CSV_NAME)
            df.to_csv(blank_csv, index=False)
            self._s3_client.upload_file(blank_csv, self.BUCKET_NAME, self.RELATIVE_CSV_PATH)

    def to_pandas_df(self, **kwargs: Any) -> pd.DataFrame | None:
        # Type Checker struggles with BytesIO and S3 Objects
        cols = list(self.DTYPES)
        return (
            pd.read_csv(
                io.BytesIO(self._s3_object),
                dtype={k: v for k, v in self.DTYPES.items() if v != datetime},
                parse_dates=[k for k, v in self.DTYPES.items() if v == datetime],
                usecols=cols,
                **kwargs,
            )[cols]
            if self.exists()
            else None
        )  # type: ignore

    def to_polars_df(self, **kwargs: Any) -> pl.DataFrame | pl.Series | None:
        return pl.read_csv(self.CSV_PATH, **kwargs) if self.exists() else None

    def save(self, df: pd.DataFrame | pl.DataFrame) -> None:
        # Using pandas with its built-in S3 support
        if isinstance(df, pl.DataFrame):
            df = df.to_pandas()

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        self._s3_client.put_object(Bucket=self.BUCKET_NAME, Key=self.RELATIVE_CSV_PATH, Body=csv_buffer.getvalue())

        # TODO: Use the line alongside updating botocore version
        # The simple line below involves installing fspec, s3fs[boto3], and aiobotocore
        # and there is no aibotocore version that supports our current supported botocore 1.34.58
        # df.to_csv(self.CSV_PATH, index=False)


class LakeHouseCurrentMigration(CSVModel):
    BUCKET_NAME = CONFIG.REFERENCE_S3_BUCKET
    DATABASE_NAME = LakeHouseDatabase.BRONZE
    TABLE_NAME = "migrations"
    DESCRIPTION = "Keeps track of migrations for all Lakehouse Models"
    CSV_NAME = "current_migrations.csv"
    PK = "model_id"
    UNIQUE_CONSTRAINTS = ["model"]
    MIGRATION_HISTORY = []
    DTYPES = {
        "created_at": datetime,
        "updated_at": datetime,
        "model_id": pd.Int64Dtype(),
        "model": pd.StringDtype(),
        "current_migration": pd.StringDtype(),
    }


class ExternalDataLoadDate(CSVModel):
    BUCKET_NAME = CONFIG.REFERENCE_S3_BUCKET
    DATABASE_NAME = LakeHouseDatabase.BRONZE
    TABLE_NAME = "external_data_load_date"
    DESCRIPTION = "Keeps track of load dates of certain external data Lakehouse models"
    CSV_NAME = "external_load_date.csv"
    PK = "external_data_load_date_id"
    UNIQUE_CONSTRAINTS = ["name"]
    MIGRATION_HISTORY = []
    DTYPES = {
        "created_at": datetime,
        "updated_at": datetime,
        "external_data_load_date_id": pd.Int64Dtype(),  # nullable int
        "name": pd.StringDtype(),
        "description": pd.StringDtype(),
        "last_load_date_start": datetime,
        "last_load_date_end": datetime,
    }


def update_external_data_load_date(model: LakeHouseModel, start_time: datetime, end_time: datetime):
    """Update the external_data_load_date table with the start and end times for the given data type

    Args:
        model: the corresponding model associated with the loader to update the load dates
        start_time: a datetime object indicating the start time of the external data load
        end_time: a datetime object indicating the end time of the external data load
    """
    edld_model = ExternalDataLoadDate()
    df = edld_model.to_pandas_df()

    if df is None:
        raise FileNotFoundError(
            f"ExternalDataLoadDate CSV not found."
            f" Please run initialize recreate or upload the file to {edld_model.CSV_PATH}"
        )
    last_stored_obj = df[df.name == model.TABLE_REF]
    if last_stored_obj.empty:
        new_entry_dict = {
            "created_at": convert_timestamp_df(datetime.now()),
            "updated_at": [None],  # will be updated later
            "external_data_load_date_id": [edld_model.next_id()],
            "name": [model.TABLE_REF],
            "description": [model.DESCRIPTION],
            "last_load_date_start": [None],  # will be updated later
            "last_load_date_end": [None],  # will be updated later
        }
        new_entry = pd.DataFrame(new_entry_dict)

        last_stored_obj = pd.concat([last_stored_obj, new_entry], ignore_index=True)
        df = pd.concat([df, new_entry], ignore_index=True)

    last_stored_obj["last_load_date_start"] = convert_timestamp_df(start_time)
    last_stored_obj["last_load_date_end"] = convert_timestamp_df(end_time)
    last_stored_obj["updated_at"] = convert_timestamp_df(datetime.now())

    df.set_index(edld_model.PK, inplace=True)
    last_stored_obj.set_index(edld_model.PK, inplace=True)
    df.update(last_stored_obj)
    df.reset_index(inplace=True)

    edld_model.save(df)
