import os
import logging
import pandas as pd
import polars as pl
from abc import ABC
from argparse import ArgumentTypeError
from typing import Callable

from deltalake import DeltaTable  # , QueryBuilder, Field, schema
from deltalake.exceptions import TableNotFoundError

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from brus_backend_common.config import _SRC_ROOT_DIR
from brus_backend_common.helpers.aws_helpers import get_aws_credentials


logger = logging.getLogger(__name__)


def get_storage_options():
    """DeltaLake library doesn't use boto3 and doesn't pull the aws creds the same way."""
    aws_creds = get_aws_credentials()
    return {
        "AWS_ACCESS_KEY_ID": aws_creds.access_key,
        "AWS_SECRET_ACCESS_KEY": aws_creds.secret_key,
        "AWS_REGION": "us-gov-west-1",
        "AWS_SESSION_TOKEN": aws_creds.token,
    }


class DeltaModel(ABC):
    S3_BUCKET: str
    DATABASE: str
    TABLE_NAME: str
    CSV_NAME: str
    FORMAT: str = "delta"
    PK: str
    UNIQUE_CONSTRAINTS: [(str,)]
    MIGRATION_HISTORY: [str]

    @classmethod
    @property
    def DATABASE_PATH(cls):
        return f"s3://{cls.S3_BUCKET}/data/delta/{cls.DATABASE}"

    @classmethod
    @property
    def DATABASE_PATH_HADOOP(cls):
        return f"s3a://{cls.S3_BUCKET}/data/delta/{cls.DATABASE}"

    @classmethod
    @property
    def TABLE_PATH(cls):
        _csv_extension = f'/{cls.CSV_NAME or f"{cls.TABLE_NAME}.csv"}' if cls.FORMAT == "csv" else ""
        return f"s3://{cls.S3_BUCKET}/data/delta/{cls.DATABASE}/{cls.TABLE_NAME}{_csv_extension}"

    @classmethod
    @property
    def TABLE_PATH_HADOOP(cls):
        _csv_extension = f'/{cls.CSV_NAME or f"{cls.TABLE_NAME}.csv"}' if cls.FORMAT == "csv" else ""
        return f"s3a://{cls.S3_BUCKET}/data/delta/{cls.DATABASE}/{cls.TABLE_NAME}{_csv_extension}"

    @classmethod
    @property
    def TABLE_REF(cls):
        return f"{cls.DATABASE}.{cls.TABLE_NAME}"

    # The schema/structure of the delta table as StructType with StructFields
    STRUCTURE: StructType

    # Used to repopulate the table from scratch
    # If the text is too large for the model, pull the text from a separate script.
    REPOPULATE_QUERY: str | Callable[[SparkSession], DataFrame]

    # Used to increment to the table
    # If the text is too large for the model, pull the text from a separate script.
    INCREMENT_QUERY: str | Callable[[SparkSession], DataFrame]

    def __init__(self, spark=None):
        self.spark = spark

        try:
            self.dt = DeltaTable(self.TABLE_PATH, storage_options=get_storage_options())
        except TableNotFoundError:
            self.dt = None

    def exists(self):
        return self.dt is not None

    def to_pandas_df(self):
        return self.dt.to_pyarrow_table().to_pandas()

    def to_polars_df(self):
        return pl.from_arrow(self.dt.to_pyarrow_table())

    def initialize(self, recreate=False):
        logger.info(f"Initializing {self.TABLE_REF}")
        self._register_table_hive(recreate=recreate)
        if not self.dt:
            self.dt = DeltaTable(self.TABLE_PATH, storage_options=get_storage_options())
        else:
            logger.info(f"{self.TABLE_PATH} already initialized")

    def _register_table_hive(self, recreate=False):
        self.spark.sql(
            rf"""
            CREATE DATABASE IF NOT EXISTS {self.DATABASE}
            LOCATION '{self.DATABASE_PATH_HADOOP}'
        """
        )
        df = self.spark.createDataFrame([], self.STRUCTURE)
        if recreate:
            (
                df.write.format(self.FORMAT)
                .option("path", self.TABLE_PATH_HADOOP)
                .option("overwriteSchema", "true")
                .mode("overwrite")
                .saveAsTable(self.TABLE_REF)
            )
        else:
            (
                df.write.format(self.FORMAT)
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

    def migrate(self, start=0):
        """
        start (int): starting index of the migration list to run
            0 - all migrations
            -1 - last migration
        """
        migrations_dir = os.path.join(_SRC_ROOT_DIR, "models", "migrations")
        for migration in self.MIGRATION_HISTORY[start:]:
            path = os.path.join(migrations_dir, f"{migration}.sql")
            logger.info(f"Running migration {path} on {self.TABLE_REF}")
            if not os.path.exists(path):
                raise FileNotFoundError(f"Migration {migration} not found.")
            with open(path, "r") as f:
                spark_sql = f.read()
            if spark_sql:
                self.spark.sql(spark_sql)
            else:
                logger.info(f"No SQL found in {path}.")

    def repopulate(self):
        self.load_query(self.REPOPULATE_QUERY)

    def increment(self):
        self.load_query(self.INCREMENT_QUERY)

    def load_query(self, query: str | Callable[[SparkSession], DataFrame]):
        if isinstance(query, str):
            self.spark.sql(query)
        elif isinstance(query, Callable):
            (
                query(self.spark)
                .write.format("delta")
                .mode("overwrite")
                .option("path", self.TABLE_PATH_HADOOP)
                .saveAsTable(self.TABLE_REF)
            )
        else:
            raise ArgumentTypeError(f"Invalid query. `{query}` must be a string or a Callable.")

    def merge(self, df: [pd.DataFrame, pl.DataFrame]):
        if isinstance(df, pd.DataFrame):
            df = pl.from_pandas(df)

        if not self.dt:
            raise Exception("Table not instantiated")

        self.dt.merge(
            source=df,
            predicate=f"s.{self.PK} = t.{self.PK}",
            source_alias="s",
            target_alias="t",
        ).when_matched_update_all().when_not_matched_insert_all().execute()

    def delete(self, predicate: str = None):
        if not self.dt:
            raise Exception("Table not instantiated")

        self.dt.delete(predicate=predicate)
