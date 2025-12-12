"""Module to verify that spark-based integration tests can run in our CI environment, with all required
spark components (docker-compose container services) are up and running and integratable.
"""

import logging
import random
import sys
import uuid

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row
from brus_backend_common.helpers.aws import _get_boto3

logger = logging.getLogger(__name__)


def test_jvm_sparksession(spark: SparkSession):
    with SparkContext._lock:
        # Check the Singleton instance populated if there's an active SparkContext
        assert SparkContext._active_spark_context is not None
        sc = SparkContext._active_spark_context
        assert sc._jvm
        assert sc._jvm.SparkSession
        assert not sc._jvm.SparkSession.getDefaultSession().get().sparkContext().isStopped()


def test_hive_metastore_db(spark: SparkSession, s3_unittest_data_bucket, hive_unittest_metastore_db):
    """Ensure that schemas and tables created are tracked in the hive metastore_db"""
    test_schema = "my_delta_test_schema"
    test_table = "my_delta_test_table"
    spark.sql(f"create schema if not exists {test_schema}")
    spark.sql(
        f"""
        create table if not exists {test_schema}.{test_table}(id INT, name STRING, age INT)
        using delta
        location 's3a://{s3_unittest_data_bucket}/{test_table}'
    """
    )

    schemas_in_metastore = [s[0] for s in spark.sql("SHOW SCHEMAS").collect()]
    assert len(schemas_in_metastore) == 2
    assert "default" in schemas_in_metastore
    assert test_schema in schemas_in_metastore

    spark.sql(f"USE {test_schema}")
    tables_in_test_schema = [t for t in spark.sql("SHOW TABLES").collect()]
    assert len(tables_in_test_schema) == 1
    assert tables_in_test_schema[0]["namespace"] == test_schema
    assert tables_in_test_schema[0]["tableName"] == test_table


def test_tmp_hive_metastore_db_empty_on_test_start(spark: SparkSession, hive_unittest_metastore_db):
    """Test that when using the spark test fixture, the metastore_db is configured to live in a tmp directory,
    so that schemas and tables created while under-test only live or are known for the duration of a SINGLE test,
    not a test SESSION. And test that the metastore used for unit tests is empty on each test run (except for the
    empty "default" database"""
    # Ensure only the default schema exists
    schemas_in_metastore = [s[0] for s in spark.sql("SHOW SCHEMAS").collect()]
    assert len(schemas_in_metastore) == 1
    assert schemas_in_metastore[0] == "default"

    # Ensure the default schema has no tables
    spark.sql("USE DEFAULT")
    tables_in_default_schema = [t for t in spark.sql("SHOW TABLES").collect()]
    assert len(tables_in_default_schema) == 0


def test_spark_app_run_local_master(spark: SparkSession):
    """Execute a simple spark app and verify it logged expected output.
    Effectively if it runs without failing, it worked.

    NOTE: This will probably work regardless of whether any separately running (dockerized) spark infrastructure is
    present in the CI integration test env, because it will leverage the pyspark PyPI dependent package that is
    discovered in the PYTHONPATH, and treat the client machine as the spark driver.
    And furthermore, the default config for spark.master property if not set is local[*]
    """
    hadoop_version = (
        spark.sparkContext._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion()
        if spark.sparkContext._gateway
        else None
    )
    versions = f"""
    @       Python Version: {sys.version}
    @       Spark Version: {spark.version}
    @       Hadoop Version: {hadoop_version}
        """
    logger.info(versions)


def test_spark_write_csv_app_run(spark: SparkSession, s3_unittest_data_bucket):
    """More involved integration test that requires MinIO to be up as an s3 alternative."""
    data = [
        {"first_col": "row 1", "id": str(uuid.uuid4()), "color": "blue", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 2", "id": str(uuid.uuid4()), "color": "green", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 3", "id": str(uuid.uuid4()), "color": "pink", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 4", "id": str(uuid.uuid4()), "color": "yellow", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 5", "id": str(uuid.uuid4()), "color": "red", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 6", "id": str(uuid.uuid4()), "color": "orange", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 7", "id": str(uuid.uuid4()), "color": "magenta", "numeric_val": random.randint(-100, 100)},
    ]

    df = spark.createDataFrame([Row(**data_row) for data_row in data])
    # NOTE! NOTE! NOTE! MinIO locally does not support a TRAILING SLASH after object (folder) name
    df.write.option("header", True).csv(f"s3a://{s3_unittest_data_bucket}" f"/write_to_s3")

    # Verify there are *.csv part files in the chosen bucket
    s3_client = _get_boto3("client", "s3")
    response = s3_client.list_objects_v2(Bucket=s3_unittest_data_bucket)
    assert "Contents" in response  # the Bucket has contents
    bucket_objects = [c["Key"] for c in response["Contents"]]
    assert any([obj.endswith(".csv") for obj in bucket_objects])
