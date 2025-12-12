import logging
import os
import pytest
import uuid
from typing import TYPE_CHECKING, Generator, List

from botocore.errorfactory import ClientError

from brus_backend_common.helpers.spark import (
    configure_spark_session,
    is_spark_context_stopped,
    stop_spark_context,
)
from brus_backend_common.config import CONFIG
from brus_backend_common.helpers.aws import _get_boto3
from brus_backend_common.helpers.configs import LOCAL_BASIC_EXTRA_CONF  # LOCAL_EXTENDED_EXTRA_CONF
from brus_backend_common.models import LAKEHOUSE_MODELS, LAKEHOUSE_BUCKET_NAMES
from brus_backend_common.scripts.create_migrate_lakehouse_model import main as create_migrate

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# ==== Spark Automated Integration Test Fixtures ==== #

# How to determine a working dependency set:
# 1. What platform are you using? local dev with pip-installed PySpark? EMR 6.x or 5.x? Databricks Runtime?
# 2. From there determine what versions of Spark + Hadoop are supported on that platform. If going cross-platform,
#    try to pick a combo that's supported on both
# 3. Is there a hadoop-aws version matching the platform's Hadoop version used? Because we need to have Spark writing
#    to S3, we are beholden to the AWS-provided JARs that implement the S3AFileSystem, which are part of the
#    hadoop-aws JAR.
# 4. Going from the platform-hadoop version, find the same version of hadoop-aws up in
#    https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/
#    and look to see what version its dependent JARs are at that your code requires are runtime. If seeing errors or are
#    uncertain of compatibility, see what working version-sets are aligned to an Amazon EMR release here:
#    https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-app-versions-6.x.html

DELTA_LAKE_UNITTEST_SCHEMA_NAME = "unittest"


def s3_unittest_data_bucket_setup(bucket_name: str) -> str:
    logging.warning(
        f"Attempting to create unit test data bucket {bucket_name} "
        f"at: http://{CONFIG.AWS_S3_ENDPOINT} using CONFIG.AWS_ACCESS_KEY and CONFIG.AWS_SECRET_KEY"
    )

    s3_client = _get_boto3("client", "s3")
    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        if "BucketAlreadyOwnedByYou" in str(e):
            # Simplest way to ensure the bucket is created is to swallow the exception saying it already exists
            logging.warning("Unit Test Data Bucket not created; already exists.")
            pass
        else:
            raise e

    logging.info(
        f"Unit Test Data Bucket '{bucket_name}' created (or found to exist) at S3 endpoint "
        f"'{bucket_name}'. Current Buckets:"
    )
    for bucket in s3_client.list_buckets()["Buckets"]:
        logging.info(f"  {bucket['Name']}")

    return bucket_name


def s3_unittest_data_bucket_teardown(bucket_name: str, remove_bucket: bool = False) -> None:
    s3_client = _get_boto3("client", "s3")
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    if "Contents" in response:
        for object in response["Contents"]:
            s3_client.delete_object(Bucket=bucket_name, Key=object["Key"])
    if remove_bucket:
        s3_client.delete_bucket(Bucket=bucket_name)


@pytest.fixture(scope="session")
def s3_unittest_data_bucket_setup_and_teardown(worker_id: str) -> Generator[str, None, None]:
    """Create a test bucket so the tests can use it

    Args:
        worker_id: ID of worker if this session is one of multiple in a parallel pytest-xdist run, used as a suffix
        to the unit test S3 bucket if provided.

    Returns:
        unittest_data_bucket: Bucket name of the unit test S3 bucket created for this pytest session
    """
    worker_prefix = "" if (not worker_id or worker_id == "master") else worker_id + "-"
    unittest_data_bucket = "unittest-data-{}".format(worker_prefix + str(uuid.uuid4()))
    unittest_data_bucket = s3_unittest_data_bucket_setup(unittest_data_bucket)

    yield unittest_data_bucket

    # Cleanup by removing all objects in the bucket by key, and then the bucket itself after the test session
    s3_unittest_data_bucket_teardown(unittest_data_bucket, remove_bucket=True)


@pytest.fixture(scope="function")
def s3_unittest_data_bucket(s3_unittest_data_bucket_setup_and_teardown: str) -> Generator[str, None, None]:
    """Use the S3 unit test data bucket created for the test session, and cleanup any contents created in it after
    each test
    """
    unittest_data_bucket = s3_unittest_data_bucket_setup_and_teardown

    yield unittest_data_bucket

    # Cleanup any contents added to the bucket for this test
    s3_unittest_data_bucket_teardown(unittest_data_bucket, remove_bucket=False)
    # NOTE: Leave the bucket itself there for other tests in this session. It will get cleaned up at the end of the
    # test session by the dependent fixture


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Generator["SparkSession", None, None]:
    """Throw an error if coming into a test using this fixture which needs to create a
    NEW SparkContext (i.e. new JVM invocation to run Spark in a java process)
    AND, proactively cleanup any SparkContext created by this test after it completes

    This fixture will create ONE single SparkContext to be shared by ALL unit tests (and therefore must be populated
    with universally compatible config and with the superset of all JAR dependencies our test code might need.
    """
    if not is_spark_context_stopped():
        raise Exception(
            "Error: Test session cannot create a SparkSession because one already exists at the time this "
            "test-session-scoped fixture is being evaluated."
        )

    # Storing spark warehouse and hive metastore_db in a tmpdir so it does not leave cruft behind from test session runs
    # So as not to have interfering schemas and tables in the metastore_db from individual test run to run,
    # another test-scoped fixture should be created, pulling this in, and blowing away all schemas and tables as part
    # of each run
    spark_sql_warehouse_dir = str(tmp_path_factory.mktemp(basename="spark-warehouse", numbered=False))
    extra_conf = {
        **LOCAL_BASIC_EXTRA_CONF,
        # **LOCAL_EXTENDED_EXTRA_CONF,
        "spark.sql.warehouse.dir": spark_sql_warehouse_dir,
        "spark.hadoop.javax.jdo.option.ConnectionURL": f"jdbc:derby:;databaseName={spark_sql_warehouse_dir}/metastore_db;create=true",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    }
    spark = configure_spark_session(
        app_name="Unit Test Session",
        log_level=logging.INFO,
        log_spark_config_vals=True,
        enable_hive_support=True,
        # Type Checkers struggle with **kwargs and there's still no consensus on how to resolve them
        **extra_conf,  # type: ignore
    )  # type: SparkSession

    # Cut down spark logs to warning, overwrites each time
    spark_home = os.environ.get("SPARK_HOME")
    if spark_home:
        noe4j_conf_dir = os.path.join(spark_home, "conf")
        neo4j_properties_path = os.path.join(noe4j_conf_dir, "log4j2.properties")
        if not os.path.exists(noe4j_conf_dir):
            os.mkdir(noe4j_conf_dir)
        neo4j_properties_config = {
            "appender.console.type": "Console",
            "appender.console.name": "CONSOLE",
            "appender.console.layout.type": "PatternLayout",
            "appender.console.layout.pattern": "[%d{yyyy-MM-dd HH:mm:ss.SSS}][%p] - %m%n",
            "rootLogger.level": "WARN",
            "rootLogger.appenderRef.0.ref": "CONSOLE",
            "rootLogger.appenderRef.0.level": "WARN",
        }
        with open(neo4j_properties_path, "w") as neo4j_properties:
            for k, v in neo4j_properties_config.items():
                neo4j_properties.write(f"{k} = {v}\n")

    yield spark

    stop_spark_context()


@pytest.fixture
def hive_unittest_metastore_db(spark: "SparkSession") -> Generator[str | None, None, None]:
    """A fixture that WIPES all of the schemas (aka databases) and tables in each schema from the hive metastore_db
    at the end of each test run, so that the metastore is fresh.

    NOTE: This relies on setup in the session-scoped ``spark`` fixture:
      - That fixture must enableHiveSupport() when creating the SparkSession
      - That fixture needs to set the filesystem location of the hive metastore_db (Derby DB) folder in a tmp dir
        - (so that it doesn't interfere or leave cruft behind)
      - That fixture needs to set the Spark SQL Warehouse dir in a tmp dir
        - (so that it doesn't interfere or leave cruft behind)

    WARNING: If the spark test fixture is not setup to initialize the hive metastore_db in this way for the
    SparkSession used by tests, then this fixture may inadvertently wipe all hive schemas and tables in you dev env
    """
    metastore_db_path = None
    metastore_db_url = spark.conf.get("spark.hadoop.javax.jdo.option.ConnectionURL")
    if metastore_db_url:
        metastore_db_path = metastore_db_url.split("=")[1].split(";")[0]

    yield metastore_db_path

    schemas_in_metastore = [s[0] for s in spark.sql("SHOW SCHEMAS").collect()]

    # Cascade will remove the tables and functions in each SCHEMA *other than* the default (cannot drop that one)
    for s in schemas_in_metastore:
        if s == "default":
            continue
        spark.sql(f"DROP SCHEMA IF EXISTS {s} CASCADE")

    # Handle default schema specially
    spark.sql("USE DEFAULT")
    tables_in_default_schema = [t for t in spark.sql("SHOW TABLES").collect()]
    for t in tables_in_default_schema:
        spark.sql(f"DROP TABLE IF EXISTS {t['tableName']}")


@pytest.fixture
def delta_lake_unittest_schema(spark: "SparkSession", hive_unittest_metastore_db: str) -> Generator[str, None, None]:
    """Specify which Delta 'SCHEMA' to use (NOTE: 'SCHEMA' and 'DATABASE' are interchangeable in Delta Spark SQL),
    and cleanup any objects created in the schema after the test run."""

    # Force default usage of the unittest schema in this SparkSession
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DELTA_LAKE_UNITTEST_SCHEMA_NAME}")
    spark.sql(f"USE {DELTA_LAKE_UNITTEST_SCHEMA_NAME}")

    # Yield the name of the db that test delta lake tables and records should be put in.
    yield DELTA_LAKE_UNITTEST_SCHEMA_NAME

    # The dependent hive_unittest_metastore_db fixture will take care of cleaning up this schema post-test


@pytest.fixture(scope="session")
def setup_teardown_buckets_session() -> Generator[List[str], None, None]:
    buckets = LAKEHOUSE_BUCKET_NAMES + [CONFIG.PUBLIC_FILES_BUCKET, CONFIG.METRICS_BUCKET, CONFIG.DATA_SOURCES_BUCKET]
    for bucket in buckets:
        s3_unittest_data_bucket_setup(bucket)

    yield buckets

    for bucket in buckets:
        s3_unittest_data_bucket_teardown(bucket, remove_bucket=False)


@pytest.fixture(scope="function")
def setup_teardown_buckets(setup_teardown_buckets_session: str) -> Generator[str, None, None]:
    buckets = setup_teardown_buckets_session

    yield buckets

    # for every function test, clear out the buckets (while still keeping them around)
    for bucket in buckets:
        s3_unittest_data_bucket_teardown(bucket, remove_bucket=False)


def create_all_delta_tables(spark: "SparkSession", s3_bucket: str, tables_to_load: list) -> None:
    load_tables = [val for val in tables_to_load if val in LAKEHOUSE_MODELS]
    for dest_table in load_tables:
        create_migrate(dest_table)


def create_and_load_all_delta_tables(spark: "SparkSession", s3_bucket: str, tables_to_load: list) -> None:
    create_all_delta_tables(spark, s3_bucket, tables_to_load)

    load_tables = [val for val in tables_to_load if val in LAKEHOUSE_MODELS]

    for dest_table in load_tables:
        dest_table.populate()
