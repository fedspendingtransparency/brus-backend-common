"""

Both Broker and USAspending have their own distinct config/setting/environment infrastructures.
While this repository may lead those infrastructures to merge together down the road (or may be its own special setup),
for now, this file includes a dict config of all the values shared by these components and it'll be required of each of
them to override the config with their specific values upon importing and using this package. This is also to avoid
having to keep track of multiple configs in various locations.

At the beginning of a script using this library or after the web application has loaded its values:

from brus_backend_common.config import set_brus_config
...
set_brus_config({
    'DB1': [your application's postgres connection url]
    ...
}
...

Note: When working locally, do not modify this file and update your values in ".env" (copied from .env.template)
      Pydantic's dotenv will automagically populate it based on it
"""

import os
import pathlib
from pydantic import BaseSettings, SecretStr

from brus_backend_common.helpers.uri import get_jdbc_url_from_pg_uri

_PROJECT_NAME = "brus-backend-common"
# WARNING: This is relative to THIS file's location. If it is moved/refactored, this needs to be confirmed to point
# to the project root dir (i.e. brus-backend-common/)
_PROJECT_ROOT_DIR: pathlib.Path = pathlib.Path(__file__).parent.parent.resolve()
_SRC_ROOT_DIR: pathlib.Path = _PROJECT_ROOT_DIR / _PROJECT_NAME.replace("-", "_")

ENV_FILE_PATH = os.path.join(_PROJECT_ROOT_DIR, ".env")


class DefaultConfig(BaseSettings):
    """Top-level config that defines all configuration variables, and their default, overridable values

    Attributes:
        # App
        IS_LOCAL: Whether it's running locally or remotely
        PROJECT_LOG_DIR: where log files will be kept
        TRACE_ENV: set by deploys to help with tracing

        # AWS
        AWS_ACCESS_KEY: The current AWS access key
        AWS_SECRET_KEY: The current AWS secret key
        AWS_PROFILE: The current AWS profile
        AWS_REGION: The current AWS region
        AWS_S3_ENDPOINT: (derived) The current AWS S3 endpoint
        AWS_STS_ENDPOINT: (derived) The current AWS S3 endpoint

        # Buckets
        DATA_SOURCES_BUCKET: The S3 data sources bucket name
        PUBLIC_FILES_BUCKET: The S3 public files bucket name
        BROKER_S3_BUCKET: The S3 broker bucket name
        REFERENCE_S3_BUCKET: The S3 reference bucket name
        USAS_S3_BUCKET: The S3 USAS bucket name
        METRICS_BUCKET: The S3 metrics bucket name

        # Postgres
        DB1_URL: Postgres url to your applications database
        DB2_URL: Postgres url to another application database
                     (i.e. USAS pulling from the broker database directly)
        JDBC_DB1_URL: (derived) JDBC url to your applications database
        JDBC_DB2_URL: (derived) JDBC url to another applications database

        # Metastore
        METASTORE_URL: Postgres url to your metastore database (note: doesn't require user:pass)
        JDBC_METASTORE_URL: (derived) JDBC url to your metastore database (note: doesn't require user:pass)

        # Spark
        JAVA_VERSION: the java version used
        SPARK_VERSION: the spark version used
        HADOOP_VERSION: the hadoop version used
        SCALA_VERSION: the scala version used
        DELTA_VERSION: the delta version used

        SPARK_MASTER_HOST: the hostname for the spark master server
        SPARK_MASTER_PORT: the port for the spark master server
        SPARK_MASTER_WEBUI_PORT: the port for the spark webui server
        SPARK_HISTORY_SERVER_PORT: the port for the spark history server
        SPARK_SQL_WAREHOUSE_DIR: local location of warehouse directory
        HIVE_METASTORE_DERBY_DB_DIR: local location of hive metastore db directory
        SPARK_SCHEDULER_MODE: the spark scheduler mode

        # Minio
        MINIO_HOST: the hostname for the minio server
        MINIO_PORT: the port for the minio server
        MINIO_CONSOLE_PORT: the port for the minio console
        MINIO_ROOT_USER: the root user for the minio server
        MINIO_ROOT_PASSWORD: the root password for the minio server
        MINIO_DATA_DIR: the data directory to represent the minio S3
    """

    class Config:
        env_file = ENV_FILE_PATH

    # App
    IS_LOCAL: bool = True
    PROJECT_LOG_DIR: str = str(_SRC_ROOT_DIR / "logs")
    TRACE_ENV: str = ""

    # AWS
    AWS_ACCESS_KEY: SecretStr = SecretStr("")
    AWS_SECRET_KEY: SecretStr = SecretStr("")
    AWS_PROFILE: str = ""
    AWS_REGION: str = ""

    @property
    def AWS_S3_ENDPOINT(self):
        return f"s3.{self.AWS_REGION}.amazonaws.com" if not self.IS_LOCAL else f"{self.MINIO_HOST}:{self.MINIO_PORT}"

    @property
    def AWS_STS_ENDPOINT(self):
        return f"sts.{self.AWS_REGION}.amazonaws.com" if not self.IS_LOCAL else f"{self.MINIO_HOST}:{self.MINIO_PORT}"

    # Buckets
    DATA_SOURCES_BUCKET: str = ""
    PUBLIC_FILES_BUCKET: str = ""
    BROKER_S3_BUCKET = ""
    REFERENCE_S3_BUCKET = ""
    USAS_S3_BUCKET = ""
    METRICS_BUCKET: str = ""

    # Postgres
    DB1_URL: SecretStr = SecretStr("")
    DB2_URL: SecretStr = SecretStr("")

    @property
    def JDBC_DB1_URL(self):
        return get_jdbc_url_from_pg_uri(self.DB1_URL.get_secret_value()) if self.DB1_URL else ""

    @property
    def JDBC_DB2_URL(self):
        return get_jdbc_url_from_pg_uri(self.DB2_URL.get_secret_value()) if self.DB2_URL else ""

    # Metastore
    METASTORE_URL: SecretStr = SecretStr("")

    @property
    def JDBC_METASTORE_URL(self):
        return get_jdbc_url_from_pg_uri(self.METASTORE_URL.get_secret_value()) if self.METASTORE_URL else ""

    # Spark
    JAVA_VERSION: str = ""
    SPARK_VERSION: str = ""
    HADOOP_VERSION: str = ""
    SCALA_VERSION: str = ""
    DELTA_VERSION: str = ""

    SPARK_MASTER_HOST: str = "spark-master"
    SPARK_MASTER_PORT: int = 7077
    SPARK_MASTER_WEBUI_PORT: int = 4040
    SPARK_HISTORY_SERVER_PORT: int = 18080
    SPARK_SQL_WAREHOUSE_DIR: str = os.path.join(_PROJECT_ROOT_DIR, "spark-warehouse")
    HIVE_METASTORE_DERBY_DB_DIR: str = os.path.join(SPARK_SQL_WAREHOUSE_DIR, "metastore_db")
    # SPARK_SCHEDULER_MODE = "FAIR"  # if used with weighted pools, could allow round-robin tasking of simultaneous jobs
    # TODO: have to deal with this if really wanting balanced (FAIR) task execution
    # WARN FairSchedulableBuilder: Fair Scheduler configuration file not found so jobs will be scheduled in FIFO
    # order. To use fair scheduling, configure pools in fairscheduler.xml or set spark.scheduler.allocation.file to a
    # file that contains the configuration.
    # 01:58:26 INFO FairSchedulableBuilder: Created default pool: default, schedulingMode: FIFO, minShare: 0, weight: 1
    SPARK_SCHEDULER_MODE: str = "FIFO"  # the default Spark scheduler mode

    # Minio
    MINIO_HOST: str = "minio"
    MINIO_PORT: int = 10001
    MINIO_CONSOLE_PORT: int = 10002
    MINIO_ROOT_USER: SecretStr = SecretStr("minio_user")
    MINIO_ROOT_PASSWORD: SecretStr = SecretStr("minio_secret")
    # Should point to a path where data can be persisted beyond docker restarts, outside of the git source repository
    # The specified directory needs to exist before Docker can mount it
    MINIO_DATA_DIR: str = ""


CONFIG = DefaultConfig()


def set_brus_config(config):
    """Takes in a config dict of the attributes to override"""
    for attr, value in config.items():
        setattr(CONFIG, attr, value)
