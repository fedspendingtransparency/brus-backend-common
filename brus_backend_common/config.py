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
"""

import os
import pathlib
from pydantic import BaseSettings

from brus_backend_common.helpers.uri_helper import get_jdbc_url_from_pg_uri

_PROJECT_NAME = "brus-backend-common"
# WARNING: This is relative to THIS file's location. If it is moved/refactored, this needs to be confirmed to point
# to the project root dir (i.e. usaspending-api/)
_PROJECT_ROOT_DIR: pathlib.Path = pathlib.Path(__file__).parent.parent.resolve()
_SRC_ROOT_DIR: pathlib.Path = _PROJECT_ROOT_DIR / _PROJECT_NAME.replace("-", "_")


class DefaultConfig(BaseSettings):
    """Top-level config that defines all configuration variables, and their default, overridable values

    Attributes:
        # App
        IS_LOCAL: Whether it's running locally or remotely

        # AWS
        AWS_ACCESS_KEY: The current AWS access key
        AWS_SECRET_KEY: The current AWS secret key
        AWS_PROFILE: The current AWS profile
        AWS_REGION: The current AWS region
        AWS_S3_ENDPOINT: (derived) The current AWS S3 endpoint

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
        SPARK_SQL_WAREHOUSE_DIR: local location of warehouse directory
        HIVE_METASTORE_DERBY_DB_DIR: local location of hive metastore db directory
        SPARK_SCHEDULER_MODE: the spark scheduler mode
    """

    # App
    IS_LOCAL: bool = True

    # AWS
    AWS_ACCESS_KEY: str = ""
    AWS_SECRET_KEY: str = ""
    AWS_PROFILE: str = ""
    AWS_REGION: str = "us-gov-west-1"

    @property
    def AWS_S3_ENDPOINT(self):
        return f"s3.{self.AWS_REGION}.amazonaws.com" if self.AWS_REGION else ""

    # Postgres
    DB1_URL: str = ""
    DB2_URL: str = ""

    @property
    def JDBC_DB1_URL(self):
        return get_jdbc_url_from_pg_uri(self.DB1_URL) if self.DB1_URL else ""

    @property
    def JDBC_DB2_URL(self):
        return get_jdbc_url_from_pg_uri(self.DB2_URL) if self.DB2_URL else ""

    # Metastore
    METASTORE_URL: str = ""

    @property
    def JDBC_METASTORE_URL(self):
        return get_jdbc_url_from_pg_uri(self.METASTORE_URL) if self.METASTORE_URL else ""

    # Spark
    SPARK_SQL_WAREHOUSE_DIR: str = os.path.join(_SRC_ROOT_DIR, "helpers", "spark-warehouse")
    HIVE_METASTORE_DERBY_DB_DIR: str = os.path.join(SPARK_SQL_WAREHOUSE_DIR, "metastore_db")
    # SPARK_SCHEDULER_MODE = "FAIR"  # if used with weighted pools, could allow round-robin tasking of simultaneous jobs
    # TODO: have to deal with this if really wanting balanced (FAIR) task execution
    # WARN FairSchedulableBuilder: Fair Scheduler configuration file not found so jobs will be scheduled in FIFO
    # order. To use fair scheduling, configure pools in fairscheduler.xml or set spark.scheduler.allocation.file to a
    # file that contains the configuration.
    # 01:58:26 INFO FairSchedulableBuilder: Created default pool: default, schedulingMode: FIFO, minShare: 0, weight: 1
    SPARK_SCHEDULER_MODE: str = "FIFO"  # the default Spark scheduler mode


CONFIG = DefaultConfig()


def set_brus_config(config):
    """Takes in a config dict of the attributes to override"""
    for attr, value in config.items():
        setattr(CONFIG, attr, value)
