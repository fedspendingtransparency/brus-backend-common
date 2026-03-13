from brus_backend_common.config import CONFIG

# List of Maven coordinates for required JAR files used by running code, which can be added to the driver and
# executor class paths
SPARK_SESSION_JARS = [
    # "com.amazonaws:aws-java-sdk:1.12.31",
    # hadoop-aws is an add-on to hadoop with Classes that allow hadoop to interface with an S3A (AWS S3) FileSystem
    # NOTE That in order to work, the version number should be the same as the Hadoop version used by your Spark runtime
    # It SHOULD pull in (via Ivy package manager from maven repo) the version of com.amazonaws:aws-java-sdk that is
    # COMPATIBLE with it (so that should not  be set as a dependent package by us)
    f"org.apache.hadoop:hadoop-aws:{CONFIG.HADOOP_VERSION}",
    # "org.postgresql:postgresql:42.2.23",
    # f"io.delta:delta-spark_{CONFIG.SCALA_VERSION}:{CONFIG.DELTA_VERSION}",  # TODO: Use for Delta > 2.4.0
    f"io.delta:delta-core_{CONFIG.SCALA_VERSION}:{CONFIG.DELTA_VERSION}",  # Use for Delta <= 2.4.0
    f"org.apache.spark:spark-hive_{CONFIG.SCALA_VERSION}:{CONFIG.SPARK_VERSION}",
]
OPTIONAL_SPARK_HIVE_JAR = f"org.apache.spark:spark-hive_{CONFIG.SCALA_VERSION}:{CONFIG.SPARK_VERSION}"

# TODO: This should be used more widely across our different commands
DEFAULT_EXTRA_CONF = {
    # Config for Delta Lake tables and SQL. Need these to keep Dela table metadata in the metastore
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    # See comment below about old date and time values cannot parse without these
    "spark.sql.parquet.datetimeRebaseModeInWrite": "LEGACY",  # for dates at/before 1900
    "spark.sql.parquet.int96RebaseModeInWrite": "LEGACY",  # for timestamps at/before 1900
    "spark.sql.jsonGenerator.ignoreNullFields": "false",  # keep nulls in our json
}

LOCAL_BASIC_EXTRA_CONF = {
    **DEFAULT_EXTRA_CONF,
    # This is the default, but being explicit
    "spark.master": "local[*]",
    "spark.driver.host": "127.0.0.1",  # if not set fails in local envs, trying to use network IP instead
    # Client deploy mode is the default, but being explicit.
    # Means the driver node is the place where the SparkSession is instantiated (and/or where spark-submit
    # process is started from, even if started under the hood of a Py4J JavaGateway). With a "standalone" (not
    # YARN or Mesos or Kubernetes) cluster manager, only client mode is supported.
    "spark.submit.deployMode": "client",
    # Default of 1g (1GiB) for Driver. Increase here if the Java process is crashing with memory errors
    "spark.driver.memory": "1g",
    "spark.executor.memory": "1g",
    "spark.ui.enabled": "false",  # Does the same as setting SPARK_TESTING=true env var
    "spark.jars.packages": ",".join(SPARK_SESSION_JARS),
}


LOCAL_EXTENDED_EXTRA_CONF = {
    **LOCAL_BASIC_EXTRA_CONF,
    "spark.hadoop.fs.s3a.endpoint": getattr(CONFIG, "AWS_S3_ENDPOINT", ""),
    "spark.hadoop.fs.s3a.connection.ssl.enabled": False,
    "spark.hadoop.fs.s3a.path.style.access": True,
    "spark.sql.catalogImplementation": "hive",
    "spark.sql.warehouse.dir": getattr(CONFIG, "SPARK_SQL_WAREHOUSE_DIR", ""),
}

if getattr(CONFIG, "MINIO_ROOT_USER", False) and getattr(CONFIG.MINIO_ROOT_USER, "get_secret_value", False):
    LOCAL_EXTENDED_EXTRA_CONF["spark.hadoop.fs.s3a.access.key"] = CONFIG.MINIO_ROOT_USER.get_secret_value()

if getattr(CONFIG, "MINIO_ROOT_PASSWORD", False):
    LOCAL_EXTENDED_EXTRA_CONF["spark.hadoop.fs.s3a.secret.key"] = CONFIG.MINIO_ROOT_PASSWORD.get_secret_value()

if getattr(CONFIG, "HIVE_METASTORE_DERBY_DB_DIR", False):
    LOCAL_EXTENDED_EXTRA_CONF["spark.hadoop.javax.jdo.option.ConnectionURL"] = (
        f"jdbc:derby:;databaseName={CONFIG.HIVE_METASTORE_DERBY_DB_DIR};create=true"
    )
