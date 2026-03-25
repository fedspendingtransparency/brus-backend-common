import logging
import os
import sys

# mypy doesn't see this internal function
from logging import _checkLevel  # type: ignore

# py4j doesn't have a stub packages, ignoring
from py4j.java_gateway import (  # type: ignore
    JavaGateway,
)
from py4j.protocol import Py4JJavaError  # type: ignore
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.find_spark_home import _find_spark_home
from pyspark.java_gateway import launch_gateway
from pyspark.serializers import read_int, UTF8Deserializer
from pyspark.sql import SparkSession

from brus_backend_common.config import CONFIG
from brus_backend_common.helpers.aws import get_aws_credentials

logger = logging.getLogger(__name__)


def get_active_spark_session() -> SparkSession | None:
    """Returns the active ``SparkSession`` if there is one and it's not stopped, otherwise returns None"""
    if is_spark_context_stopped():
        return None
    return SparkSession.getActiveSession()


def is_spark_context_stopped() -> bool:
    is_stopped = True
    with SparkContext._lock:
        # Check the Singleton instance populated if there's an active SparkContext
        if SparkContext._active_spark_context is not None:
            sc = SparkContext._active_spark_context
            is_stopped = not (sc._jvm and not sc._jvm.SparkSession.getDefaultSession().get().sparkContext().isStopped())
    return is_stopped


def stop_spark_context() -> bool:
    stopped_without_error = True
    with SparkContext._lock:
        # Check the Singleton instance populated if there's an active SparkContext
        if SparkContext._active_spark_context is not None:
            sc = SparkContext._active_spark_context
            if (
                sc._jvm
                and hasattr(sc._jvm, "SparkSession")
                and sc._jvm.SparkSession
                and not sc._jvm.SparkSession.getDefaultSession().get().sparkContext().isStopped()
            ):
                try:
                    sc.stop()
                except Exception:
                    # Swallow errors if not able to stop (e.g. may have already been stopped)
                    stopped_without_error = False
    return stopped_without_error


class SparkScriptSession:
    """To prevent duplicate code across all the spark scripts, use this which will keep track of your spark session
    regardless if it's new or already existing

    Usage:
        extra_config = {'extra.config.value.for.specific.script': 'true'}
        with SparkScriptSession(**extra_config) as spark:
            # script_main_function(spark, ...)
    """

    def __init__(self, **extra_conf):
        self.extra_conf = {
            # Config for Delta Lake tables and SQL. Need these to keep Dela table metadata in the metastore
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            # See comment below about old date and time values cannot parsed without these
            "spark.sql.parquet.datetimeRebaseModeInWrite": "LEGACY",  # for dates at/before 1900
            "spark.sql.parquet.int96RebaseModeInWrite": "LEGACY",  # for timestamps at/before 1900
            "spark.sql.jsonGenerator.ignoreNullFields": "false",  # keep nulls in our json
        }
        if extra_conf:
            self.extra_conf.update(extra_conf)
        self.spark = None
        self.spark_created_by_script = False

    def __enter__(self):
        self.spark = get_active_spark_session()
        if not self.spark:
            self.spark_created_by_script = True
            self.spark = configure_spark_session(spark_context=self.spark, **self.extra_conf)
        return self.spark

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.spark_created_by_script:
            self.spark.stop()


def configure_spark_session(
    java_gateway: JavaGateway | None = None,
    spark_context: SparkContext | None = None,
    master: str | None = None,
    app_name: str | None = "Spark App",
    log_level: int | None = None,
    log_spark_config_vals: bool = False,
    log_hadoop_config_vals: bool = False,
    enable_hive_support: bool = False,
    **options: str,
) -> SparkSession:
    """Get a SparkSession object with some of the default/boiler-plate config needed for THIS project pre-set

    Providing no arguments will work, and give a plain-vanilla SparkSession wrapping a plain vanilla SparkContext
    with all the default spark configurations set (or set with any file-based configs that have been established in
    the runtime environment (e.g. $SPARK_HOME/spark-defaults.conf)

    Use arguments in varying combinations to override or provide pre-configured components of the SparkSession.
    Lastly, provide a dictionary or exploded dict (like: **my_options) of name-value pairs of spark properties with
    specific values.

    Args:
        java_gateway (JavaGateway): Provide your own JavaGateway, which is typically a network interface to a running
            spark-submit process, through which PySpark jobs can be submitted to a JVM-based Spark runtime.
            NOTE: Only JavaGateway and not ClientServer (which would be used for support of PYSPARK_PIN_THREAD) is
            supported at this time.

        spark_context (SparkContext): Provide your own pre-built or fetched-from-elsewhere SparkContext object that
            the built SparkSession will wrap. The given SparkContext must be active (not stopped). Since an active
            SparkContext will have its own active underlying JVM gateway, you cannot provide this AND a java_gateway.

        master (str): URL in the form of spark://host:port where the master node of the Spark cluster can be found.
            If not provided here, or via a conf property spark.master, the default value of local[*] will remain.

        app_name (str): The name given to the app running in this SparkSession. This is not a modifiable property,
            and can only be set if creating a brand new SparkContext and SparkSession.

        log_level (int): Set the log level. Only set AFTER construction of the SparkContext, unfortunately.
            Values are one of: logging.ERROR, logging.WARN, logging.WARNING, logging.INFO, logging.DEBUG

        log_spark_config_vals (bool): If True, log at INFO the current spark config property values

        log_hadoop_config_vals (bool): If True, log at INFO the current hadoop config property values

        enable_hive_support (bool): If True, enable hive on the created SparkSession. Doing so internally sets the
            spark conf spark.sql.catalogImplementation=hive, which persists the metastore_db to its configured location
            (by default the working dir of the spark command run as a Derby DB folder, but configured explicitly here
            if running locally (not AWS))

        options (kwargs): dict or named-arguments (unlikely due to dots in properties) of key-value pairs representing
            additional spark config values to set as the SparkContext and SparkSession are created.
            NOTE: If a value is provided, and a SparkContext is also provided, the value must be a modifiable
            property, otherwise an error will be thrown.
    """
    if spark_context and (
        not spark_context._jvm or spark_context._jvm.SparkSession.getDefaultSession().get().sparkContext().isStopped()
    ):
        raise ValueError("The provided spark_context arg is a stopped SparkContext. It must be active.")
    if spark_context and java_gateway:
        raise Exception(
            "Cannot provide BOTH spark_context and java_gateway args. The active spark_context supplies its own gateway"
        )

    conf = SparkConf()

    # Normalize all timestamps read into the SparkSession to UTC time.
    # So if timezone-aware timestamps are read-in, Spark will shifted to UTC and then strip off the timezone so they
    # are only an "instant" (no timezone part)
    # - See also: https://docs.databricks.com/spark/latest/dataframes-datasets/dates-timestamps.html#timestamps-and
    #   time-zones
    # - if not set, it will fallback to the timezone of the JVM running spark, which could be the local time zone of
    #   the machine
    #   - Still would be ok so long as reads and writes of an "instant" happen from the same session timezone
    conf.set("spark.sql.session.timeZone", "UTC")
    conf.set("spark.scheduler.mode", CONFIG.SPARK_SCHEDULER_MODE)
    # Don't try to re-run the whole job if there's an error
    # Assume that random errors are rare, and jobs have long runtimes, so fail fast, fix and retry manually.
    conf.set("spark.yarn.maxAppAttempts", "1")
    conf.set("spark.hadoop.fs.s3a.endpoint", CONFIG.AWS_S3_ENDPOINT)

    if CONFIG.IS_LOCAL:  # i.e. running in a "local" [development] environment
        # Set configs to allow the S3AFileSystem to work against a local MinIO object storage proxy
        conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # "Enable S3 path style access ie disabling the default virtual hosting behaviour.
        # Useful for S3A-compliant storage providers as it removes the need to set up DNS for virtual hosting."
        conf.set("spark.hadoop.fs.s3a.path.style.access", "true")

        # Documenting for Awareness:
        # Originally it was thought that the S3AFileSystem "Committer" needed config changes to be compliant when
        # hitting MinIO locally instead of AWS S3 service. However, those changs were proven unnecessary.
        # - There is however an intermitten issue which we cannot quite identify the root cause (perhaps changing
        #   back to defaults will keep it from happening again). See: https://github.com/minio/minio/issues/10744
        #   - The error comes back as "FileAlreadyExists" ... or just: "<file/folder> already exists"
        #   - It is either some cache purging of Docker or MinIO or both over time that fixes it
        #   - Or some fiddling with these committer settings
        # - For Committer Details: https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/committers.html
        # - Findings:
        #   - If USE_AWS = True, and you point it at an AWS bucket...
        #     - s3a.path.style.access=true works, by itself as well as with no committer specified (falls back to
        #      FileOutputCommitter) and any combo of conflict-mode and tmp.path
        #     - However if committer.name="directory" (it uses the StagingOutputCommitter) AND conflict-mode=replace,
        #       it will replace the whole directory at the last file write, which is the _SUCCESS file, and that's why
        #       that's the only file you see
        # - The below settings are the DEFAULT when not set, but documenting here FYI
        # conf.set("spark.hadoop.fs.s3a.committer.name", "file")
        # conf.set("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "fail")
        # conf.set("spark.hadoop.fs.s3a.committer.staging.tmp.path", "tmp/staging")

        # Turn on Hive support to use a Derby filesystem DB as the metastore DB for tracking of schemas and tables
        enable_hive_support = True

        # Add Spark conf to set the Spark SQL Warehouse to an explicit directory,
        # and to make the Hive metastore_db folder get stored under that warehouse dir
        conf.set("spark.sql.warehouse.dir", CONFIG.SPARK_SQL_WAREHOUSE_DIR)
        conf.set(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={CONFIG.HIVE_METASTORE_DERBY_DB_DIR};create=true",
        )

    # If the directories don't already exist, Spark will make placeholder "[name]_$folder$" files
    # Update the hadoop configuration to prevent these lingering directories.
    conf.set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # To prevent these and the SUCCESS files mentioned above
    # conf.set("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    # Set AWS credentials in the Spark config
    # Hint: If connecting to AWS resources when executing program from a local env, and you usually authenticate with
    # an AWS_PROFILE, set each of these config values to empty/None, and ensure your AWS_PROFILE env var is set in
    # the shell when executing this program, and set temporary_creds=True.
    configure_s3_credentials(
        conf,
        CONFIG.AWS_ACCESS_KEY.get_secret_value(),
        CONFIG.AWS_SECRET_KEY.get_secret_value(),
        CONFIG.AWS_PROFILE,
        temporary_creds=False,
    )

    # Set optional config key=value items passed in as args
    # Do this after all required config values are set with their defaults to allow overrides by passed-in values
    [conf.set(str(k), str(v)) for k, v in options.items() if options]

    # NOTE: If further configuration needs to be set later (after SparkSession is built), use the below, where keys are
    # not prefixed with "spark.hadoop.", e.g.:
    # spark.sparkContext._jsc.hadoopConfiguration().set("key", value), e.g.
    # spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY)
    # EXPLORE THE ABOVE WITH CAUTION. It may not stick, and if it does, it's generally dangerous to modify a
    #     JavaSparkContext (_jsc) since all SparkSessions share that single context and its config

    # Build the SparkSession based on args provided
    if spark_context:
        builder = SparkSession(sparkContext=spark_context).builder
    elif java_gateway:
        sc_with_gateway = SparkContext(gateway=java_gateway)
        builder = SparkSession(sparkContext=sc_with_gateway).builder
    else:
        builder = SparkSession.builder
    if master:
        builder = builder.master(master)
    if app_name:
        builder = builder.appName(app_name)
    if enable_hive_support:
        builder = builder.enableHiveSupport()
    spark = builder.config(conf=conf).getOrCreate()

    # Now that the SparkSession was created, check whether certain provided config values were ignored if given a
    # pre-existing SparkContext, and error-out if so
    if spark_context:
        built_conf = spark.conf
        provided_conf_keys = [item[0] for item in conf.getAll()]
        non_modifiable_conf = [k for k in provided_conf_keys if not built_conf.isModifiable(k)]
        if non_modifiable_conf:
            raise ValueError(
                "An active SparkContext was given along with NEW spark config values. The following "
                "spark config values were not set because they are not modifiable on the active "
                "SparkContext"
            )

    # Override log level, if provided
    # While this is a bit late (missing out on any logging at SparkSession instantiation time),
    # could not find a way (aside from injecting a ${SPARK_HOME}/conf/log4.properties file) to have it pick up
    # the desired log level at Spark startup time
    if log_level:
        _checkLevel(log_level)  # throws error if not recognized
        log_level_name = logging.getLevelName(log_level)
        if log_level_name == "WARNING":
            log_level_name = "WARN"  # tranlate to short-form used by log4j
        spark.sparkContext.setLogLevel(log_level_name)

    logger.info("PySpark Job started!")
    hadoop_version = (
        spark.sparkContext._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion()
        if spark.sparkContext._gateway
        else None
    )
    logger.info(
        f"""
@       Found SPARK_HOME: {_find_spark_home()}
@       Python Version: {sys.version}
@       Spark Version: {spark.version}
@       Hadoop Version: {hadoop_version}
    """
    )
    logger.info(
        f"Running Job with:\n"
        f"\tDB = {CONFIG.JDBC_DB1_URL.rsplit('=', 1)[0] + '=********'}"
        f"\n\tS3 = {conf.get('spark.hadoop.fs.s3a.endpoint')} with "
        f"spark.hadoop.fs.s3a.access.key='{conf.get('spark.hadoop.fs.s3a.access.key')}' and "
        f"spark.hadoop.fs.s3a.secret.key='{'********' if conf.get('spark.hadoop.fs.s3a.secret.key') else ''}'"
    )

    if log_spark_config_vals:
        log_spark_config(spark)
    if log_hadoop_config_vals:
        log_hadoop_config(spark)
    return spark


def read_java_gateway_connection_info(
    gateway_conn_info_path: os.PathLike,
) -> tuple[int, str]:  # pragma: no cover -- useful development util
    """Read the port and auth token from a file holding connection info to a running spark-submit process

    Args:
        gateway_conn_info_path (path-like): File path of a file that the spun-up spark-submit process would have
            written its port and secret info to. In order to do so this file path would have needed to be provided in an
            environment variable named _PYSPARK_DRIVER_CONN_INFO_PATH in the environment where the spark-submit
            process was started. It will read that, and dump out its connection info to that file upon starting.
    """
    with open(gateway_conn_info_path, "rb") as conn_info:
        gateway_port = read_int(conn_info)
        gateway_secret = UTF8Deserializer().loads(conn_info)
    return gateway_port, gateway_secret


def attach_java_gateway(
    gateway_port: int,
    gateway_auth_token: str,
) -> JavaGateway:  # pragma: no cover -- useful development util
    """Create a new JavaGateway that latches onto the port of a running spark-submit process

    Args:
        gateway_port (int): Port on which the spark-submit process will allow the gateway to attach
        gateway_auth_token: Shared secret that must be provided to attach to the spark-submit process

    Returns: The instantiated JavaGateway, which acts as a network interface for PySpark to submit spark jobs through
        to the JVM-based Spark runtime
    """
    os.environ["PYSPARK_GATEWAY_PORT"] = str(gateway_port)
    os.environ["PYSPARK_GATEWAY_SECRET"] = gateway_auth_token

    gateway = launch_gateway()

    return gateway


def get_jdbc_connection_properties(fix_strings: bool = True) -> dict:
    SPARK_PARTITION_ROWS = 10000
    jdbc_props = {
        "driver": "org.postgresql.Driver",
        "fetchsize": str(SPARK_PARTITION_ROWS),
    }
    if fix_strings:
        # This setting basically tells the JDBC driver how to process the strings, which could be a special type casted
        # as a string (ex. UUID, JSONB). By default, it assumes they are actually strings. Setting this to "unspecified"
        # tells the driver to not make that assumption and let the schema try to infer the type on insertion.
        # See the `stringtype` param on https://jdbc.postgresql.org/documentation/94/connect.html for details.
        jdbc_props["stringtype"] = "unspecified"
    return jdbc_props


def log_java_exception(logger: logging.Logger, exc: Exception, err_msg: str = "") -> None:
    if exc and (isinstance(exc, Py4JJavaError) or hasattr(exc, "java_exception")):
        logger.error(f"{err_msg}\n{str(exc.java_exception)}")
    elif exc and hasattr(exc, "printStackTrace"):
        logger.error(f"{err_msg}\n{str(exc.printStackTrace)}")
    else:
        try:
            logger.error(err_msg, exc)
        except Exception:
            logger.error(f"{err_msg}\n{str(exc)}")


def configure_s3_credentials(
    conf: SparkConf,
    access_key: str | None = None,
    secret_key: str | None = None,
    profile: str | None = None,
    temporary_creds: bool = False,
) -> None:
    """Set Spark config values allowing authentication to S3 for bucket data

    See Also:
        Details on authenticating to AWS for s3a access:
          - https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Authenticating_with_S3
          -                                ^---- change docs to version of hadoop being used

    Args:
        conf: Spark configuration object
        access_key: AWS Access Key ID
        secret_key: AWS Secret Access Key
        profile: AWS profile, from which to derive access key and secret key
        temporary_creds: When set to True, use ``org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider``
          - This provider issues short-lived credentials that are routinely refreshed on the client system. Typically
            the client uses an AWS_PROFILE, under which the credentials are refreshed. When authenticating with these
            credentials, the access_key, secret_key, and token must be provided. Additionally the endpoint to a Security
            Token Service that can validate that the given temporary credentials were in fact issued must be configured.
    """
    if access_key and secret_key and not profile and not temporary_creds:
        # Short-circuit the need for boto3 if the caller gave the creds directly as access/secret keys
        conf.set("spark.hadoop.fs.s3a.access.key", access_key)
        conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)
        return

    # Use boto3 Session to derive creds
    aws_creds = get_aws_credentials(access_key, secret_key, profile)
    if not aws_creds:
        logger.warning("No AWS credentials found. Not updating spark config.")
        return
    conf.set("spark.hadoop.fs.s3a.access.key", aws_creds.access_key)
    conf.set("spark.hadoop.fs.s3a.secret.key", aws_creds.secret_key)
    if temporary_creds:
        conf.set(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )
        conf.set("spark.hadoop.fs.s3a.session.token", str(aws_creds.token))
        conf.set("spark.hadoop.fs.s3a.assumed.role.sts.endpoint", CONFIG.AWS_STS_ENDPOINT)
        conf.set("spark.hadoop.fs.s3a.assumed.role.sts.endpoint.region", CONFIG.AWS_REGION)


def log_spark_config(spark: SparkSession, config_key_contains: str = "") -> None:
    """Log at log4j INFO the values of the SparkConf object in the current SparkSession"""
    for item in spark.sparkContext.getConf().getAll():
        if config_key_contains in item[0]:
            logger.info(f"{item[0]}={item[1]}")


def log_hadoop_config(spark: SparkSession, config_key_contains: str = "") -> None:
    """Print out to the log the current config values for hadoop. Limit to only those whose key contains the string
    provided to narrow in on a particular subset of config values.
    """
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    for config_value in conf.iterator():
        for k, v in {str(config_value).split("=")[0]: str(config_value).split("=")[1]}.items():
            if config_key_contains in k:
                logger.info(f"{k}={v}")
