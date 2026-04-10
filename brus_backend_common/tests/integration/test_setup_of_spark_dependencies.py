"""
This module should contain only the one test.
It is to be called by name in a pytest session that is used to prime or prepare a second pytest session where the
bulk of tests are performed. The intention of this test is to trigger the loading of Spark JAR dependencies to the
Ivy dependency management cache directory, so subsequent tests do not need to re-download them. This works around a
collision that happens using pytest-xdist where multiple pytest sessions from multiple workers try to download
dependencies simultaneously and fail because of race conditions.

NOTE: It should be run in a single-process pytest session (no xdist parallel sessions)!
"""

from pyspark.sql import SparkSession


def test_preload_spark_jars(spark: SparkSession):
    """See module docstring above.

    Run this test alone, like:

        pytest --no-cov --disable-warnings -r=fEs -vv 'brus_backend_common/tests/integration/test_setup_of_spark_dependencies.py::test_preload_spark_jars'
    """
    # The prep work here is done by the spark fixture loading dependencies
    assert spark.sparkContext._jsc, "Could not get a reference to the Java SparkContext from PySpark SparkSession"
    assert spark.sparkContext._jsc.sc().listJars().size() > 0
