import os
from pyspark.sql import SparkSession

from brus_backend_common.config import _SRC_ROOT_DIR
from brus_backend_common.helpers.aws import _get_boto3
from brus_backend_common.scripts.create_migrate_lakehouse_model import main
from brus_backend_common.models import LAKEHOUSE_MODELS


# CSV
def test_csv_initialize(spark: SparkSession, setup_teardown_buckets):  # hive_unittest_metastore_db
    # doesn't *need* spark but calling the script uses spark, so import that as well

    # Using DEFC as it is a relatively small easy example
    csv_model_name = "raw.defc"
    csv_model = LAKEHOUSE_MODELS[csv_model_name]()

    # Create an empty CSV model in local S3
    s3_client = _get_boto3("client", "s3")
    csv_file_path = os.path.join(_SRC_ROOT_DIR, "tests", "integration", "data", "DEFC_LIST_FOR_USAS.csv")
    s3_client.upload_file(csv_file_path, csv_model.BUCKET_NAME, csv_model.RELATIVE_CSV_PATH)

    # Setup the model
    main(csv_model_name)

    assert csv_model.exists()
    df = csv_model.to_pandas_df()
    assert df is not None and not df.empty


# def test_csv_migrate(setup_teardown_buckets):
#     pass


# Delta
