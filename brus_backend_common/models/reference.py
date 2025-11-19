from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    # DateType,
    # DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from brus_backend_common.models.delta_model import DeltaModel

REFERENCE_S3_BUCKET = "dti-delta-reference-nonprod"  # TODO: edit for prod/nonprod


class DEFCDeltaRaw(DeltaModel):
    s3_bucket = REFERENCE_S3_BUCKET
    database = "raw"
    table_name = "defc"
    format = "csv"
    pk = "DEFC_CODE"
    unique_constraints = []
    migration_history = []

    @property
    def structure(self):
        return StructType(
            [
                StructField("DEFC_CODE", StringType(), False),
                StructField("DEFC_TITLE", StringType(), False),
            ]
        )


class DEFCDeltaInt(DeltaModel):
    s3_bucket = REFERENCE_S3_BUCKET
    database = "int"
    table_name = "defc"
    pk = "defc_id"
    unique_constraints = ["code"]
    migration_history = ["add_test_column", "drop_test_column"]

    @property
    def structure(self):
        return StructType(
            [
                StructField("created_at", TimestampType(), True),
                StructField("updated_at", TimestampType(), True),
                StructField("defc_id", IntegerType(), False),
                StructField("code", StringType(), False),
                StructField("public_laws", ArrayType(StringType(), True), True),
                StructField("public_law_short_titles", ArrayType(StringType(), True), True),
                StructField("group", StringType(), True),
                StructField("urls", ArrayType(StringType(), True), True),
                StructField("is_valid", BooleanType(), False),
                StructField("earliest_pl_action_date", TimestampType(), True),
            ]
        )


class ExternalDataLoadDateDelta(DeltaModel):
    s3_bucket = REFERENCE_S3_BUCKET
    database = "int"
    table_name = "external_data_load_date"
    format = "csv"
    pk = "external_data_load_date_id"
    unique_constraints = ["name"]
    migration_history = []

    @property
    def structure(self):
        return StructType(
            [
                StructField("created_at", TimestampType(), True),
                StructField("updated_at", TimestampType(), True),
                StructField("external_data_load_date_id", IntegerType(), False),
                StructField("name", StringType(), False),
                StructField("description", StringType(), False),
                StructField("last_load_date_start", TimestampType(), False),
                StructField("last_load_date_end", TimestampType(), False),
            ]
        )
