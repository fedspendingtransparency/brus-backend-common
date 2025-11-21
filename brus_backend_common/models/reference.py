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
from brus_backend_common.models.delta_model import DeltaModel, CSVModel

REFERENCE_S3_BUCKET = "dti-delta-reference-nonprod"  # TODO: edit for prod/nonprod


class DEFCDeltaRaw(CSVModel):
    S3_BUCKET = REFERENCE_S3_BUCKET
    DATABASE = "raw"
    TABLE_NAME = "defc"
    CSV_NAME = "DEFC_LIST_FOR_USAS.csv"
    PK = "DEFC_CODE"
    UNIQUE_CONSTRAINTS = []
    MIGRATION_HISTORY = []

    STRUCTURE = StructType(
        [
            StructField("DEFC_CODE", StringType(), False),
            StructField("DEFC_TITLE", StringType(), False),
        ]
    )


class DEFCDeltaInt(DeltaModel):
    S3_BUCKET = REFERENCE_S3_BUCKET
    DATABASE = "int"
    TABLE_NAME = "defc"
    PK = "defc_id"
    UNIQUE_CONSTRAINTS = ["code"]
    MIGRATION_HISTORY = []

    STRUCTURE = StructType(
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
    S3_BUCKET = REFERENCE_S3_BUCKET
    DATABASE = "int"
    TABLE_NAME = "external_data_load_date"
    FORMAT = "csv"
    PK = "external_data_load_date_id"
    UNIQUE_CONSTRAINTS = ["name"]
    MIGRATION_HISTORY = []

    STRUCTURE = StructType(
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
