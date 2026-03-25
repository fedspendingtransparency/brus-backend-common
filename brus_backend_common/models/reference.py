from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from brus_backend_common.config import CONFIG
from brus_backend_common.models.lakehouse_model import DeltaModel, CSVModel, LakeHouseDatabase


class DEFCRaw(CSVModel):
    BUCKET_NAME = CONFIG.REFERENCE_S3_BUCKET
    DATABASE_NAME = LakeHouseDatabase.RAW
    TABLE_NAME = "defc"
    DESCRIPTION = "Raw DEFC CSV placed in S3"
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


class DEFCGroup(CSVModel):
    BUCKET_NAME = CONFIG.REFERENCE_S3_BUCKET
    DATABASE_NAME = LakeHouseDatabase.INT
    TABLE_NAME = "defc_mapping"
    DESCRIPTION = "Internal CSV to dynamically group DEFCs together"
    CSV_NAME = "DEFC_MAPPING.csv"
    PK = "DEFC_CODE"
    UNIQUE_CONSTRAINTS = []
    MIGRATION_HISTORY = []

    STRUCTURE = StructType(
        [
            StructField("code", StringType(), False),
            StructField("group", StringType(), False),
        ]
    )


class DEFCInt(DeltaModel):
    BUCKET_NAME = CONFIG.REFERENCE_S3_BUCKET
    DATABASE_NAME = LakeHouseDatabase.INT
    TABLE_NAME = "defc"
    DESCRIPTION = "DEFC data after initial processing"
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
