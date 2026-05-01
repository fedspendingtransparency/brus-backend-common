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
from brus_backend_common.models.lakehouse_model import CSVModel, LakeHouseDatabase


class DEFCBronze(CSVModel):
    BUCKET_NAME = CONFIG.LAKEHOUSE_REFERENCE_BUCKET
    DATABASE_NAME = LakeHouseDatabase.BRONZE
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
    BUCKET_NAME = CONFIG.LAKEHOUSE_REFERENCE_BUCKET
    DATABASE_NAME = LakeHouseDatabase.GOLD
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


class DEFCGold(CSVModel):
    BUCKET_NAME = CONFIG.LAKEHOUSE_REFERENCE_BUCKET
    DATABASE_NAME = LakeHouseDatabase.GOLD
    TABLE_NAME = "defc"
    DESCRIPTION = "DEFC data after initial processing"
    CSV_NAME = "def_codes.csv"
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


class FONBronze(CSVModel):
    BUCKET_NAME = CONFIG.REFERENCE_S3_BUCKET
    DATABASE_NAME = LakeHouseDatabase.BRONZE
    TABLE_NAME = "funding_opportunity"
    DESCRIPTION = "Raw FON data pulled from Grants.gov"
    CSV_NAME = "funding_opportunity.csv"
    PK = "id"
    UNIQUE_CONSTRAINTS = [""]
    MIGRATION_HISTORY = []

    STRUCTURE = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("number", StringType(), False),
            StructField("title", StringType(), False),
            StructField("agencyCode", StringType(), True),
            StructField("agency", StringType(), True),
            StructField("openDate", TimestampType(), True),
            StructField("closeDate", TimestampType(), True),
            StructField("oppStatus", StringType(), True),
            StructField("docType", StringType(), True),
            StructField("cfdaList", ArrayType(StringType(), True), True),
        ]
    )


class FONGold(CSVModel):
    BUCKET_NAME = CONFIG.REFERENCE_S3_BUCKET
    DATABASE_NAME = LakeHouseDatabase.GOLD
    TABLE_NAME = "funding_opportunity"
    DESCRIPTION = "Processed FON data from FONBronze"
    CSV_NAME = "funding_opportunity.csv"
    PK = "funding_opportunity_id"
    UNIQUE_CONSTRAINTS = []
    MIGRATION_HISTORY = []

    STRUCTURE = StructType(
        [
            StructField("funding_opportunity_id", IntegerType(), False),
            StructField("funding_opportunity_number", StringType(), False),
            StructField("title", StringType(), True),
            StructField("assistance_listing_numbers", ArrayType(StringType(), True), True),
            StructField("agency_name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("open_date", TimestampType(), True),
            StructField("close_date", TimestampType(), True),
            StructField("doc_type", StringType(), True),
            StructField("internal_id", IntegerType(), True),
        ]
    )
