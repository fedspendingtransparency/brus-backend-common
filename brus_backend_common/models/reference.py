from datetime import datetime

import pandas as pd

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


class DEFCBronze(CSVModel):
    BUCKET_NAME = CONFIG.REFERENCE_S3_BUCKET
    DATABASE_NAME = LakeHouseDatabase.BRONZE
    TABLE_NAME = "defc"
    DESCRIPTION = "Raw DEFC CSV placed in S3"
    CSV_NAME = "DEFC_LIST_FOR_USAS.csv"
    PK = "DEFC_CODE"
    UNIQUE_CONSTRAINTS = []
    MIGRATION_HISTORY = []

    DTYPES = {
        "DEFC_CODE": pd.StringDtype(),
        "DEFC_TITLE": pd.StringDtype(),
    }


class DEFCGroup(CSVModel):
    BUCKET_NAME = CONFIG.REFERENCE_S3_BUCKET
    DATABASE_NAME = LakeHouseDatabase.SILVER
    TABLE_NAME = "defc_mapping"
    DESCRIPTION = "Internal CSV to dynamically group DEFCs together"
    CSV_NAME = "DEFC_MAPPING.csv"
    PK = "DEFC_CODE"
    UNIQUE_CONSTRAINTS = []
    MIGRATION_HISTORY = []

    DTYPES = {
        "code": pd.StringDtype(),
        "group": pd.StringDtype(),
    }


class DEFCSilver(DeltaModel):
    BUCKET_NAME = CONFIG.REFERENCE_S3_BUCKET
    DATABASE_NAME = LakeHouseDatabase.SILVER
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


class ProgramActivityParkBronze(CSVModel):
    BUCKET_NAME = CONFIG.REFERENCE_S3_BUCKET
    DATABASE_NAME = LakeHouseDatabase.BRONZE
    TABLE_NAME = "program_activity_park"
    DESCRIPTION = "Raw program activity park data"
    CSV_NAME = "PARK_PROGRAM_ACTIVITY.csv"
    PK = "PARK"
    DTYPES = {
        "FY": pd.StringDtype(),
        "PD": pd.StringDtype(),
        "ALLOC_XFER_AGENCY": pd.StringDtype(),
        "AID": pd.StringDtype(),
        "MAIN_ACCT": pd.StringDtype(),
        "SUB_ACCT": pd.StringDtype(),
        "COMPOUND_KEY": pd.StringDtype(),
        "PARK": pd.StringDtype(),
        "PARK_NAME": pd.StringDtype(),
        "RECORD_UPDATE_TS": pd.StringDtype(),
        "FILE_UPDATE_TS": pd.StringDtype(),
    }


class ProgramActivityParkGold(CSVModel):
    BUCKET_NAME = CONFIG.REFERENCE_S3_BUCKET
    DATABASE_NAME = LakeHouseDatabase.GOLD
    TABLE_NAME = "program_activity_park"
    DESCRIPTION = "Program activity park data after initial processing"
    CSV_NAME = "PROGRAM_ACTIVITY_PARK.csv"
    PK = "park_code"
    DTYPES = {
        "created_at": datetime,
        "updated_at": datetime,
        "fiscal_year": pd.Int64Dtype(),
        "period": pd.Int64Dtype(),
        "allocation_transfer_id": pd.StringDtype(),
        "agency_id": pd.StringDtype(),
        "main_account_number": pd.StringDtype(),
        "sub_account_number": pd.StringDtype(),
        "park_code": pd.StringDtype(),
        "park_name": pd.StringDtype(),
    }
