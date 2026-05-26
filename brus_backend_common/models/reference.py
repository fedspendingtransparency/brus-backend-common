from brus_backend_common.config import CONFIG
from brus_backend_common.models.lakehouse_model import BaseSchema, CSVModel, LakeHouseDatabase, SchemaField, SchemaType


class DEFCBronze(CSVModel):
    BUCKET_NAME = CONFIG.LAKEHOUSE_REFERENCE_BUCKET
    DATABASE_NAME = LakeHouseDatabase.BRONZE
    TABLE_NAME = "defc"
    DESCRIPTION = "Raw DEFC CSV placed in S3"
    CSV_NAME = "DEFC_LIST_FOR_USAS.csv"
    PK = "DEFC_CODE"
    UNIQUE_CONSTRAINTS = []
    MIGRATION_HISTORY = []

    STRUCTURE = BaseSchema(
        [
            SchemaField("DEFC_CODE", SchemaType.STRING, False),
            SchemaField("DEFC_TITLE", SchemaType.STRING, False),
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

    STRUCTURE = BaseSchema(
        [
            SchemaField("code", SchemaType.STRING, False),
            SchemaField("group", SchemaType.STRING, False),
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
    STRUCTURE = BaseSchema(
        [
            SchemaField("created_at", SchemaType.TIMESTAMP, True),
            SchemaField("updated_at", SchemaType.TIMESTAMP, True),
            SchemaField("defc_id", SchemaType.INTEGER, False),
            SchemaField("code", SchemaType.STRING, False),
            SchemaField("public_laws", SchemaType.LIST, True, SchemaType.STRING, True),
            SchemaField(
                name="public_law_short_titles",
                type=SchemaType.LIST,
                nullable=True,
                sub_type=SchemaType.STRING,
                sub_nullable=True,
            ),
            SchemaField("group", SchemaType.STRING, True),
            SchemaField("urls", SchemaType.LIST, True, SchemaType.STRING, True),
            SchemaField("is_valid", SchemaType.BOOLEAN, False),
            SchemaField("earliest_pl_action_date", SchemaType.TIMESTAMP, True),
        ]
    )


class FONBronze(CSVModel):
    BUCKET_NAME = CONFIG.LAKEHOUSE_REFERENCE_BUCKET
    DATABASE_NAME = LakeHouseDatabase.BRONZE
    TABLE_NAME = "funding_opportunity"
    DESCRIPTION = "Raw FON data pulled from Grants.gov"
    CSV_NAME = "funding_opportunity.csv"
    PK = "id"
    UNIQUE_CONSTRAINTS = [""]
    MIGRATION_HISTORY = []

    STRUCTURE = BaseSchema(
        [
            SchemaField("id", SchemaType.INTEGER, False),
            SchemaField("number", SchemaType.STRING, False),
            SchemaField("title", SchemaType.STRING, False),
            SchemaField("agencyCode", SchemaType.STRING, True),
            SchemaField("agency", SchemaType.STRING, True),
            SchemaField("openDate", SchemaType.TIMESTAMP, True),
            SchemaField("closeDate", SchemaType.TIMESTAMP, True),
            SchemaField("oppStatus", SchemaType.STRING, True),
            SchemaField("docType", SchemaType.STRING, True),
            SchemaField("cfdaList", SchemaType.LIST, True, SchemaType.STRING, True),
        ]
    )


class FONGold(CSVModel):
    BUCKET_NAME = CONFIG.LAKEHOUSE_REFERENCE_BUCKET
    DATABASE_NAME = LakeHouseDatabase.GOLD
    TABLE_NAME = "funding_opportunity"
    DESCRIPTION = "Processed FON data from FONBronze"
    CSV_NAME = "funding_opportunity.csv"
    PK = "funding_opportunity_id"
    UNIQUE_CONSTRAINTS = []
    MIGRATION_HISTORY = []

    STRUCTURE = BaseSchema(
        [
            SchemaField("created_at", SchemaType.TIMESTAMP, True),
            SchemaField("updated_at", SchemaType.TIMESTAMP, True),
            SchemaField("funding_opportunity_id", SchemaType.INTEGER, False),
            SchemaField("funding_opportunity_number", SchemaType.STRING, False),
            SchemaField("title", SchemaType.STRING, True),
            SchemaField("assistance_listing_numbers", SchemaType.LIST, True, SchemaType.STRING, True),
            SchemaField("agency_name", SchemaType.STRING, True),
            SchemaField("status", SchemaType.STRING, True),
            SchemaField("open_date", SchemaType.TIMESTAMP, True),
            SchemaField("close_date", SchemaType.TIMESTAMP, True),
            SchemaField("doc_type", SchemaType.STRING, True),
            SchemaField("internal_id", SchemaType.INTEGER, True),
        ]
    )


class ProgramActivityParkBronze(CSVModel):
    BUCKET_NAME = CONFIG.LAKEHOUSE_REFERENCE_BUCKET
    DATABASE_NAME = LakeHouseDatabase.BRONZE
    TABLE_NAME = "program_activity_park"
    DESCRIPTION = "Raw program activity park data"
    CSV_NAME = "PARK_PROGRAM_ACTIVITY.csv"
    PK = "PARK"
    STRUCTURE = BaseSchema(
        [
            SchemaField("FY", SchemaType.STRING),
            SchemaField("PD", SchemaType.STRING),
            SchemaField("ALLOC_XFER_AGENCY", SchemaType.STRING),
            SchemaField("AID", SchemaType.STRING),
            SchemaField("MAIN_ACCT", SchemaType.STRING),
            SchemaField("SUB_ACCT", SchemaType.STRING),
            SchemaField("COMPOUND_KEY", SchemaType.STRING),
            SchemaField("PARK", SchemaType.STRING),
            SchemaField("PARK_NAME", SchemaType.STRING),
            SchemaField("RECORD_UPDATE_TS", SchemaType.STRING),
            SchemaField("FILE_UPDATE_TS", SchemaType.STRING),
        ]
    )


class ProgramActivityParkGold(CSVModel):
    BUCKET_NAME = CONFIG.LAKEHOUSE_REFERENCE_BUCKET
    DATABASE_NAME = LakeHouseDatabase.GOLD
    TABLE_NAME = "program_activity_park"
    DESCRIPTION = "Program activity park data after initial processing"
    CSV_NAME = "PROGRAM_ACTIVITY_PARK.csv"
    PK = "park_code"
    STRUCTURE = BaseSchema(
        [
            SchemaField("created_at", SchemaType.TIMESTAMP),
            SchemaField("updated_at", SchemaType.TIMESTAMP),
            SchemaField("fiscal_year", SchemaType.INTEGER),
            SchemaField("period", SchemaType.INTEGER),
            SchemaField("allocation_transfer_id", SchemaType.STRING),
            SchemaField("agency_id", SchemaType.STRING),
            SchemaField("main_account_number", SchemaType.STRING),
            SchemaField("sub_account_number", SchemaType.STRING),
            SchemaField("park_code", SchemaType.STRING),
            SchemaField("park_name", SchemaType.STRING),
        ]
    )
