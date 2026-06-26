from brus_backend_common.config import CONFIG
from brus_backend_common.models.lakehouse_model import BaseSchema, CSVModel, LakeHouseDatabase, SchemaField, SchemaType


class AgencyBronze(CSVModel):
    BUCKET_NAME = CONFIG.LAKEHOUSE_REFERENCE_BUCKET
    DATABASE_NAME = LakeHouseDatabase.BRONZE
    TABLE_NAME = "agency_codes"
    DESCRIPTION = "Raw Agency CSV placed in S3"
    CSV_NAME = "agency_codes.csv"
    UNIQUE_CONSTRAINTS = [("CGAC AGENCY CODE", "FREC", "SUBTIER CODE")]
    MIGRATION_HISTORY = []

    STRUCTURE = BaseSchema(
        [
            SchemaField("CGAC AGENCY CODE", SchemaType.STRING, False),
            SchemaField("AGENCY NAME", SchemaType.STRING, False),
            SchemaField("AGENCY ABBREVIATION", SchemaType.STRING, True),
            SchemaField("FREC", SchemaType.STRING, True),
            SchemaField("FREC Entity Description", SchemaType.STRING, True),
            SchemaField("FREC ABBREVIATION", SchemaType.STRING, True),
            SchemaField("SUBTIER CODE", SchemaType.STRING, True),
            SchemaField("SUBTIER NAME", SchemaType.STRING, True),
            SchemaField("SUBTIER ABBREVIATION", SchemaType.STRING, True),
            SchemaField("Admin Org Name", SchemaType.STRING, True),
            SchemaField("ADMIN_ORG", SchemaType.STRING, True),
            SchemaField("TOPTIER_FLAG", SchemaType.BOOLEAN, False),
            SchemaField("IS_FREC", SchemaType.BOOLEAN, False),
            SchemaField("FREC CGAC ASSOCIATION", SchemaType.BOOLEAN, False),
            SchemaField("USER SELECTABLE ON USASPENDING.GOV", SchemaType.BOOLEAN, False),
            SchemaField("MISSION", SchemaType.STRING, True),
            SchemaField("ABOUT AGENCY DATA", SchemaType.STRING, True),
            SchemaField("WEBSITE", SchemaType.STRING, True),
            SchemaField("CONGRESSIONAL JUSTIFICATION", SchemaType.STRING, True),
            SchemaField("ICON FILENAME", SchemaType.STRING, True),
            SchemaField("COMMENT", SchemaType.STRING, True),
        ]
    )


class CGACGold(CSVModel):
    BUCKET_NAME = CONFIG.LAKEHOUSE_REFERENCE_BUCKET
    DATABASE_NAME = LakeHouseDatabase.GOLD
    TABLE_NAME = "cgac"
    DESCRIPTION = "CGAC Data after processing"
    CSV_NAME = "cgac.csv"
    PK = "cgac_id"
    UNIQUE_CONSTRAINTS = ["cgac_code"]
    MIGRATION_HISTORY = []

    STRUCTURE = BaseSchema(
        [
            SchemaField("cgac_id", SchemaType.INTEGER, False),
            SchemaField("cgac_code", SchemaType.STRING, False),
            SchemaField("agency_name", SchemaType.STRING, False),
            SchemaField("agency_abbreviation", SchemaType.STRING, True),
            SchemaField("display_name", SchemaType.STRING, False),
            SchemaField("icon_name", SchemaType.STRING, True),
        ]
    )


class FRECGold(CSVModel):
    BUCKET_NAME = CONFIG.LAKEHOUSE_REFERENCE_BUCKET
    DATABASE_NAME = LakeHouseDatabase.GOLD
    TABLE_NAME = "frec"
    DESCRIPTION = "FREC Data after processing"
    CSV_NAME = "frec.csv"
    PK = "frec_id"
    UNIQUE_CONSTRAINTS = ["frec_code"]
    MIGRATION_HISTORY = []

    STRUCTURE = BaseSchema(
        [
            SchemaField("frec_id", SchemaType.INTEGER, False),
            SchemaField("frec_code", SchemaType.STRING, False),
            SchemaField("agency_name", SchemaType.STRING, False),
            SchemaField("agency_abbreviation", SchemaType.STRING, True),
            SchemaField("display_name", SchemaType.STRING, False),
            SchemaField("cgac_code", SchemaType.STRING, False),
            SchemaField("icon_name", SchemaType.STRING, True),
        ]
    )


class SubTierAgencyGold(CSVModel):
    BUCKET_NAME = CONFIG.LAKEHOUSE_REFERENCE_BUCKET
    DATABASE_NAME = LakeHouseDatabase.GOLD
    TABLE_NAME = "subtier_agency"
    DESCRIPTION = "Sub Tier Agency Data after processing"
    CSV_NAME = "subtier_agency.csv"
    PK = "subtier_agency_id"
    UNIQUE_CONSTRAINTS = ["subtier_code"]
    MIGRATION_HISTORY = []

    STRUCTURE = BaseSchema(
        [
            SchemaField("subtier_agency_id", SchemaType.INTEGER, False),
            SchemaField("subtier_code", SchemaType.STRING, False),
            SchemaField("subtier_name", SchemaType.STRING, False),
            SchemaField("cgac_code", SchemaType.STRING, False),
            SchemaField("frec_code", SchemaType.STRING, False),
            SchemaField("priority", SchemaType.INTEGER, False),
            SchemaField("is_frec", SchemaType.BOOLEAN, False),
        ]
    )


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
    CSV_NAME = "funding_opportunity_bronze.csv"
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
    CSV_NAME = "funding_opportunity_gold.csv"
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
            SchemaField("FY", SchemaType.STRING, False),
            SchemaField("PD", SchemaType.STRING, False),
            SchemaField("ALLOC_XFER_AGENCY", SchemaType.STRING, True),
            SchemaField("AID", SchemaType.STRING, False),
            SchemaField("MAIN_ACCT", SchemaType.STRING, False),
            SchemaField("SUB_ACCT", SchemaType.STRING, True),
            SchemaField("COMPOUND_KEY", SchemaType.STRING, False),
            SchemaField("PARK", SchemaType.STRING, False),
            SchemaField("PARK_NAME", SchemaType.STRING, False),
            SchemaField("RECORD_UPDATE_TS", SchemaType.STRING, True),
            SchemaField("FILE_UPDATE_TS", SchemaType.STRING, True),
        ]
    )


class ProgramActivityParkGold(CSVModel):
    BUCKET_NAME = CONFIG.LAKEHOUSE_REFERENCE_BUCKET
    DATABASE_NAME = LakeHouseDatabase.GOLD
    TABLE_NAME = "program_activity_park"
    DESCRIPTION = "Program activity park data after initial processing"
    CSV_NAME = "park.csv"
    PK = "park_code"
    STRUCTURE = BaseSchema(
        [
            SchemaField("created_at", SchemaType.TIMESTAMP, True),
            SchemaField("updated_at", SchemaType.TIMESTAMP, True),
            SchemaField("fiscal_year", SchemaType.INTEGER, False),
            SchemaField("period", SchemaType.INTEGER, False),
            SchemaField("allocation_transfer_id", SchemaType.STRING, True),
            SchemaField("agency_id", SchemaType.STRING, False),
            SchemaField("main_account_number", SchemaType.STRING, True),
            SchemaField("sub_account_number", SchemaType.STRING, False),
            SchemaField("park_code", SchemaType.STRING, False),
            SchemaField("park_name", SchemaType.STRING, False),
        ]
    )
