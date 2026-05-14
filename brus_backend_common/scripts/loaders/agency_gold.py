import argparse
import json
import logging
import pandas as pd
from datetime import datetime

from brus_backend_common.models import AgencyBronze, CGACGold, FRECGold, SubTierAgencyGold
from brus_backend_common.models.lakehouse_model import update_external_data_load_date
from brus_backend_common.helpers.aws import _get_boto3
from brus_backend_common.helpers.pandas import check_dataframe_diff
from brus_backend_common.helpers.scripts import clean_data, exit_if_nonlocal
from brus_backend_common.config import CONFIG

logger = logging.getLogger(__name__)

CUSTOM_CGACS = [
    {
        "CGAC AGENCY CODE": "999",
        "AGENCY NAME": "Non-published FABS Vendor Agency",
        "AGENCY ABBREVIATION": "TFVA",
        "ICON FILENAME": None,
    }
]

CUSTOM_SUBTIERS = [
    {
        "CGAC AGENCY CODE": "999",
        "SUBTIER CODE": "TFVA",
        "SUBTIER NAME": "Non-published FABS Vendor Subtier Agency",
        "TOPTIER_FLAG": "TRUE",
        "IS_FREC": "FALSE",
        "ICON FILENAME": None,
    }
]


def load_cgac(
    raw_data: pd.DataFrame, start_time: datetime, force_reload: bool = False, metrics_json: dict = None
) -> dict:
    """Loads the CGAC data into the gold table

    Args:
        raw_data: the raw agency codes bronze table
        start_time: the start time of the script
        force_reload: Boolean flag to determine if a reload should happen regardless of new data
        metrics_json: dict to collect metrics for the script

    Returns:
        metrics dict
    """
    cgac_model = CGACGold()
    if not cgac_model.exists():
        raise ValueError(f"{cgac_model.TABLE_REF} doesn't exist. Use create_migrate_delta_table beforehand.")

    # Check and add custom CGACs to incoming list for comparison
    for custom_cgac in CUSTOM_CGACS:
        if custom_cgac["CGAC AGENCY CODE"] in raw_data["CGAC AGENCY CODE"].values:
            raise ValueError(
                f"Custom CGAC code found in agency list: {custom_cgac['CGAC AGENCY CODE']}."
                f" Consult the latest agency list with the custom CGAC code."
            )
        else:
            custom_cgac_row = pd.DataFrame([custom_cgac])
            raw_data = pd.concat([raw_data, custom_cgac_row], ignore_index=True)

    cgac_mapping = {
        "cgac_agency_code": "cgac_code",
        "agency_name": "agency_name",
        "agency_abbreviation": "agency_abbreviation",
        "icon_filename": "icon_name",
    }

    cgac_data = clean_data(raw_data, cgac_mapping, {"cgac_code": {"pad_to_length": 3}})

    cgac_data.drop_duplicates(subset=["cgac_code"], inplace=True)
    diff_found = check_dataframe_diff(cgac_data, cgac_model.to_pandas_df(), ["cgac_id", "display_name"], ["cgac_code"])

    if force_reload or diff_found:
        metrics_json["cgac_loaded"] = len(cgac_data)
        cgac_data["display_name"] = cgac_data.apply(
            lambda row: (
                f"{row["agency_name"]} ({row["agency_abbreviation"]})"
                if row["agency_abbreviation"]
                else f"{row["agency_name"]} (nan)"
            ),
            axis=1,
        )
        logger.info("Overwriting new CGAC data to Broker")
        cgac_model.save(cgac_data)
        update_external_data_load_date(cgac_model, start_time, datetime.now())

    return metrics_json


def load_frec(
    raw_data: pd.DataFrame, start_time: datetime, force_reload: bool = False, metrics_json: dict = None
) -> dict:
    """Loads the FREC data into the gold table

    Args:
        raw_data: the raw agency codes bronze table
        start_time: the start time of the script
        force_reload: Boolean flag to determine if a reload should happen regardless of new data
        metrics_json: dict to collect metrics for the script

    Returns:
        metrics dict
    """
    frec_model = FRECGold()
    if not frec_model.exists():
        raise ValueError(f"{frec_model.TABLE_REF} doesn't exist. Use create_migrate_delta_table beforehand.")

    frec_mapping = {
        "frec": "frec_code",
        "cgac_agency_code": "cgac_code",
        "frec_entity_description": "agency_name",
        "frec_abbreviation": "agency_abbreviation",
        "frec_cgac_association": "frec_cgac",
        "icon_filename": "icon_name",
    }

    frec_data = clean_data(
        raw_data,
        frec_mapping,
        {"frec": {"keep_null": False}, "cgac_code": {"pad_to_length": 3}, "frec_code": {"pad_to_length": 4}},
    )

    # de-dupe
    frec_data = frec_data[frec_data.frec_cgac == "TRUE"]
    frec_data.drop(["frec_cgac"], axis=1, inplace=True)
    frec_data.drop_duplicates(subset=["frec_code"], inplace=True)

    diff_found = check_dataframe_diff(frec_data, frec_model.to_pandas_df(), ["frec_id", "display_name"], ["frec_code"])

    if force_reload or diff_found:
        metrics_json["frec_loaded"] = len(frec_data)
        frec_data["display_name"] = frec_data.apply(
            lambda row: (
                f"{row["agency_name"]} ({row["agency_abbreviation"]})"
                if row["agency_abbreviation"]
                else f"{row["agency_name"]} (nan)"
            ),
            axis=1,
        )
        logger.info("Overwriting new FREC data to Broker")
        frec_model.save(frec_data)
        update_external_data_load_date(frec_model, start_time, datetime.now())

    return metrics_json


def load_subtier(
    raw_data: pd.DataFrame, start_time: datetime, force_reload: bool = False, metrics_json: dict = None
) -> dict:
    """Loads the SubTier data into the gold table

    Args:
        raw_data: the raw agency codes bronze table
        start_time: the start time of the script
        force_reload: Boolean flag to determine if a reload should happen regardless of new data
        metrics_json: dict to collect metrics for the script

    Returns:
        metrics dict
    """
    subtier_model = SubTierAgencyGold()
    if not subtier_model.exists():
        raise ValueError(f"{subtier_model.TABLE_REF} doesn't exist. Use create_migrate_delta_table beforehand.")

    # Check and add custom Subtiers to incoming list for comparison
    for custom_subtier in CUSTOM_SUBTIERS:
        if custom_subtier["SUBTIER CODE"] in raw_data["SUBTIER CODE"].values:
            raise ValueError(
                f"Custom Subtier code found in agency list: {custom_subtier['SUBTIER CODE']}."
                f" Consult the latest agency list with the custom Subtier code."
            )
        else:
            custom_cgac_row = pd.DataFrame([custom_subtier])
            raw_data = pd.concat([raw_data, custom_cgac_row], ignore_index=True)

    condition = raw_data["TOPTIER_FLAG"] == "TRUE"
    raw_data.loc[condition, "PRIORITY"] = 1
    raw_data.loc[~condition, "PRIORITY"] = 2
    raw_data["PRIORITY"] = raw_data["PRIORITY"].astype(int)
    raw_data.replace({"TRUE": True, "FALSE": False}, inplace=True)

    subtier_mapping = {
        "cgac_agency_code": "cgac_code",
        "subtier_code": "subtier_code",
        "priority": "priority",
        "frec": "frec_code",
        "subtier_name": "subtier_name",
        "is_frec": "is_frec",
    }

    subtier_data = clean_data(
        raw_data,
        subtier_mapping,
        {
            "cgac_code": {"pad_to_length": 3},
            "frec_code": {"pad_to_length": 4},
            "subtier_code": {"pad_to_length": 4},
        },
    )

    # de-dupe
    subtier_data.drop_duplicates(subset=["subtier_code"], inplace=True)

    diff_found = check_dataframe_diff(
        subtier_data, subtier_model.to_pandas_df(), ["subtier_agency_id", "display_name"], ["frec_code"]
    )

    if force_reload or diff_found:
        metrics_json["subtiers_loaded"] = len(subtier_data)
        logger.info("Overwriting new SubTier data to Broker")
        subtier_model.save(subtier_data)
        update_external_data_load_date(subtier_model, start_time, datetime.now())

    return metrics_json


def main(local_file: str | None = None, force_reload: bool = False, metrics_json: dict = None) -> dict:
    """Loads the raw Agency file into a bronze table and separates it into its constituent parts of CGAC, FREC, and SubTier.

    Args:
        local_file: use local csv (and not the Raw DEFC model)
        force_reload: Boolean flag to determine if a reload should happen regardless of new data
        metrics_json: dict to collect metrics for the script

    Returns:
        metrics dict
    """
    if not metrics_json:
        metrics_json = {}

    raw_model = AgencyBronze()
    if not raw_model.exists():
        raise ValueError(f"{raw_model.TABLE_REF} doesn't exist. Use create_migrate_delta_table beforehand.")

    start_time = datetime.now()
    metrics_json["start_time"] = str(start_time)

    logger.info("Parsing Agency data")
    try:
        if not local_file:
            raw_data = raw_model.to_pandas_df(dtype=str, na_filter=False)
        else:
            raw_data = pd.read_csv(local_file, dtype=str, na_filter=False)
    except pd.errors.EmptyDataError:
        metrics_json["blank_file"] = True
        metrics_json["exit_code"] = 4  # exit code chosen arbitrarily, to indicate distinct failure states
        return metrics_json

    metrics_json = load_cgac(raw_data, start_time, force_reload, metrics_json)
    metrics_json = load_frec(raw_data, start_time, force_reload, metrics_json)
    metrics_json = load_subtier(raw_data, start_time, force_reload, metrics_json)

    end_time = datetime.now()
    metrics_json["end_time"] = str(end_time)
    metrics_json["duration"] = str(end_time - start_time)
    return metrics_json


def setup_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Separating parser functionality as USAS uses Django Commands"""
    parser.add_argument(
        "--local_file",
        "-l",
        type=str,
        required=False,
        help="Local file to pull from instead of pulling from S3",
    )
    parser.add_argument(
        "--force_reload",
        "-f",
        required=False,
        action="store_true",
        help="Force reload of the data",
    )
    return parser


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process the bronze defc data into the gold defc table.")
    parser = setup_parser(parser)
    args = parser.parse_args()

    metrics_json = {
        "script_name": "load_agency.py",
        "cgac_loaded": 0,
        "frec_loaded": 0,
        "subtiers_loaded": 0,
        "blank_file": False,
        "exit_code": 0,
    }

    metrics_json = main(args.local_file, args.force_reload, metrics_json)

    blank_file = metrics_json.pop("blank_file")
    exit_code = metrics_json.pop("exit_code")

    with open("load_agency_metrics.json", "w+") as metrics_file:
        json.dump(metrics_json, metrics_file)

    s3 = _get_boto3("client", "s3")
    s3.upload_file("load_agency_metrics.json", CONFIG.METRICS_BUCKET, "load_agency_metrics.json")

    if exit_code != 0:
        exit_if_nonlocal(exit_code, blank_file)
