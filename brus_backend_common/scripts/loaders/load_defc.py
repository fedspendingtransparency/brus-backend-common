import argparse
import itertools
import json
import logging
import numpy as np
import os
import pandas as pd
import re
from datetime import datetime

from brus_backend_common.models import DEFCRaw, DEFCInt, DEFCGroup
from brus_backend_common.models.lakehouse_model import update_external_data_load_date
from brus_backend_common.helpers.aws import _get_boto3
from brus_backend_common.helpers.spark import SparkScriptSession
from brus_backend_common.helpers.pandas import check_dataframe_diff
from brus_backend_common.helpers.scripts import (
    clean_data,
    exit_if_nonlocal,
    get_with_exception_hand,
)
from brus_backend_common.config import CONFIG

logger = logging.getLogger(__name__)

VALID_HEADERS = {"DEFC_CODE", "DEFC_TITLE"}


def apply_defc_derivations(defc_df: pd.DataFrame, group_mapping: dict[str, list[str]]):
    """Given a base DEFC dataframe with 'DEFC' and 'Public Law', generate a dataframe with the derived elements

    Args:
        defc_df: the defc dataframe
        group_mapping: mapping of group -> [codes]

    Returns:
        the same dataframe with additional derived columns
    """
    logger.info("Deriving Public Law Data")
    defc_df = defc_df.merge(defc_df["Public Law"].apply(derive_pls_data), on="Public Law")
    # Ideally we would just update the data inplace
    # but since we're basing the merge and derivations off the original Public Law,
    # it's easier to just drop the old and rename the new
    defc_df = defc_df.drop(columns=["Public Law"])
    defc_df = defc_df.rename(columns={"Public Laws": "Public Law"})

    group_mappings_flat = dict(
        list(
            itertools.chain.from_iterable(
                [[(defc, group_name) for defc in defc_list] for group_name, defc_list in group_mapping.items()]
            )
        )
    )
    # Type Checker not happy with apply with lambdas
    defc_df["Group Name"] = defc_df.apply(lambda row: group_mappings_flat.get(row["DEFC"], None), axis=1)  # type: ignore
    defc_df["Is Valid"] = True

    return defc_df


def derive_pls_data(public_law: str):
    """Generates a series of the public law data derived from the public laws string

    Args:
        public_law: the full string containing the public laws

    Returns:
        a series populated with associated public law data
    """
    public_laws = []
    pl_short_titles = []
    urls = []
    dates_approved = []
    pl_nums = re.findall(r"(\d+-\d+)", public_law)

    # Rebuilding the Public Law string (accounting for multiple public laws)
    pl_types = {
        "Nonemergency": "Non-emergency",
        "Emergency": "Emergency",
        "Disaster": "Disaster",
        "Wildfire Suppression": "Wildfire Suppression",
    }
    pl_type = ""
    for pl_type_raw, pl_type_str in pl_types.items():
        if pl_type_raw.lower() in public_law.lower():
            pl_type = f"{pl_type_str} "
            break
    for pl_num in pl_nums:
        public_laws.append(f"{pl_type}P.L. {pl_num}")
        short_title, url, date_approved = derive_pl_data(pl_num)
        pl_short_titles.append(short_title)
        urls.append(url)
        if date_approved:
            dates_approved.append(date_approved)
    if len(pl_nums) == 0:
        public_laws = pl_short_titles = [public_law]

    return pd.Series(
        {
            "Public Law": public_law,
            "Public Laws": public_laws,
            "Public Law Short Title": pl_short_titles,
            "URLs": urls,
            "Earliest Public Law Enactment Date": min(dates_approved) if dates_approved else None,
        }
    )


def derive_pl_data(public_law: str):
    """Looks up the public law data from GovInfo and Congress.gov

    Args:
        public_law: a public law string ('<congress>-<law number')

    Returns:
        the short title
        url
        the date approved
    """
    short_title = ""
    url = ""
    date_approved = ""
    congress, law_number = public_law.split("-")
    govinfo_url = f"https://www.govinfo.gov/wssearch/getContentDetail?packageId=PLAW-{congress}publ{law_number}"
    govinfo_data = get_with_exception_hand(govinfo_url)
    if govinfo_data and isinstance(govinfo_data, dict) and "title" in govinfo_data:
        short_title = govinfo_data["title"]
        # Cutting out the 'Public Law <congress> - <law> - '
        short_title = short_title[11 + len(congress) + 3 + len(law_number) + 3 :]
        # stripping quotes from short title
        for char in ["'", '"', "`"]:
            short_title = short_title.replace(char, "")

        date_approved = govinfo_data["dcMD"]["origDateIssued"]

        url = govinfo_data["download"]["pdflink"]
        url = f"http:{url}"
    return short_title, url, date_approved


def add_defc_outliers(defc_df: pd.DataFrame, group_mapping: dict[str, list[str]]):
    """Given a DEFC dataframe, generate a dataframe with manually added records

    Args:
        defc_df: the defc dataframe
        group_mapping: mapping of group -> [codes]

    Returns:
        the same dataframe with manually added records
    """
    logger.info("Adding DEFC outliers")
    # DEFC 9
    covid_defcs = group_mapping["covid_19"]
    defc_9_title = (
        f"DEFC of '9' Indicates that the data for this row is not related to a COVID-19 P.L."
        f" (DEFC not one of the following: {covid_defcs}), but that the agency has declined to specify"
        f" which other DEFC (or combination of DEFCs, in the case that the money hasn't been split out"
        f" like it would be with a specific DEFC value) applies."
        f" This code was discontinued on July 13, 2021."
    )
    defc_9_df = pd.DataFrame(
        [
            {
                "DEFC": "9",
                "Public Law": [defc_9_title],
                "Public Law Short Title": [defc_9_title],
                "Is Valid": False,
                "URLs": [],
            }
        ]
    )
    defc_df = pd.concat([defc_df, defc_9_df], ignore_index=True)

    # DEFC QQQ
    defc_qqq_title = "Excluded from tracking (uses non-emergency/non-disaster designated appropriations)"
    defc_qqq_df = pd.DataFrame(
        [
            {
                "DEFC": "QQQ",
                "Public Law": [defc_qqq_title],
                "Public Law Short Title": [defc_qqq_title],
                "Is Valid": True,
                "URLs": [],
            }
        ]
    )
    defc_df = pd.concat([defc_df, defc_qqq_df], ignore_index=True)

    return defc_df


def setup_parser(parser: argparse.ArgumentParser):
    """Separating parser functionality as USAS uses Django Commands"""
    parser.add_argument(
        "--local_file",
        "-f",
        type=str,
        required=False,
        help="Load from a local file instead of pulling from S3",
    )
    parser.add_argument(
        "--force_reload",
        "-f",
        required=False,
        action="store_true",
        help="Force reload of the data",
    )
    return parser


def main(local_file: str | None = None, force_reload: bool = False, metrics_json: dict = None) -> dict:
    """Loads the Raw DEFC model into the Int DEFC model. Exports int DEFC csv to Public Files bucket if successful.

    Args:
        local_file: use local csv (and not the Raw DEFC model)
        force_reload: mapping of group -> [codes]

    Returns:
        metrics dict
    """
    if not metrics_json:
        metrics_json = {}

    s3 = _get_boto3("client", "s3")

    with SparkScriptSession() as spark:
        raw_model = DEFCRaw()
        if not raw_model.exists():
            raise ValueError(f"{raw_model.TABLE_REF} doesn't exist. Use create_migrate_delta_table beforehand.")

        group_model = DEFCGroup()
        if not group_model.exists():
            raise ValueError(f"{group_model.TABLE_REF} doesn't exist. Use create_migrate_delta_table beforehand.")

        int_model = DEFCInt(spark=spark)
        if not int_model.exists():
            raise ValueError(f"{int_model.TABLE_REF} doesn't exist. Use create_migrate_delta_table beforehand.")

        start_time = datetime.now()
        metrics_json["start_time"] = str(start_time)

        logger.info("Parsing DEFC data")
        try:
            if not local_file:
                raw_data = raw_model.to_pandas_df(dtype=str, na_filter=False)
            else:
                raw_data = pd.read_csv(local_file, dtype=str, na_filter=False)
        except pd.errors.EmptyDataError:
            metrics_json["blank_file"] = True
            metrics_json["exit_code"] = 4  # exit code chosen arbitrarily, to indicate distinct failure states
            return metrics_json
        headers = set([header.upper() for header in list(raw_data)])

        if not VALID_HEADERS.issubset(headers):
            logger.error("Missing required headers. Required headers include: %s" % str(VALID_HEADERS))
            metrics_json["exit_code"] = 4
            return metrics_json
        metrics_json["records_received"] = len(raw_data)
        # Creating a dataframe of the export csv first and then copying columns to match the database
        raw_data = raw_data.rename(columns={"DEFC_CODE": "DEFC", "DEFC_TITLE": "Public Law"})

        group_model_df = group_model.to_pandas_df()
        group_mapping = group_model_df.groupby("group")["code"].agg(list).to_dict()

        raw_data = apply_defc_derivations(raw_data, group_mapping)

        raw_data = add_defc_outliers(raw_data, group_mapping)

        # Clear any lingering np.nan's
        raw_data = raw_data.replace({np.nan: None})

        logger.info("Checking for differences in DEFC data")
        defc_mapping = {
            "defc": "code",
            "public_law": "public_laws",
            "public_law_short_title": "public_law_short_titles",
            "group_name": "group",
            "urls": "urls",
            "is_valid": "is_valid",
            "earliest_public_law_enactment_date": "earliest_pl_action_date",
        }
        data = clean_data(raw_data, defc_mapping, {})
        diff_found = check_dataframe_diff(data, int_model, ["defc_id"], ["code"], date_format="%Y-%m-%d")
        if force_reload or diff_found:

            # The only diff should be whenever a new code is added. Noting it here
            if diff_found:
                incoming_defcs = list(data["code"])
                curr_defcs = list(int_model.to_pandas_df()["code"])
                diff_defcs = list(set(incoming_defcs) - set(curr_defcs))
                metrics_json["new_defc"] = diff_defcs
                logger.info(f"Difference found: {diff_defcs}")

            logger.info("Overwriting new DEFC data to Broker")
            int_model.save(data)

            update_external_data_load_date(int_model, start_time, datetime.now())
            logger.info("{} records inserted to DEFC".format(len(data)))

            # convert the arrays to pipe-delimited strings
            defc_delim = "|"
            array_cols = ["Public Law", "Public Law Short Title", "URLs"]
            for array_col in array_cols:
                raw_data[array_col] = raw_data[array_col].apply(lambda value: defc_delim.join(value))

            header_order = [
                "DEFC",
                "Public Law",
                "Public Law Short Title",
                "Group Name",
                "URLs",
                "Is Valid",
                "Earliest Public Law Enactment Date",
            ]
            raw_data = raw_data[header_order]
            export_name = "def_codes.csv"
            logger.info("Exporting loaded DEFC file to {}".format(export_name))
            raw_data.to_csv(export_name, index=0)

            s3.upload_file(export_name, CONFIG.PUBLIC_FILES_BUCKET, export_name)

            os.remove(export_name)
        else:
            logger.info("No differences found, skipping defc table reload.")

        total_defc_count = int_model.count()

    metrics_json["total_defc_count"] = total_defc_count

    end_time = datetime.now()
    metrics_json["end_time"] = str(end_time)
    metrics_json["duration"] = str(end_time - start_time)

    if not (force_reload or diff_found):
        metrics_json["exit_code"] = 3

    return metrics_json


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process the raw.defc data into the int.defc table.")
    parser = setup_parser(parser)
    args = parser.parse_args()

    metrics_json = {
        "script_name": "load_defc.py",
        "records_received": 0,
        "new_defc": [],
        "total_defc_count": 0,
        "blank_file": False,
        "exit_code": 0,
    }

    metrics_json = main(args.local_file, args.force_reload, metrics_json=metrics_json)

    blank_file = metrics_json.pop("blank_file")
    exit_code = metrics_json.pop("exit_code")

    with open("load_defc_metrics.json", "w+") as metrics_file:
        json.dump(metrics_json, metrics_file)

    s3 = _get_boto3("client", "s3")
    s3.upload_file("load_defc_metrics.json", CONFIG.METRICS_BUCKET, "load_defc_metrics.json")

    if exit_code != 0:
        exit_if_nonlocal(exit_code, blank_file)
