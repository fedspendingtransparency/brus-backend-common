import argparse
import asyncio
import logging
import sys
from datetime import datetime, timedelta
from typing import Any, Awaitable

import aiohttp
import numpy as np
import pandas as pd
from tqdm import tqdm

from brus_backend_common.config import CONFIG
from brus_backend_common.helpers.aws import _get_boto3
from brus_backend_common.helpers.generic import get_utc_now
from brus_backend_common.helpers.scripts import async_get_with_exception_hand, get_with_exception_hand, trim_nested_obj
from brus_backend_common.logging import configure_logging
from brus_backend_common.models.lakehouse_model import ExternalDataLoadDate, update_external_data_load_date
from brus_backend_common.models.reference import SubTierAgencyGold, OfficeGold

logger = logging.getLogger(__name__)


class OfficeLoader:

    API_URL: str = CONFIG.SAM_OFFICE_URL
    REQUESTS_AT_ONCE: int = 10
    LIMIT: int = 500
    TOP_SUB_LEVELS: list[str] = ["1", "2"]
    OFFICE_LEVELS: list[str] = ["3", "4", "5", "6", "7"]
    OFFICE_TYPES: list[str] = [
        "contract_funding",
        "contract_awards",
        "financial_assistance_awards",
        "financial_assistance_funding",
    ]

    def __init__(self) -> None:
        self.s3 = _get_boto3("client", "s3")
        self.metrics = {
            "script_name": "office_gold.py",
            "start_time": str(datetime.now()),
            "level_1_records": 0,
            "level_2_records": 0,
            "level_3_records": 0,
            "level_4_records": 0,
            "level_5_records": 0,
            "level_6_records": 0,
            "level_7_records": 0,
            "missing_cgacs": [],
            "missing_subtier_codes": [],
        }

    @staticmethod
    def export_office(filename: str) -> None:
        OfficeGold().to_pandas_df().to_csv(filename, index=False)

    async def load_offices(self, filename: str | None, update_db: bool, pull_all: bool, updated_date_from: str) -> None:
        start_time = get_utc_now()
        offices_list = []
        empty_pull_count = 0
        levels = self.TOP_SUB_LEVELS + self.OFFICE_LEVELS
        params = {"status": "all", "api_key": CONFIG.SAM_API_KEY.get_secret_value()}
        if pull_all and updated_date_from:
            params["updatedatefrom"] = updated_date_from
        for level in levels:
            total_expected_records = get_with_exception_hand(self.API_URL, params={**params, "level": level})[
                "totalrecords"
            ]
            self.metrics[f"level_{level}_records"] = total_expected_records
            if total_expected_records == 0:
                empty_pull_count += 1
                continue
            entries_processed = 0
            with tqdm(total=total_expected_records, desc=f"Level {level}", file=sys.stdout) as pbar:
                while entries_processed < total_expected_records:
                    office_data = await self.pull_offices({**params, "level": level}, entries_processed)
                    df = pd.DataFrame([org for office in office_data for org in office["orglist"]])
                    entries_processed += len(df)
                    new_office = self.parse_raw_office(df)
                    if not new_office.empty:
                        offices_list.append(new_office)
                        self.metrics["missing_cgacs"].append(new_office["agency_code"].tolist())
                        self.metrics["missing_subtier_codes"].append(new_office["sub_tier_code"].tolist())
                    pbar.update(len(df))
            if entries_processed > total_expected_records:
                # We have somehow retrieved more records than existed at the beginning of the pull
                logger.error(
                    f"Total expected records: {total_expected_records}, "
                    f"Number of records retrieved: {entries_processed}"
                )
                sys.exit(2)
        if offices_list:
            offices = pd.concat(offices_list, ignore_index=True)
            if OfficeGold().exists() and OfficeGold().count() > 0:
                offices = self.dedupe_offices(offices, pull_all, params)
            if filename:
                offices.to_csv(filename, index=False)
            if update_db:
                og = OfficeGold()
                end_time = get_utc_now()
                offices["created_at"] = end_time
                offices["updated_at"] = end_time
                og.save(offices)
                update_external_data_load_date(og, start_time, end_time)

    def dedupe_offices(self, new_offices: pd.DataFrame, pull_all: bool, params: dict) -> pd.DataFrame:
        date_cols = ["effective_start_date", "effective_end_date"]
        type_cols = [f"{office_type}_office" for office_type in self.OFFICE_TYPES]
        other_cols = ["office_name", "agency_code", "sub_tier_code"]
        shared_cols = date_cols + type_cols + other_cols
        shared_df_cols = ["office_code"] + shared_cols
        old_offices = OfficeGold().to_pandas_df().loc[lambda df: df["office_code"].isin(new_offices.office_code)]
        if pull_all:
            merged_offices = self.merge_offices(new_offices, old_offices)
        else:
            # Nightly load: split into active and inactive for different scenarios
            new_active_df = new_offices[new_offices["effective_end_date"].isnull()]
            new_inactive_df = new_offices[new_offices["effective_end_date"].notnull()]

            # If, for some odd reason, a daily load has *both* an active and inactive record with the same code,
            # delete it from the active list. The new inactive record may have an older start date
            # and we'd be merging it with the active record anyways
            new_active_df = new_active_df[~new_active_df["office_code"].isin(new_inactive_df["office_code"])]

            merged_offices = pd.DataFrame(columns=shared_df_cols)
            if not new_active_df.empty:
                # Active: Keep everything the same but take the older start date and orgtypes if applicable.
                old_active_df = old_offices[old_offices["office_code"].isin(new_active_df["office_code"])]
                if not old_active_df.empty:
                    # Set the DB effective end date to way back to distinguish it from the incoming active record
                    old_active_df["effective_end_date"] = "2000-01-01 00:00"
                    new_active_df = pd.concat([new_active_df[shared_df_cols], old_active_df[shared_df_cols]])
                    new_active_df = self.merge_offices(new_active_df, old_active_df)
                merged_offices = pd.concat([merged_offices[shared_df_cols], new_active_df[shared_df_cols]])

            if not new_inactive_df.empty:
                # Inactive: Discard incoming record but keep the code. It could be either
                #   officially declaring this record is now inactive
                #   OR an older historical record
                # Get the office's history and figure out its status from there
                new_inactive_office_codes = list(new_inactive_df["office_code"])
                logger.info(f"New inactive records found ({new_inactive_office_codes}). Looking them up...")
                new_inactive_df = pd.DataFrame(columns=shared_df_cols)
                for new_inactive_office_code in new_inactive_office_codes:
                    # This assumes an office's historical record count is always less than LIMIT
                    # If it's not, use pull_offices instead to multiply it by REQUESTS_AT_ONCE
                    office_history = get_with_exception_hand(self.API_URL, params=params)
                    df = pd.DataFrame([org for office in office_history for org in office["orglist"]])
                    historical_record = self.parse_raw_office(df)
                    if not historical_record.empty:
                        logger.info("Inactive records figured out")
                        new_inactive_df = self.merge_offices(historical_record, new_inactive_df)
                merged_offices = pd.concat([merged_offices[shared_df_cols], new_inactive_df[shared_df_cols]])
        return merged_offices

    def merge_offices(
        self, new_offices: pd.DataFrame, old_offices: pd.DataFrame, combine_org_types: bool = True
    ) -> pd.DataFrame:
        date_cols = ["effective_start_date", "effective_end_date"]
        type_cols = [f"{office_type}_office" for office_type in self.OFFICE_TYPES]
        other_cols = ["office_name", "agency_code", "sub_tier_code"]
        shared_cols = date_cols + type_cols + other_cols
        shared_df_cols = ["office_code"] + shared_cols
        agg_selections = {col: lambda x: x.values[-1] for col in shared_df_cols}
        agg_selections["effective_start_date"] = "min"
        for org_type in self.OFFICE_TYPES:
            agg_selections[f"{org_type}_office"] = "any" if combine_org_types else (lambda x: x.values[-1])
        return (
            pd.concat([new_offices[shared_df_cols], old_offices[shared_df_cols]], ignore_index=True)
            .sort_values(by=["effective_end_date", "effective_start_date"])
            .groupby(by=["office_code"], sort=False, as_index=False, dropna=False)
            .agg(agg_selections)
        )

    async def pull_offices(self, params: dict[str, str], entries_processed: int = 0) -> Awaitable[list[dict]]:
        params["limit"] = str(self.LIMIT)

        async def _fetch_single(session: aiohttp.ClientSession, offset: int | str) -> dict[str, Any]:
            """Fetch a single page of results with retry logic"""
            request_params = params.copy()
            request_params["offset"] = str(offset)
            # Use the retry function
            result = await async_get_with_exception_hand(
                session=session,
                url_string=self.API_URL,
                params=request_params,
                max_retries=5,  # Adjust as needed
                decode=True,
                resp_type="json",
            )

            return result if result else {}

        async def _fed_hierarchy_async_get(entries_already_processed: int) -> Awaitable[list[dict]]:
            # Create single session for connection pooling
            async with aiohttp.ClientSession() as session:
                tasks = []
                for start_offset in range(self.REQUESTS_AT_ONCE):
                    offset = entries_already_processed + (start_offset * self.LIMIT)
                    tasks.append(_fetch_single(session, offset))

                # Execute all requests concurrently
                return await asyncio.gather(*tasks)

        return await _fed_hierarchy_async_get(entries_processed)

    def parse_raw_office(self, df: pd.DataFrame) -> pd.DataFrame:
        required_cols = ["aacofficecode", "agencycode", "cgaclist", "fhorgname"]
        if not all(col in df for col in required_cols):
            return pd.DataFrame()
        result = (
            df.loc[df.fhorgname != "DO NOT USE"]
            .assign(
                agency_code=(
                    df.apply(trim_nested_obj)
                    .cgaclist.str[0]
                    .str["cgac"]
                    # TEMPORARILY REPLACE Navy, Army, AND Air Force WITH DOD
                    .replace(["017", "021", "057"], "097")
                )
            )
            .dropna(how="any", subset=["aacofficecode", "agencycode", "agency_code"])
            .assign(
                effective_end_date=lambda x: pd.to_datetime(
                    np.where(x.status == "ACTIVE", x.effectiveenddate, x.effectiveenddate.fillna("2000-01-02 00:00")),
                    errors="coerce",
                ).fillna("2000-01-02 00:00")
            )
        )
        if not "effectivestartdate" in result:
            result["effective_start_date"] = "2000-01-01 00:00"
        else:
            result = result.assign(
                effective_start_date=pd.to_datetime(
                    np.where(
                        pd.to_datetime(result["effectivestartdate"]) > pd.to_datetime("2000-01-01"),
                        result["effectivestartdate"],
                        "2000-01-01 00:00",
                    ),
                    errors="coerce",
                ).fillna("2000-01-01 00:00")
            )
        for ot in self.OFFICE_TYPES:
            result[f"{ot}_office"] = False
        if "fhorgofficetypelist" in df and result["fhorgofficetypelist"].notna().any():
            office_types = result["fhorgofficetypelist"].explode().str["officetype"].str.lower().str.replace(" ", "_")
            for ot in self.OFFICE_TYPES:
                result[f"{ot}_office"] = result.index.isin(office_types.loc[office_types == ot].index)
        return result.rename(
            columns={
                "aacofficecode": "office_code",
                "fhorgname": "office_name",
                "agencycode": "sub_tier_code",
            }
        )[
            [
                "office_code",
                "office_name",
                "sub_tier_code",
                "agency_code",
                "effective_start_date",
                "effective_end_date",
                "contract_funding_office",
                "contract_awards_office",
                "financial_assistance_awards_office",
                "financial_assistance_funding_office",
            ]
        ]

    @staticmethod
    def get_normalized_agency_code(agency_code: str, subtier_code: str) -> str | None:
        if agency_code in ["011", "016", "352", "537", "033", "511"]:
            frec_codes = SubTierAgencyGold().to_pandas_df().loc[lambda x: x["subtier_code"] == subtier_code].frec_code
            return frec_codes.iloc[0] if not frec_codes.empty else None
        return agency_code


if __name__ == "__main__":
    configure_logging()
    parser = argparse.ArgumentParser(description="Pull data from the Federal Hierarchy API.")
    parser.add_argument("-a", "--all", help="Clear out the database and get historical data", action="store_true")
    parser.add_argument("-f", "--filename", help="Generate a local CSV file from the data.", nargs=1, type=str)
    parser.add_argument("-d", "--pull_date", help="Date from which to start the pull", nargs=1, type=str)
    parser.add_argument(
        "-o",
        "--export_office",
        help="Export the current office table. Please provide the file name/path.",
        nargs=1,
        type=str,
    )
    parser.add_argument("-i", "--ignore_db", help="Do not update the DB tables.", action="store_true")
    args = parser.parse_args()
    if args.all and args.pull_date:
        logger.error("The -a and -d flags conflict, cannot use both at once.")
        sys.exit(1)
    updated_date_from = None
    if args.pull_date:
        try:
            updated_date_from = args.pull_date[0]
            datetime.strptime(updated_date_from, "%Y-%m-%d")
        except ValueError:
            logger.error("The date given to the -d flag was not parseable.")
            sys.exit(1)

    if not args.all and not updated_date_from:
        last_pull_date = (
            ExternalDataLoadDate()
            .to_pandas_df()
            .loc[lambda df: df["name"] == OfficeGold().TABLE_REF]
            .get(0, default=None)
        )
        if not last_pull_date:
            logger.error("The -a or -d flag must be set when there is no latest run in the database.")
            sys.exit(1)
        # We want to make the date one day earlier to account for any timing weirdness between the two systems
        updated_date_from = last_pull_date[0].date() - timedelta(days=1)

    office_loader = OfficeLoader()
    asyncio.run(
        office_loader.load_offices(
            filename=args.filename[0] if args.filename else None,
            update_db=False if args.ignore_db else True,
            pull_all=args.all,
            updated_date_from=updated_date_from,
        )
    )
