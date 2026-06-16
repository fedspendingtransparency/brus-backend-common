import asyncio
import logging
import sys
import time
from datetime import datetime

import aiohttp
import numpy as np
import pandas as pd

from brus_backend_common.config import CONFIG
from brus_backend_common.helpers.aws import _get_boto3
from brus_backend_common.helpers.scripts import get_with_exception_hand, trim_nested_obj
from brus_backend_common.logging import configure_logging
from brus_backend_common.models.lakehouse_model import ExternalDataLoadDate, update_external_data_load_date
from brus_backend_common.models.reference import SubTierAgencyGold, OfficeGold

logger = logging.getLogger(__name__)


class OfficeLoader:

    API_URL: str = CONFIG.SAM_OFFICE_URL
    REQUESTS_AT_ONCE: int = 5
    LIMIT: int = 100
    TOP_SUB_LEVELS: list[str] = ["1", "2"]
    OFFICE_LEVELS: list[str] = ["3", "4", "5", "6", "7"]
    OFFICE_TYPES: list[str] = [
        "contract_funding",
        "contract_awards",
        "financial_assistance_awards",
        "financial_assistance_funding",
    ]

    def __init__(self):
        self.s3 = _get_boto3("client", "s3")
        self.metrics = {
            "script_name": "load_federal_hierarchy.py",
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

    async def load_offices(self):
        start_time = time.perf_counter()
        offices = pd.DataFrame()
        empty_pull_count = 0
        levels = self.TOP_SUB_LEVELS + self.OFFICE_LEVELS
        for level in levels:
            params = {"level": level, "status": "all", "api_key": CONFIG.SAM_API_KEY.get_secret_value()}
            total_expected_records = get_with_exception_hand(self.API_URL, params=params)["totalrecords"]
            self.metrics[f"level_{level}_records"] = total_expected_records
            logger.info(f"{total_expected_records} level-{level} record(s) expected")
            if total_expected_records == 0:
                empty_pull_count += 1
                continue
            entries_processed = 0
            while True:
                df = pd.DataFrame(
                    [org for office in await self.pull_offices(params, entries_processed) for org in office["orglist"]]
                )
                start = entries_processed + 1
                entries_processed += len(df)
                new_office = self.parse_raw_office(df)
                if not new_office.empty:
                    # store all the cgacs/subtiers loaded in from this run, to be filtered later
                    self.metrics["missing_cgacs"].append(new_office["agency_code"].tolist())
                    self.metrics["missing_subtier_codes"].append(new_office["sub_tier_code"].tolist())
                if not new_office.empty:
                    end_time = time.perf_counter()
                    logger.info(f"Processed {new_office.shape[0]} new offices in {end_time - start_time:.2f} seconds")
                    offices = pd.concat([offices, new_office])

                logger.info("Processed rows %s-%s", start, entries_processed)
                if entries_processed == total_expected_records:
                    # Feed has finished
                    end_time = time.perf_counter()
                    logger.info(f"Processed {total_expected_records} records in {end_time - start_time} seconds")
                    break

                if entries_processed > total_expected_records:
                    # We have somehow retrieved more records than existed at the beginning of the pull
                    logger.error(
                        f"Total expected records: {total_expected_records},"
                        f" Number of records retrieved: {entries_processed}"
                    )
                    sys.exit(2)
            OfficeGold().save(offices)

    async def pull_offices(self, params, entries_processed=0):
        """Hit the FH API and return the raw json responses

        Args:
            params: dict of the current params to lookup
            entries_processed: how many entries have been processed (for offset counting)

        Returns:
            list of json responses
        """
        params["limit"] = str(self.LIMIT)

        async def _fetch_single(session, offset):
            """Fetch a single page of results"""
            request_params = params.copy()
            request_params["offset"] = str(offset)

            async with session.get(self.API_URL, params=request_params) as response:
                response.raise_for_status()  # Raises exception for 4xx/5xx
                return await response.json()

        async def _fed_hierarchy_async_get(entries_already_processed):
            # Create single session for connection pooling
            async with aiohttp.ClientSession() as session:
                tasks = []
                for start_offset in range(self.REQUESTS_AT_ONCE):
                    offset = entries_already_processed + (start_offset * self.LIMIT)
                    tasks.append(_fetch_single(session, offset))

                # Execute all requests concurrently
                return await asyncio.gather(*tasks)

        return await _fed_hierarchy_async_get(entries_processed)

    def parse_raw_office(self, df: pd.DataFrame, add_office_cols=False) -> pd.DataFrame:
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
                effective_end_date=lambda x: np.where(
                    x.status == "ACTIVE", x.effectiveenddate, x.effectiveenddate.fillna("2000-01-02 00:00")
                )
            )
        )
        if not "effectivestartdate" in result:
            result["effective_start_date"] = "2000-01-01 00:00"
        else:
            result = result.assign(
                effective_start_date=np.where(
                    pd.to_datetime(result["effectivestartdate"]) > pd.to_datetime("2000-01-01"),
                    result["effectivestartdate"],
                    "2000-01-01 00:00",
                )
            )
        office_type_col_names = []
        if "fhorgofficetypelist" in df and add_office_cols:
            office_type = (
                result["fhorgofficetypelist"]
                .explode()
                .str["officetype"]
                .str.lower()
                .str.replace(" ", "_")
                .to_frame(name="office_type")
            )
            office_type_cols = (
                office_type.loc[office_type["office_type"].isin(self.OFFICE_TYPES)]
                .assign(value=True)
                .pivot_table(
                    index=office_type.index,
                    columns="office_type",
                    values="value",
                    aggfunc="first",
                    fill_value=False,
                )
            )
            office_type_col_names = [f"{col}_office" for col in office_type_cols.columns]
            office_type_cols.columns = office_type_col_names
            result = result.join(office_type_cols, how="left").fillna(False)

        return result.assign(
            **{
                "contract_funding_office": False,
                "contract_awards_office": False,
                "financial_assistance_awards_office": False,
                "financial_assistance_funding_office": False,
            }
        ).rename(
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
            + office_type_col_names
        ]

    @staticmethod
    def get_normalized_agency_code(agency_code, subtier_code):
        if agency_code in ["011", "016", "352", "537", "033", "511"]:
            frec_codes = SubTierAgencyGold().to_pandas_df().loc[lambda x: x["subtier_code"] == subtier_code].frec_code
            return frec_codes.iloc[0] if not frec_codes.empty else None

        return agency_code


if __name__ == "__main__":
    configure_logging()
    print("hello")
    office_loader = OfficeLoader()
    asyncio.run(office_loader.load_offices())
