import os
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from brus_backend_common.config import _SRC_ROOT_DIR
from brus_backend_common.helpers.aws import _get_boto3
from brus_backend_common.models.lakehouse_model import ExternalDataLoadDate
from brus_backend_common.models.reference import OfficeGold, SubTierAgencyGold
from brus_backend_common.scripts.loaders.office_gold import OfficeLoader


@pytest.fixture(scope="function")
def mock_sam_api_response():
    """Mock response from SAM API"""
    return {
        "totalrecords": 2,
        "orglist": [
            {
                "aacofficecode": "TEST001",
                "fhorgname": "Test Office 1",
                "agencycode": "ABC",
                "cgaclist": [{"cgac": "012"}],
                "status": "ACTIVE",
                "effectivestartdate": "2020-01-01 00:00",
                "effectiveenddate": None,
                "fhorgofficetypelist": [{"officetype": "Contract Funding"}],
            },
            {
                "aacofficecode": "TEST002",
                "fhorgname": "Test Office 2",
                "agencycode": "DEF",
                "cgaclist": [{"cgac": "013"}],
                "status": "INACTIVE",
                "effectivestartdate": "2019-01-01 00:00",
                "effectiveenddate": "2023-12-31 00:00",
                "fhorgofficetypelist": [{"officetype": "Contract Awards"}],
            },
        ],
    }


@pytest.fixture(scope="function")
def sample_office_dataframe():
    """Sample office DataFrame for testing"""
    return pd.DataFrame(
        {
            "office_code": ["TEST001", "TEST002"],
            "office_name": ["Test Office 1", "Test Office 2"],
            "sub_tier_code": ["ABC", "DEF"],
            "agency_code": ["012", "013"],
            "effective_start_date": pd.to_datetime(["2020-01-01", "2019-01-01"]),
            "effective_end_date": [pd.NaT, pd.to_datetime("2023-12-31")],
            "contract_funding_office": [True, False],
            "contract_awards_office": [False, True],
            "financial_assistance_awards_office": [False, False],
            "financial_assistance_funding_office": [False, False],
            "created_at": ["2023-01-01 00:00", "2023-01-01 00:00"],
            "updated_at": ["2023-01-01 00:00", "2023-01-01 00:00"],
        }
    )


@pytest.fixture(scope="function")
def upload_office_test_data(s3_unittest_data_bucket):
    """Upload test office data to S3"""
    gold = OfficeGold()
    s3_client = _get_boto3("client", "s3")
    csv_file_path = os.path.join(_SRC_ROOT_DIR, "tests", "integration", "data", "test_offices.csv")
    test_df = pd.DataFrame(
        {
            "office_id": [1],
            "office_code": ["OLD001"],
            "office_name": ["Old Office"],
            "sub_tier_code": ["XYZ"],
            "agency_code": ["014"],
            "effective_start_date": ["2018-01-01 00:00"],
            "effective_end_date": [None],
            "contract_funding_office": [True],
            "contract_awards_office": [False],
            "financial_assistance_awards_office": [False],
            "financial_assistance_funding_office": [False],
            "created_at": ["2023-01-01 00:00"],
            "updated_at": ["2023-01-01 00:00"],
        }
    )
    test_df.to_csv(csv_file_path, index=False)
    s3_client.upload_file(csv_file_path, s3_unittest_data_bucket, gold.RELATIVE_CSV_PATH)

    yield

    s3_client.delete_object(Bucket=s3_unittest_data_bucket, Key=gold.RELATIVE_CSV_PATH)
    if os.path.exists(csv_file_path):
        os.remove(csv_file_path)


class TestOfficeLoader:

    def test_initialization(self):
        """Test OfficeLoader initialization"""
        loader = OfficeLoader()
        assert loader.API_URL is not None
        assert loader.REQUESTS_AT_ONCE == 10
        assert loader.LIMIT == 500
        assert "script_name" in loader.metrics
        assert loader.metrics["script_name"] == "office_gold.py"

    def test_parse_raw_office_valid_data(self):
        """Test parsing valid office data"""
        loader = OfficeLoader()
        raw_df = pd.DataFrame(
            {
                "aacofficecode": ["TEST001"],
                "fhorgname": ["Test Office"],
                "agencycode": ["ABC"],
                "cgaclist": [[{"cgac": "012"}]],
                "status": ["ACTIVE"],
                "effectivestartdate": ["2020-01-01 00:00"],
                "effectiveenddate": [None],
                "fhorgofficetypelist": [[{"officetype": "Contract Funding"}]],
            }
        )

        result = loader.parse_raw_office(raw_df)

        assert not result.empty
        assert result["office_code"].iloc[0] == "TEST001"
        assert result["office_name"].iloc[0] == "Test Office"
        assert result["sub_tier_code"].iloc[0] == "ABC"
        assert result["agency_code"].iloc[0] == "012"
        assert result["contract_funding_office"].iloc[0]

    def test_parse_raw_office_missing_columns(self):
        """Test parsing with missing required columns"""
        loader = OfficeLoader()
        raw_df = pd.DataFrame({"aacofficecode": ["TEST001"]})

        result = loader.parse_raw_office(raw_df)

        assert result.empty

    def test_parse_raw_office_do_not_use_filter(self):
        """Test that 'DO NOT USE' offices are filtered out"""
        loader = OfficeLoader()
        raw_df = pd.DataFrame(
            {
                "aacofficecode": ["TEST001"],
                "fhorgname": ["DO NOT USE"],
                "agencycode": ["ABC"],
                "cgaclist": [[{"cgac": "012"}]],
                "status": ["ACTIVE"],
                "effectiveenddate": ["2020-01-02 00:00"],
            }
        )

        result = loader.parse_raw_office(raw_df)

        assert result.empty

    def test_parse_raw_office_military_agency_replacement(self):
        """Test that Navy, Army, Air Force codes are replaced with DOD"""
        loader = OfficeLoader()
        for military_code in ["017", "021", "057"]:
            raw_df = pd.DataFrame(
                {
                    "aacofficecode": ["TEST001"],
                    "fhorgname": ["Military Office"],
                    "agencycode": ["ABC"],
                    "cgaclist": [[{"cgac": military_code}]],
                    "status": ["ACTIVE"],
                    "effectiveenddate": ["2020-01-02 00:00"],
                }
            )

            result = loader.parse_raw_office(raw_df)

            assert result["agency_code"].iloc[0] == "097"

    def test_merge_offices_combine_org_types(self, sample_office_dataframe):
        """Test merging offices with org type combination"""
        loader = OfficeLoader()

        new_offices = sample_office_dataframe.copy()
        old_offices = sample_office_dataframe.copy()
        old_offices["contract_awards_office"] = True

        result = loader.merge_offices(new_offices, old_offices, combine_org_types=True)

        assert result.loc[result["office_code"] == "TEST001", "contract_awards_office"].iloc[0]

    def test_merge_offices_take_earliest_start_date(self, sample_office_dataframe):
        """Test that merge takes the earliest start date"""
        loader = OfficeLoader()

        new_offices = sample_office_dataframe.copy()
        old_offices = sample_office_dataframe.copy()
        old_offices["effective_start_date"] = pd.to_datetime("2015-01-01")

        result = loader.merge_offices(new_offices, old_offices)

        assert result["effective_start_date"].iloc[0] == pd.to_datetime("2015-01-01")

    def test_get_normalized_agency_code_frec_codes(self):
        """Test agency code normalization for FREC codes"""
        with patch.object(SubTierAgencyGold, "to_pandas_df") as mock_df:
            mock_df.return_value = pd.DataFrame({"subtier_code": ["ABC"], "frec_code": ["9999"]})

            result = OfficeLoader.get_normalized_agency_code("011", "ABC")

            assert result == "9999"

    def test_get_normalized_agency_code_regular_codes(self):
        """Test agency code normalization for regular codes"""
        result = OfficeLoader.get_normalized_agency_code("012", "ABC")

        assert result == "012"

    @pytest.mark.asyncio
    async def test_pull_offices(self, mock_sam_api_response):
        """Test pulling offices from API"""
        loader = OfficeLoader()

        with patch(
            "brus_backend_common.scripts.loaders.office_gold.async_get_with_exception_hand",
            new_callable=AsyncMock,
        ) as mock_get:
            mock_get.return_value = mock_sam_api_response

            result = await loader.pull_offices({"level": "3", "api_key": "test"}, 0)

            assert len(result) == loader.REQUESTS_AT_ONCE
            mock_get.assert_called()

    def test_export_office(self, tmp_path):
        """Test exporting office data to CSV"""
        test_file = tmp_path / "test_offices.csv"

        with patch.object(OfficeGold, "to_pandas_df") as mock_df:
            mock_df.return_value = pd.DataFrame({"office_code": ["TEST001"]})

            OfficeLoader.export_office(str(test_file))

            assert test_file.exists()

    @pytest.mark.asyncio
    async def test_load_offices_integration(
        self, s3_unittest_data_bucket, hive_unittest_metastore_db, upload_office_test_data
    ):
        """Integration test for loading offices"""
        with (
            patch.object(OfficeGold, "BUCKET_NAME", s3_unittest_data_bucket),
            patch.object(ExternalDataLoadDate, "BUCKET_NAME", s3_unittest_data_bucket),
            patch("brus_backend_common.scripts.loaders.office_gold.get_with_exception_hand") as mock_get,
            patch.object(OfficeLoader, "pull_offices", new_callable=AsyncMock) as mock_pull,
        ):
            edld_model = ExternalDataLoadDate()
            edld_model.initialize(recreate=True)
            mock_get.return_value = {"totalrecords": 1}
            mock_pull.return_value = [
                {
                    "orglist": [
                        {
                            "aacofficecode": "NEW001",
                            "fhorgname": "New Office",
                            "agencycode": "ABC",
                            "cgaclist": [{"cgac": "012"}],
                            "status": "ACTIVE",
                            "effectivestartdate": "2024-01-01 00:00",
                        }
                    ]
                }
            ]

            loader = OfficeLoader()
            await loader.load_offices(filename=None, update_db=True, pull_all=False, updated_date_from="2024-01-01")

            office_model = OfficeGold()
            assert office_model.exists()

            edld_model = ExternalDataLoadDate()
            assert edld_model.exists()
            df = edld_model.to_pandas_df()
            assert not df.loc[df["name"] == office_model.TABLE_REF].empty

    def test_dedupe_offices_pull_all(self, sample_office_dataframe):
        """Test deduplication logic for full pull removes duplicates"""
        loader = OfficeLoader()

        # Create a DataFrame with duplicate office codes
        duplicate_offices = pd.DataFrame(
            {
                "office_code": ["TEST001", "TEST001", "TEST002"],
                "office_name": ["Test Office 1", "Test Office 1 Updated", "Test Office 2"],
                "sub_tier_code": ["ABC", "ABC", "DEF"],
                "agency_code": ["012", "012", "013"],
                "effective_start_date": pd.to_datetime(["2020-01-01", "2019-01-01", "2019-01-01"]),
                "effective_end_date": [pd.NaT, pd.NaT, pd.to_datetime("2023-12-31")],
                "contract_funding_office": [True, False, False],
                "contract_awards_office": [False, True, True],
                "financial_assistance_awards_office": [False, False, False],
                "financial_assistance_funding_office": [False, False, False],
            }
        )

        with patch.object(OfficeGold, "to_pandas_df") as mock_df:
            # Mock existing offices in database
            mock_df.return_value = sample_office_dataframe.copy()

            result = loader.dedupe_offices(duplicate_offices, pull_all=True, params={})

            # Verify duplicates are removed - should only have 2 unique office codes
            assert len(result) == 2
            assert result["office_code"].nunique() == 2

            # Verify the earliest start date is kept
            test001_record = result[result["office_code"] == "TEST001"]
            assert test001_record["effective_start_date"].iloc[0] == pd.to_datetime("2019-01-01")

            # Verify org types are combined (both should be True)
            assert test001_record["contract_funding_office"].iloc[0]
            assert test001_record["contract_awards_office"].iloc[0]

    def test_dedupe_offices_nightly_active_records(self):
        """Test deduplication logic for nightly load with active records"""
        loader = OfficeLoader()

        # New active offices
        new_offices = pd.DataFrame(
            {
                "office_code": ["TEST001", "TEST002"],
                "office_name": ["Test Office 1", "Test Office 2"],
                "sub_tier_code": ["ABC", "DEF"],
                "agency_code": ["012", "013"],
                "effective_start_date": pd.to_datetime(["2024-01-01", "2024-01-01"]),
                "effective_end_date": [pd.NaT, pd.NaT],
                "contract_funding_office": [True, False],
                "contract_awards_office": [False, True],
                "financial_assistance_awards_office": [False, False],
                "financial_assistance_funding_office": [False, False],
            }
        )

        # Existing offices in database with earlier start dates
        old_offices = pd.DataFrame(
            {
                "office_code": ["TEST001"],
                "office_name": ["Test Office 1 Old"],
                "sub_tier_code": ["ABC"],
                "agency_code": ["012"],
                "effective_start_date": pd.to_datetime(["2020-01-01"]),
                "effective_end_date": [pd.NaT],
                "contract_funding_office": [False],
                "contract_awards_office": [True],
                "financial_assistance_awards_office": [False],
                "financial_assistance_funding_office": [False],
            }
        )

        with patch.object(OfficeGold, "to_pandas_df") as mock_df:
            mock_df.return_value = old_offices.copy()

            result = loader.dedupe_offices(new_offices, pull_all=False, params={})

            # Should have 2 unique offices
            assert len(result) == 2

            # should have the earlier start date from old record
            test001_record = result[result["office_code"] == "TEST001"]
            assert test001_record["effective_start_date"].iloc[0] == pd.to_datetime("2020-01-01")

            # should be new
            test002_record = result[result["office_code"] == "TEST002"]
            assert not test002_record.empty

    def test_dedupe_offices_nightly_inactive_records(self):
        """Test deduplication logic for nightly load with inactive records"""
        loader = OfficeLoader()

        # New inactive office
        new_offices = pd.DataFrame(
            {
                "office_code": ["TEST001"],
                "office_name": ["Test Office 1"],
                "sub_tier_code": ["ABC"],
                "agency_code": ["012"],
                "effective_start_date": pd.to_datetime(["2020-01-01"]),
                "effective_end_date": pd.to_datetime(["2024-12-31"]),
                "contract_funding_office": [True],
                "contract_awards_office": [False],
                "financial_assistance_awards_office": [False],
                "financial_assistance_funding_office": [False],
            }
        )

        # Mock API response for historical records
        historical_response = [
            {
                "orglist": [
                    {
                        "aacofficecode": "TEST001",
                        "fhorgname": "Test Office 1",
                        "agencycode": "ABC",
                        "cgaclist": [{"cgac": "012"}],
                        "status": "INACTIVE",
                        "effectivestartdate": "2020-01-01 00:00",
                        "effectiveenddate": "2024-12-31 00:00",
                        "fhorgofficetypelist": [{"officetype": "Contract Funding"}],
                    }
                ]
            }
        ]

        with (
            patch.object(OfficeGold, "to_pandas_df") as mock_df,
            patch("brus_backend_common.scripts.loaders.office_gold.get_with_exception_hand") as mock_get,
        ):
            mock_df.return_value = pd.DataFrame()  # No existing offices
            mock_get.return_value = historical_response

            result = loader.dedupe_offices(new_offices, pull_all=False, params={"api_key": "test"})

            # Should have the inactive record
            assert len(result) == 1
            assert result["office_code"].iloc[0] == "TEST001"
            assert pd.notna(result["effective_end_date"].iloc[0])

    def test_dedupe_offices_nightly_mixed_active_inactive(self):
        """Test deduplication when same office code has both active and inactive records"""
        loader = OfficeLoader()

        # Same office code with both active and inactive records
        new_offices = pd.DataFrame(
            {
                "office_code": ["TEST001", "TEST001"],
                "office_name": ["Test Office 1", "Test Office 1"],
                "sub_tier_code": ["ABC", "ABC"],
                "agency_code": ["012", "012"],
                "effective_start_date": pd.to_datetime(["2024-01-01", "2020-01-01"]),
                "effective_end_date": [pd.NaT, pd.to_datetime("2023-12-31")],
                "contract_funding_office": [True, True],
                "contract_awards_office": [False, False],
                "financial_assistance_awards_office": [False, False],
                "financial_assistance_funding_office": [False, False],
            }
        )

        historical_response = [
            {
                "orglist": [
                    {
                        "aacofficecode": "TEST001",
                        "fhorgname": "Test Office 1",
                        "agencycode": "ABC",
                        "cgaclist": [{"cgac": "012"}],
                        "status": "INACTIVE",
                        "effectivestartdate": "2020-01-01 00:00",
                        "effectiveenddate": "2023-12-31 00:00",
                        "fhorgofficetypelist": [{"officetype": "Contract Funding"}],
                    }
                ]
            }
        ]

        with (
            patch.object(OfficeGold, "to_pandas_df") as mock_df,
            patch("brus_backend_common.scripts.loaders.office_gold.get_with_exception_hand") as mock_get,
        ):
            mock_df.return_value = pd.DataFrame()
            mock_get.return_value = historical_response

            result = loader.dedupe_offices(new_offices, pull_all=False, params={"api_key": "test"})

            # Active record should be removed from active list since inactive exists
            # Should only have 1 record after deduplication
            assert len(result) == 1
