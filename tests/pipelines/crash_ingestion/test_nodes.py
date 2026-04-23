"""
Unit tests for NYC Crashes data ingestion nodes.
Tests cover the three core transformation functions:
- _normalize_column_names
- _merge_and_deduplicate
- _build_metadata
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime

from tdsp_navigator.pipelines.data_ingestion.nodes import (
    _normalize_column_names,
    _merge_and_deduplicate,
    _build_metadata,
)


# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────

@pytest.fixture
def sample_csv_df():
    """Simulates a DataFrame loaded from the raw CSV (uppercase, spaced columns)."""
    return pd.DataFrame({
        "COLLISION_ID": [1, 2, 3],
        "CRASH DATE": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
        "CRASH TIME": ["08:00", "14:30", "23:15"],
        "BOROUGH": ["MANHATTAN", "BROOKLYN", "QUEENS"],
        "ZIP CODE": ["10001", "11201", "11368"],
        "LATITUDE": ["40.7128", "40.6782", "40.7282"],
        "LONGITUDE": ["-74.0060", "-73.9442", "-73.8948"],
        "NUMBER OF PERSONS INJURED": ["0", "1", "2"],
        "NUMBER OF PERSONS KILLED": ["0", "0", "1"],
        "NUMBER OF PEDESTRIANS INJURED": ["0", "1", "0"],
        "NUMBER OF PEDESTRIANS KILLED": ["0", "0", "1"],
        "NUMBER OF CYCLIST INJURED": ["0", "0", "0"],
        "NUMBER OF CYCLIST KILLED": ["0", "0", "0"],
        "NUMBER OF MOTORIST INJURED": ["0", "0", "2"],
        "NUMBER OF MOTORIST KILLED": ["0", "0", "0"],
        "CONTRIBUTING FACTOR VEHICLE 1": ["Driver Inattention", "Failure to Yield", "Speeding"],
        "ON STREET NAME": ["BROADWAY", "ATLANTIC AVE", "JUNCTION BLVD"],
        "VEHICLE TYPE CODE 1": ["Sedan", "SUV", "Taxi"],
        "VEHICLE TYPE CODE 2": [None, None, None],
        "LOCATION": [
            {"latitude": "40.7128", "longitude": "-74.0060"},
            {"latitude": "40.6782", "longitude": "-73.9442"},
            None,
        ],
    })


@pytest.fixture
def sample_api_df():
    """Simulates a DataFrame fetched from the Socrata API (snake_case columns)."""
    return pd.DataFrame({
        "collision_id": [3, 4, 5],  # collision_id 3 overlaps with CSV
        "crash_date": pd.to_datetime(["2024-01-03", "2024-01-04", "2024-01-05"]),
        "crash_time": ["23:15", "09:00", "17:45"],
        "borough": ["QUEENS", "BRONX", "STATEN ISLAND"],
        "zip_code": ["11368", "10451", "10301"],
        "latitude": [40.7282, 40.8448, 40.5795],
        "longitude": [-73.8948, -73.8648, -74.1502],
        "number_of_persons_injured": [2, 0, 3],
        "number_of_persons_killed": [1, 0, 0],
        "number_of_pedestrians_injured": [0, 0, 1],
        "number_of_pedestrians_killed": [1, 0, 0],
        "number_of_cyclist_injured": [0, 0, 0],
        "number_of_cyclist_killed": [0, 0, 0],
        "number_of_motorist_injured": [2, 0, 2],
        "number_of_motorist_killed": [0, 0, 0],
        "contributing_factor_vehicle_1": ["Speeding", "Driver Inattention", "Failure to Yield"],
        "on_street_name": ["JUNCTION BLVD", "GRAND CONCOURSE", "RICHMOND AVE"],
        "vehicle_type_code_1": ["Taxi", "Sedan", "SUV"],
        "vehicle_type_code_2": [None, None, None],
    })


# ─────────────────────────────────────────────────────────────
# _normalize_column_names
# ─────────────────────────────────────────────────────────────

class TestNormalizeColumnNames:

    def test_uppercased_columns_become_snake_case(self, sample_csv_df):
        result = _normalize_column_names(sample_csv_df)
        assert "collision_id" in result.columns
        assert "COLLISION_ID" not in result.columns

    def test_spaces_replaced_with_underscores(self, sample_csv_df):
        result = _normalize_column_names(sample_csv_df)
        assert "crash_date" in result.columns
        assert "CRASH DATE" not in result.columns

    def test_vehicle_code_columns_renamed(self, sample_csv_df):
        """vehicle_type_code1 -> vehicle_type_code_1 (adds underscore before number)"""
        df = pd.DataFrame({"VEHICLE TYPE CODE 1": ["Sedan"], "VEHICLE TYPE CODE 2": ["SUV"]})
        result = _normalize_column_names(df)
        assert "vehicle_type_code_1" in result.columns
        assert "vehicle_type_code_2" in result.columns

    def test_location_column_dropped(self, sample_csv_df):
        result = _normalize_column_names(sample_csv_df)
        assert "location" not in result.columns

    def test_zip_code_cast_to_string(self, sample_csv_df):
        result = _normalize_column_names(sample_csv_df)
        assert result["zip_code"].dtype == object  # string

    def test_latitude_longitude_cast_to_float(self, sample_csv_df):
        result = _normalize_column_names(sample_csv_df)
        assert pd.api.types.is_float_dtype(result["latitude"])
        assert pd.api.types.is_float_dtype(result["longitude"])

    def test_numeric_columns_cast(self, sample_csv_df):
        result = _normalize_column_names(sample_csv_df)
        for col in ["number_of_persons_injured", "number_of_persons_killed",
                    "number_of_pedestrians_injured", "number_of_pedestrians_killed"]:
            assert pd.api.types.is_numeric_dtype(result[col]), f"{col} should be numeric"

    def test_does_not_modify_original(self, sample_csv_df):
        original_cols = list(sample_csv_df.columns)
        _normalize_column_names(sample_csv_df)
        assert list(sample_csv_df.columns) == original_cols

    def test_already_normalized_df_unchanged(self, sample_api_df):
        """API data is already snake_case — normalization should be idempotent."""
        result = _normalize_column_names(sample_api_df)
        assert "collision_id" in result.columns
        assert "crash_date" in result.columns


# ─────────────────────────────────────────────────────────────
# _merge_and_deduplicate
# ─────────────────────────────────────────────────────────────

class TestMergeAndDeduplicate:

    def test_empty_existing_returns_new_data(self, sample_api_df):
        result = _merge_and_deduplicate(pd.DataFrame(), sample_api_df)
        assert len(result) == len(sample_api_df)

    def test_no_overlap_returns_all_rows(self, sample_csv_df, sample_api_df):
        # Remove overlapping collision_id=3 from csv
        csv = _normalize_column_names(sample_csv_df)
        csv = csv[csv["collision_id"] != 3]
        api = sample_api_df[sample_api_df["collision_id"] != 3]
        result = _merge_and_deduplicate(csv, api)
        assert len(result) == len(csv) + len(api)

    def test_duplicates_removed_by_collision_id(self, sample_api_df):
        """When collision_id=3 exists in both, result should have it only once."""
        csv = _normalize_column_names(
            pd.DataFrame({
                "collision_id": [3],
                "crash_date": pd.to_datetime(["2024-01-03"]),
                "borough": ["QUEENS"],
                "number_of_persons_killed": [1],
            })
        )
        result = _merge_and_deduplicate(csv, sample_api_df)
        assert result["collision_id"].duplicated().sum() == 0

    def test_newer_data_takes_precedence_on_duplicate(self):
        """When collision_id exists in both, keep=last (new data) should win."""
        existing = pd.DataFrame({
            "collision_id": [1],
            "crash_date": pd.to_datetime(["2024-01-01"]),
            "borough": ["OLD_VALUE"],
        })
        new = pd.DataFrame({
            "collision_id": [1],
            "crash_date": pd.to_datetime(["2024-01-01"]),
            "borough": ["NEW_VALUE"],
        })
        result = _merge_and_deduplicate(existing, new)
        assert result.loc[result["collision_id"] == 1, "borough"].iloc[0] == "NEW_VALUE"

    def test_result_sorted_by_crash_date(self, sample_api_df):
        csv = _normalize_column_names(
            pd.DataFrame({
                "collision_id": [10],
                "crash_date": pd.to_datetime(["2024-01-10"]),
            })
        )
        result = _merge_and_deduplicate(csv, sample_api_df)
        assert result["crash_date"].is_monotonic_increasing

    def test_result_has_reset_index(self, sample_api_df):
        csv = _normalize_column_names(
            pd.DataFrame({
                "collision_id": [10],
                "crash_date": pd.to_datetime(["2024-01-10"]),
            })
        )
        result = _merge_and_deduplicate(csv, sample_api_df)
        assert list(result.index) == list(range(len(result)))


# ─────────────────────────────────────────────────────────────
# _build_metadata
# ─────────────────────────────────────────────────────────────

class TestBuildMetadata:

    @pytest.fixture
    def new_df(self):
        return pd.DataFrame({
            "collision_id": [4, 5],
            "crash_date": pd.to_datetime(["2024-01-04", "2024-01-05"]),
        })

    @pytest.fixture
    def combined_df(self):
        return pd.DataFrame({
            "collision_id": [1, 2, 3, 4, 5],
            "crash_date": pd.to_datetime([
                "2024-01-01", "2024-01-02", "2024-01-03",
                "2024-01-04", "2024-01-05"
            ]),
        })

    def test_last_update_is_max_new_date(self, new_df, combined_df):
        result = _build_metadata(new_df, combined_df)
        assert result["last_update"] == "2024-01-05T00:00:00"

    def test_records_added_is_len_new_df(self, new_df, combined_df):
        result = _build_metadata(new_df, combined_df)
        assert result["records_added_this_run"] == 2

    def test_total_records_is_len_combined(self, new_df, combined_df):
        result = _build_metadata(new_df, combined_df)
        assert result["total_records_in_dataset"] == 5

    def test_date_range_min_max_correct(self, new_df, combined_df):
        result = _build_metadata(new_df, combined_df)
        assert "2024-01-01" in result["date_range"]["min"]
        assert "2024-01-05" in result["date_range"]["max"]

    def test_last_run_timestamp_is_recent(self, new_df, combined_df):
        before = datetime.now()
        result = _build_metadata(new_df, combined_df)
        after = datetime.now()
        ts = datetime.fromisoformat(result["last_run_timestamp"])
        assert before <= ts <= after

    def test_all_required_keys_present(self, new_df, combined_df):
        result = _build_metadata(new_df, combined_df)
        required_keys = {
            "last_update", "last_run_timestamp",
            "records_added_this_run", "total_records_in_dataset", "date_range"
        }
        assert required_keys.issubset(result.keys())
