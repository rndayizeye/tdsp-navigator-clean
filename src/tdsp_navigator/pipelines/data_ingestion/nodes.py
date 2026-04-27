"""
NYC Crashes Data Ingestion Nodes
"""

from urllib import response

import pandas as pd
import logging
from datetime import datetime
from typing import Dict, Tuple, Any
from sodapy import Socrata
import geopandas as gpd
import requests

logger = logging.getLogger(__name__)


def fetch_and_store_nyc_crashes(
    nyc_crashes: pd.DataFrame,
    metadata_raw: Dict,
    params: Dict,
) -> Tuple[pd.DataFrame, Dict]:
    """
    Incrementally fetch NYC crash data from Socrata API.

    Args:
        nyc_crashes: Historical crash data (may be empty on first run)
        metadata_raw: Tracking metadata with 'last_update' watermark
        params: Configuration parameters from nyc_crashes.yml

    Returns:
        Tuple of (updated DataFrame, updated metadata)
    """
    app_token = params["socrata"]["app_token"]

    # Extract parameters
    socrata_config = params.get("socrata")

    if not socrata_config:
        raise ValueError("socrata configuration not found in params!")
    socrata = params["socrata"]

    incremental_config = params["incremental"]
    initial_date = params.get("initial_date")
    if not initial_date:
        raise ValueError("initial_date not found in the passed parameters!")
    domain = params["socrata"]["domain"]
    # Normalize inputs
    existing_data = _normalize_existing_data(nyc_crashes)
    metadata = _normalize_metadata(metadata_raw)

    # Determine watermark
    start_date = metadata.get("last_update", params["initial_date"])
    logger.info(f"Fetching data from Socrata. Watermark: {start_date}")

    # Fetch new data from API
    new_df = _fetch_from_socrata(
        base_url=socrata_config.get("domain"),  # Note: your YAML uses 'domain'
        dataset_id=socrata_config.get("dataset_id"),
        start_date=start_date,
        chunk_size=socrata_config.get(
            "chunk_size", 5000000
        ),  # Adding a default just in case
        app_token=app_token,
    )

    # Early return if no new data
    if new_df.empty:
        logger.info("No new records found. Data is up-to-date.")
        return existing_data, metadata

    # Merge and deduplicate
    combined_df = _merge_and_deduplicate(existing_data, new_df)

    # Update metadata
    new_metadata = _build_metadata(new_df, combined_df)

    logger.info(
        f"Successfully processed {len(new_df)} new records. "
        f"Total records: {len(combined_df)}. "
        f"New watermark: {new_metadata['last_update']}"
    )

    return combined_df, new_metadata


def _normalize_existing_data(existing_data: Any) -> pd.DataFrame:
    """Convert existing_data to a DataFrame, handling None/empty cases."""
    if existing_data is None or not isinstance(existing_data, pd.DataFrame):
        logger.info("Existing data is None or not a DataFrame. Initializing empty.")
        return pd.DataFrame()

    if existing_data.empty:
        logger.info("Existing data is empty. Starting fresh.")
        return pd.DataFrame()
    # Normalize column names to match API output to avoid duplicating columns after merge
    existing_data = _normalize_column_names(existing_data)

    # Ensure crash_date is datetime if it exists
    if "crash_date" in existing_data.columns:
        existing_data = existing_data.copy()
        existing_data["crash_date"] = pd.to_datetime(
            existing_data["crash_date"], errors="coerce"
        )

    return existing_data


def _normalize_metadata(metadata: Any) -> Dict:
    """Ensure metadata is a valid dictionary."""
    if not metadata or not isinstance(metadata, dict):
        logger.info("Metadata is empty or invalid. Initializing empty dict.")
        return {}
    return metadata


def _fetch_from_socrata(
    base_url: str,
    dataset_id: str,
    start_date: str,
    chunk_size: int,
    app_token=None,
    **kwargs,
) -> pd.DataFrame:
    """
    Fetch records from Socrata API newer than start_date.

    Args:
        base_url: Socrata API base URL
        dataset_id: Dataset identifier
        start_date: ISO format date string for filtering
        chunk_size: Maximum records to fetch

    Returns:
        DataFrame with new records, or empty DataFrame if fetch fails/no data
    """
    client = Socrata(base_url, app_token)

    try:
        logger.info(f"Querying Socrata API: {dataset_id}")
        results = client.get(
            dataset_id,
            where=f"crash_date > '{start_date}'",
            order="crash_date ASC",
            limit=chunk_size,
        )

        if not results:
            logger.info("API returned no results.")
            return pd.DataFrame()

        new_df = pd.DataFrame.from_records(results)
        new_df = _normalize_column_names(new_df)
        logger.info(f"Received {len(new_df)} records from API")

        # Validate required columns
        if "crash_date" not in new_df.columns:
            logger.error("Missing 'crash_date' column in API response")
            raise ValueError("API response missing required column: crash_date")

        if "collision_id" not in new_df.columns:
            logger.error("Missing 'collision_id' column in API response")
            raise ValueError("API response missing required column: collision_id")

        # Parse crash_date
        new_df["crash_date"] = pd.to_datetime(new_df["crash_date"], errors="coerce")

        # Drop rows with invalid dates
        invalid_dates = new_df["crash_date"].isna().sum()
        if invalid_dates > 0:
            logger.warning(f"Dropping {invalid_dates} records with invalid crash_date")
            new_df = new_df.dropna(subset=["crash_date"])

        if new_df.empty:
            logger.warning("All fetched records had invalid dates")
            return pd.DataFrame()

        logger.info(f"Successfully validated {len(new_df)} records")
        return new_df

    except Exception as e:
        logger.error(f"Failed to fetch data from Socrata: {e}", exc_info=True)
        raise
    finally:
        client.close()


def _merge_and_deduplicate(
    existing_data: pd.DataFrame,
    new_data: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge new data with existing, removing duplicates by collision_id.

    Args:
        existing_data: Historical data
        new_data: Newly fetched data

    Returns:
        Deduplicated combined DataFrame sorted by crash_date
    """
    if existing_data.empty:
        logger.info("No existing data. Using new data as-is.")
        return new_data.sort_values("crash_date").reset_index(drop=True)

    # Concatenate datasets
    combined = pd.concat([existing_data, new_data], ignore_index=True)

    initial_count = len(combined)

    # Deduplicate - keep last occurrence (newer data takes precedence)
    combined = combined.drop_duplicates(subset="collision_id", keep="last")

    duplicates_removed = initial_count - len(combined)

    # Sort by crash_date for consistency
    combined = combined.sort_values("crash_date").reset_index(drop=True)

    logger.info(
        f"Merged data: {len(existing_data)} existing + {len(new_data)} new "
        f"= {len(combined)} total ({duplicates_removed} duplicates removed)"
    )

    return combined


def _build_metadata(new_df: pd.DataFrame, combined_df: pd.DataFrame) -> Dict:
    """
    Build metadata dictionary with updated watermark and statistics.

    Args:
        new_df: Newly fetched data
        combined_df: Complete merged dataset

    Returns:
        Metadata dictionary
    """
    # Use the maximum crash_date from new data as watermark
    new_last_date = new_df["crash_date"].max()

    metadata = {
        "last_update": new_last_date.strftime("%Y-%m-%dT%H:%M:%S"),
        "last_run_timestamp": datetime.now().isoformat(),
        "records_added_this_run": len(new_df),
        "total_records_in_dataset": len(combined_df),
        "date_range": {
            "min": combined_df["crash_date"].min().isoformat(),
            "max": combined_df["crash_date"].max().isoformat(),
        },
    }

    return metadata


def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to snake_case to match Socrata API output."""
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_", regex=False)
    # Fix the two vehicle code columns that differ between source and API
    rename_map = {
        "vehicle_type_code1": "vehicle_type_code_1",
        "vehicle_type_code2": "vehicle_type_code_2",
    }
    df = df.rename(columns=rename_map)
    if "zip_code" in df.columns:
        df["zip_code"] = df["zip_code"].astype(str).replace("nan", None)

    for col in ["latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    # Drop location column — it's a dict from API, string from CSV, redundant either way
    if "location" in df.columns:
        df = df.drop(columns=["location"])

    # Cast all count/numeric columns that come in as strings from CSV
    numeric_cols = [
        "collision_id",
        "number_of_persons_injured",
        "number_of_persons_killed",
        "number_of_pedestrians_injured",
        "number_of_pedestrians_killed",
        "number_of_cyclist_injured",
        "number_of_cyclist_killed",
        "number_of_motorist_injured",
        "number_of_motorist_killed",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# -----------------------------------------------------------
# Node to fetch census tracts geometry data from Census API
# -----------------------------------------------------------


def fetch_nyc_census_tracts_data(
    params: Dict,
) -> pd.DataFrame:
    """
    Fetch NYC census tracts data from Socrata API.

    Args:
        params: Configuration parameters from nyc_crashes.yml
    Returns:
        GeoDataFrame with census tracts geometry
    """

    # Extract parameters
    request_params = params["request_params"]
    get = request_params["get"]
    geography = request_params["geography"]
    base_url = request_params["base_url"]
    api_key = params["api_key"]
    state_fips = request_params["state_fips"]
    county_fips = request_params["county_fips"]

    url = (
        f"{base_url}"
        f"?get={get}"
        f"&for={geography}:*"
        f"&in=state:{state_fips}"
        f"&in=county:{county_fips}"
        f"&key={api_key}"
    )

    logger.info(
        f"Fetching census tracts geometry from API: {url.replace(api_key, '***')}"
    )

    try:
        response = requests.get(url)
        logger.info(
            f"Fetching census tracts geometry from API: {get} from {base_url} "
            f"| geography: {geography} | state: {state_fips}"
            f"| county: {county_fips} | status: {response.status_code}"
        )

        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes
        data = response.json()

        # The first row contains column names
        columns = data[0]
        records = data[1:]

        df = pd.DataFrame(records, columns=columns)

        # Convert GEO_ID to string and extract tract number
        df["GEO_ID"] = df["GEO_ID"].astype(str)
        df["tract"] = df["GEO_ID"].str[-6:]

        logger.info(f"Successfully fetched and processed {len(df)} census tracts")
        df = _rename_census_columns(df)

        # Convert numeric columns to appropriate types
        df = _convert_numeric_columns(df)
        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch census tracts geometry: {e}", exc_info=True)
        raise


# Helper function to rename census columns for consistency


def _rename_census_columns(df):
    """Rename columns in the census tracts GeoDataFrame for consistency."""
    rename_map = {
        "NAME": "name",
        "GEO_ID": "geo_id",
        "B01003_001E": "population_total",
        "B17001_001E": "poverty_total",
        "B17001_002E": "poverty_below_threshold",
    }
    df = df.rename(columns=rename_map)

    # convert numeric columns to appropriate types
    df = _convert_numeric_columns(df)

    # create poverty rate column
    if "poverty_total" in df.columns and "population_total" in df.columns:
        df["poverty_rate"] = df["poverty_total"] / df["population_total"]
    
    # Extract the numeric part of GEO_ID for merging with geometry data 
    df['geo_id'] = df['geo_id'].str.split("US", expand=True)[1]


    return df


# Helper function to convert numeric columns
def _convert_numeric_columns(df):
    """Convert specified columns to numeric, coercing errors to NaN."""
    numeric_columns = ["population_total", "poverty_total", "poverty_below_threshold"]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# Node to download census tracts geometry data from Census API and
# preprocess it for geospatial analyses
# ----------------------------------------------------------------.


def preprocess_census_geometry(df: pd.DataFrame,
                                geometry_df: gpd.GeoDataFrame,) -> gpd.GeoDataFrame:
    """takes the raw acs5 census tract data, add merge it with polygon data
    and convert to a GeoDataFrame"""
    # Assuming df has 'GEO_ID' and 'geometry' columns
    if "geo_id" not in df.columns:
        logger.error(f"Input DataFrame must contain 'geo_id' columns. Available columns: {list(df.columns)}")
        raise ValueError("Missing required columns in input DataFrame")
    if "GEO_ID" not in geometry_df.columns or "geometry" not in geometry_df.columns:
        logger.error(f"Geometry DataFrame must contain 'GEO_ID' and 'geometry' columns. Available columns: {list(geometry_df.columns)}")
        raise ValueError("Missing required columns in geometry DataFrame")
    # Merge the geometry data with the main DataFrame
    gdf = _merge_census_geometry(df, geometry_df)

    logger.info(
        f"Preprocessed census geometry data into GeoDataFrame with {len(gdf)} records"
    )

    return gdf


# helper to merge census geometry with main census data
def _merge_census_geometry(
    census_df: pd.DataFrame, geometry_df: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """Merge the census data with geometry data on GEO_ID and return a GeoDataFrame."""
    if "geo_id" not in census_df.columns or "GEO_ID" not in geometry_df.columns:
        logger.error("Both DataFrames must contain 'GEO_ID' column for merging")
        raise ValueError("Missing 'GEO_ID' column in one of the DataFrames")

    merged_gdf = census_df.merge(geometry_df, left_on="geo_id", right_on="GEO_ID", how="left")
    merged_gdf = gpd.GeoDataFrame(merged_gdf, geometry="geometry")


    logger.info(
        f"Merged census data with geometry. Resulting GeoDataFrame has {len(merged_gdf)} records.crs: {merged_gdf.crs}"
    )

    return merged_gdf

#----------------------------------------------------------------------------
# Node to fetch census tracts geometry data from Census API
#----------------------------------------------------------------------------

# Helper function to fetch geometry data for census tracts
def fetch_census_geometry(params: dict) -> gpd.GeoDataFrame:
    """Fetch geometry data for given GEO_IDs from Census TIGER/Line shapefiles or API."""
    # Fetch the geometry data from the Census TIGER/Line shapefiles or an appropriate API endpoint, and return a GeoDataFrame with 'GEO_ID' and 'geometry' columns.
    #get the url for the shapefile from the parameters
    
    url = params["census_geometry_url"]
    # Access shapefile of NYC recent census tracts  shapefile
    gdf = gpd.read_file(url)

    # Reproject shapefile to UTM Zone 17N
    # https://spatialreference.org/ref/epsg/wgs-84-utm-zone-17n/
    gdf = gdf.to_crs(epsg=32617)

    # Print GeoDataFrame of shapefile
    print(gdf.tail(2))
    print("Shape: ", gdf.shape)

    # Check shapefile projection
    logger.info(f"The shapefile projection is: {gdf.crs}")

    # rename GEOID column to GEO_ID and BoroCode to borough to match the main census data for merging
    gdf = gdf.rename(
        columns={"GEOID": "GEO_ID", "BoroCode": "borough"}
    )

    return gdf[["GEO_ID", "geometry"]]