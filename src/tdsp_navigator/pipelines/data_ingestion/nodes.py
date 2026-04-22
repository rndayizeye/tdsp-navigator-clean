"""
NYC Crashes Data Ingestion Nodes
"""
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, Tuple, Any
from sodapy import Socrata
from kedro.config import OmegaConfigLoader
from pathlib import Path

conf_loader = OmegaConfigLoader(conf_source=str(Path.cwd() / "conf"))
credentials = conf_loader["credentials"]
app_token = credentials.get("socrata_app_token")

app_token=app_token

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
    print("DEBUG PARAMS:", params)
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
        chunk_size=socrata_config.get("chunk_size", 5000000), # Adding a default just in case
        app_token=socrata_config.get("app_token")
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
            existing_data["crash_date"], 
            errors="coerce"
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
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )
    # Fix the two vehicle code columns that differ between source and API
    rename_map = {
        "vehicle_type_code1": "vehicle_type_code_1",
        "vehicle_type_code2": "vehicle_type_code_2",
    }
    df = df.rename(columns=rename_map)
    if "zip_code" in df.columns:
        df["zip_code"] = df["zip_code"].astype(str).replace("nan", None)

    for col in ["latitude", "longitude"]:
        if col in df.columns:df[col] = pd.to_numeric(df[col], errors="coerce")
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