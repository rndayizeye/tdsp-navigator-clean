"""
NYC Crashes Data Ingestion Pipeline
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    fetch_and_store_nyc_crashes,
    fetch_nyc_census_tracts_data,
    preprocess_census_geometry,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
        Create the NYC crashes data ingestion pipeline.
        Consists of two main nodes:
    1. fetch_and_store_nyc_crashes: Fetches crash data from the Socrata API, performs incremental updates based on the last fetch metadata, and stores the processed data.
    2. fetch_nyc_census_tracts_geometry: Fetches census tract geometry data from the Census API and stores it for later use in geospatial analyses.

        Returns:
            Pipeline for incremental data fetching from Socrata API
    """
    return pipeline(
        [
            node(
                func=fetch_and_store_nyc_crashes,
                inputs=[
                    "nyc_crashes_raw",
                    "metadata_raw",
                    "params:nyc_crashes",
                ],
                outputs=[
                    "nyc_crashes",
                    "fetch_metadata",  # Updated metadata with new watermark
                ],
                name="fetch_nyc_crashes_incremental_node",
                tags=["data_ingestion", "api", "incremental"],
            ),
            node(
                func=fetch_nyc_census_tracts_data,
                inputs=[
                    "params:nyc_census_tracts",
                ],
                outputs="nyc_census_raw",  # Raw census tracts data
                name="fetch_nyc_census_tracts_data_node",
                tags=["data_ingestion", "api", "census"],
            ),
            node(
                func=preprocess_census_geometry,
                inputs=["nyc_census_raw"],
                outputs="nyc_census_geodf",
                name="preprocess_add_geography_census_tracts_node",
                tags=["data_ingestion", "census", "preprocessing", "geography"],
            ),
        ],
        tags=["data_ingestion", "nyc_crashes", "census_tracts"],
    )
