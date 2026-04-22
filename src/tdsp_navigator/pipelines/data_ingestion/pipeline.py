"""
NYC Crashes Data Ingestion Pipeline
"""
from kedro.pipeline import Pipeline, node, pipeline
from .nodes import fetch_and_store_nyc_crashes


def create_pipeline(**kwargs) -> Pipeline:
    """
    Create the NYC crashes data ingestion pipeline.
    
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
                name="fetch_nyc_crashes_incremental",
                tags=["data_ingestion", "api", "incremental"],
            ),
        ],
        tags=["data_ingestion"],
    )