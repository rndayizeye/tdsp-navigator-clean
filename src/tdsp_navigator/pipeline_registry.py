"""Project pipelines."""
from typing import Dict
from kedro.pipeline import Pipeline
from tdsp_navigator.pipelines import data_ingestion


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    data_ingestion_pipeline = data_ingestion.create_pipeline()
    
    return {
        "data_ingestion": data_ingestion_pipeline,
        "__default__": data_ingestion_pipeline,
 }