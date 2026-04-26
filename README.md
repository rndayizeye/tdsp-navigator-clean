# TDSP Navigator — NYC Traffic Safety Analysis

A production-ready Kedro pipeline for incremental ingestion and analysis of NYC Motor Vehicle Collision data from the Socrata Open Data API, with a Vision Zero research focus.

## 🎯 Project Overview

This project implements a robust ETL pipeline for NYC crash data with:
- **Incremental loading** with watermark tracking
- **Dual-environment architecture** to avoid dependency conflicts
- **Dual-image Docker deployment** mirroring the two environments
- **Clean 28-column schema** with consistent snake_case naming
- **Vision Zero EDA notebook** in Marimo
- **Secured credentials** via `conf/local/` (gitignored)

## 📊 Data Source

- **Dataset**: [NYC Motor Vehicle Collisions - Crashes](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95)
- **Provider**: NYC Open Data (Socrata API)
- **Coverage**: July 2014 - Present
- **Update Frequency**: Daily
- **Current Size**: ~1.95M records, 28 columns

## 🏗️ Architecture

### Dual-Environment Strategy

The project uses two isolated environments to prevent dependency conflicts between Kedro Viz (requires Pydantic 1.x) and Marimo (requires Pydantic 2.x). These cannot coexist in the same environment.

#### Environment A: `tdsp_env` (Pipeline & Production)
- **Purpose**: Kedro pipelines, data ingestion, Kedro Viz
- **Kedro Version**: 0.19.15
- **Key Constraint**: Pydantic 1.x required by Kedro-Viz
- **Requirements**: `src/requirements_pipeline.txt`

#### Environment B: `marimo_env` (Analysis & EDA)
- **Purpose**: Interactive notebooks, visualization, modeling
- **Stack**: Modern Python (Pydantic 2.x, latest pandas, plotly)
- **Requirements**: `src/requirements_analysis.txt`

### Data Layers

```
data/
├── 01_raw/
│   ├── nyc_crashes.csv          # Raw CSV input
│   └── fetch_metadata.json      # Watermark tracker (input + output)
├── 02_primary/
│   └── nyc_crashes.parquet      # Processed output (28 columns, snake_case)
├── 03_model_input/
├── 04_model_output/
└── 06_reporting/
```

### Pipeline Flow

```
nyc_crashes_raw (CSV) ──┐
                         ├──► fetch_and_store_nyc_crashes ──► nyc_crashes (parquet)
metadata_raw (JSON)  ──┘                                  └──► fetch_metadata (JSON)
params:nyc_crashes ──┘
```

The pipeline fetches only records newer than the watermark, normalizes column names to snake_case, resolves type conflicts between CSV and API sources, deduplicates by `collision_id`, and updates the watermark.

## 🚀 Quick Start

### 1. Clone and Setup

```bash
git clone https://github.com/rndayizeye/tdsp-navigator.git
cd tdsp-navigator

# Create both conda environments
conda env create -f environment_tdsp.yml
conda env create -f environment_marimo.yml

# Initialize data directories
mkdir -p data/01_raw data/02_primary
echo '{}' > data/01_raw/fetch_metadata.json
```

### 2. Configure Credentials

Create `conf/local/credentials.yml` (this file is gitignored — never commit it):

```yaml
socrata_app_token: YOUR_APP_TOKEN_HERE
socrata_api_key_id: YOUR_KEY_ID_HERE
socrata_api_key_secret: YOUR_KEY_SECRET_HERE
```

Get a Socrata app token at: https://data.cityofnewyork.us/profile/edit/developer_settings

### 3. Run Data Ingestion

```bash
conda activate tdsp_env
kedro run

# Verify watermark updated
cat data/01_raw/fetch_metadata.json | python -m json.tool

# Verify output
python -c "import pandas as pd; df = pd.read_parquet('data/02_primary/nyc_crashes.parquet'); print(df.shape)"
```

### 4. Visualize Pipeline

```bash
conda activate tdsp_env
kedro viz --host 0.0.0.0 -p 4141
# Open: http://localhost:4141
```

### 5. Analyze Data

```bash
conda activate marimo_env
marimo edit notebooks/analysis.py
# Open: http://localhost:2718
```

## 📋 Configuration

### API Configuration

`conf/base/parameters.yml`:

```yaml
nyc_crashes:
  initial_date: "2014-01-01"
  socrata:
    dataset_id: "h9gi-nx95"
    domain: "data.cityofnewyork.us"
  incremental:
    date_column: "crash_date"
```

The `app_token` is loaded at runtime from `conf/local/credentials.yml` — it is never stored in `parameters.yml`.

## 🔧 Project Structure

```
tdsp-navigator/
├── conf/
│   ├── base/
│   │   ├── catalog.yml                  # Data catalog
│   │   ├── parameters.yml               # API & pipeline config (no secrets)
│   │   └── logging.yml
│   └── local/
│       └── credentials.yml              # Secrets (gitignored)
├── src/
│   └── tdsp_navigator/
│       ├── pipelines/
│       │   └── data_ingestion/
│       │       ├── nodes.py             # Pipeline logic + schema normalization
│       │       └── pipeline.py          # Pipeline definition
│       ├── requirements_pipeline.txt    # tdsp_env dependencies
│       └── requirements_analysis.txt   # marimo_env dependencies
├── notebooks/
│   └── analysis.py                      # Vision Zero EDA (Marimo)
├── tests/
│   └── pipelines/
│       └── data_ingestion/
│           └── test_nodes.py            # Unit tests
├── data/                                # Data layers (gitignored)
├── Dockerfile.pipeline                  # Kedro + Kedro Viz image
├── Dockerfile.notebook                  # Marimo analysis image
└── docker-compose.yaml
```

## 🐳 Docker Deployment

The project uses two Docker images mirroring the dual-environment strategy.

### Build

```bash
docker compose build
```

### Run Kedro Viz (pipeline image)

```bash
docker compose up pipeline
# Open: http://localhost:4141
```

### Run Marimo notebook (analysis image)

```bash
docker compose up notebook
# Open: http://localhost:8080
```

### Run pipeline (on-demand)

```bash
docker compose --profile pipeline up pipeline-runner
```

### Credential Handling in Docker

`conf/local/` is excluded from the Docker image via `.dockerignore`. Mount credentials at runtime:

```bash
docker compose run -v $(pwd)/conf/local:/app/conf/local pipeline kedro run
```

## 📊 Data Schema

The processed parquet output has 28 columns, all in snake_case:

| Column | Type | Description |
|--------|------|-------------|
| `collision_id` | int64 | Unique crash identifier |
| `crash_date` | datetime | Date of crash |
| `crash_time` | string | Time of crash |
| `borough` | string | NYC borough |
| `zip_code` | string | ZIP code (kept as string) |
| `latitude` | float64 | Crash latitude |
| `longitude` | float64 | Crash longitude |
| `on_street_name` | string | Primary street |
| `cross_street_name` | string | Cross street |
| `off_street_name` | string | Off-street location |
| `number_of_persons_injured` | float64 | Total persons injured |
| `number_of_persons_killed` | float64 | Total persons killed |
| `number_of_pedestrians_injured` | float64 | Pedestrians injured |
| `number_of_pedestrians_killed` | float64 | Pedestrians killed |
| `number_of_cyclist_injured` | float64 | Cyclists injured |
| `number_of_cyclist_killed` | float64 | Cyclists killed |
| `number_of_motorist_injured` | float64 | Motorists injured |
| `number_of_motorist_killed` | float64 | Motorists killed |
| `contributing_factor_vehicle_1–5` | string | Contributing factors |
| `vehicle_type_code_1–5` | string | Vehicle types |

## 📈 Incremental Loading

The pipeline uses watermark-based incremental loading via `data/01_raw/fetch_metadata.json`:

```json
{
  "last_update": "2026-04-19T00:00:00",
  "last_run_timestamp": "2026-04-22T21:09:50",
  "records_added_this_run": 142,
  "total_records_in_dataset": 1952040,
  "date_range": {
    "min": "2012-07-27T00:00:00",
    "max": "2026-04-19T00:00:00"
  }
}
```

Each run only fetches records newer than `last_update`.

## 🔬 Research Focus

The Marimo notebook (`notebooks/analysis.py`) covers three areas with a Vision Zero policy lens:

1. **Temporal patterns** — crashes by hour, day of week, and annual fatality trends since Vision Zero launched in 2014
2. **Geospatial patterns** — borough-level fatality rates, vulnerable road user deaths, and an interactive fatal crash map
3. **Severity analysis** — contributing factors in fatal crashes, fatality rates by road user type, and Vision Zero progress tracking

## ⚠️ Critical Notes

### Environment Stability

**DO NOT** upgrade these packages in `tdsp_env`:

| Package | Pinned Version | Reason |
|---------|---------------|--------|
| `kedro-viz` | 6.7.0 | API compatibility |
| `secure` | 0.3.0 | `AttributeError: 'Secure' object has no attribute 'framework'` |
| `pydantic` | <2.0 | Kedro-Viz requires 1.x |
| `starlette` | <0.28.0 | Kedro-Viz API compatibility |
| `strawberry-graphql` | <0.235.0 | `GraphQL.__init__() got unexpected keyword argument 'graphiql'` |

### Troubleshooting

| Error | Fix |
|-------|-----|
| `'Secure' object has no attribute 'framework'` | `pip install secure==0.3.0` |
| `GraphQL.__init__() got unexpected keyword argument 'graphiql'` | `pip install "strawberry-graphql<0.235.0"` |
| `ModuleNotFoundError: No module named 'toposort'` | `pip install toposort` |
| `UnsupportedInterpolationType: credentials` | Remove `app_token` from `parameters.yml` — load from credentials instead |

### Running Tests

```bash
conda activate tdsp_env
pip install pytest-cov
pytest tests/pipelines/data_ingestion/test_nodes.py -v
```

## 📚 Additional Resources

- [Kedro Documentation](https://docs.kedro.org/)
- [NYC Open Data Portal](https://opendata.cityofnewyork.us/)
- [Socrata API Docs](https://dev.socrata.com/)
- [Marimo Documentation](https://docs.marimo.io/)
- [Vision Zero NYC](https://www.nyc.gov/content/visionzero/pages/)

## 📝 Project Status

**Status**: Production Ready ✅  
**Last Updated**: April 2026  
**Kedro Version**: 0.19.15  
**Python Version**: 3.11

## 👥 Authors

- **Remyn Dayizeye** - [@rndayizeye](https://github.com/rndayizeye)

## 📄 License

MIT License - see LICENSE file for details

---

For troubleshooting and lessons learned, see:
- [Environment Stabilization Guide](docs/environment_stabilization_guide.md)
- [Kedro Data Loading Lessons](docs/kedro_data_loading_lessons.md)