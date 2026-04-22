# TDSP Navigator - NYC Traffic Safety Analysis

A production-ready Kedro pipeline for incremental ingestion and analysis of NYC Motor Vehicle Collision data from the Socrata Open Data API.

## 🎯 Project Overview

This project implements a robust ETL pipeline for NYC crash data with:
- **Incremental loading** with watermark tracking
- **Dual-environment architecture** to avoid dependency conflicts
- **Production-ready** configuration with pinned dependencies
- **Geospatial analysis** capabilities

## 📊 Data Source

- **Dataset**: [NYC Motor Vehicle Collisions - Crashes](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95)
- **Provider**: NYC Open Data (Socrata API)
- **Coverage**: July 2012 - Present
- **Update Frequency**: Daily

## 🏗️ Architecture

### Dual-Environment Strategy

The project uses two isolated environments to prevent dependency conflicts:

#### Environment A: `tdsp_env` (Pipeline & Production)
- **Purpose**: Kedro pipelines, data ingestion, Kedro Viz
- **Kedro Version**: 0.19.15 (LTS)
- **Key Constraint**: Pydantic 1.x required by Kedro-Viz

#### Environment B: `marimo_env` (Analysis & EDA)
- **Purpose**: Interactive notebooks, visualization, modeling
- **Stack**: Modern Python (Pydantic 2.x, latest pandas, plotly)

### Data Layers

```
data/
├── 01_raw/              # API metadata, watermarks
│   └── fetch_metadata.json
├── 02_primary/          # Main datasets (versioned)
│   └── nyc_crashes.parquet
├── 03_model_input/      # Feature engineering outputs
├── 04_model_output/     # Model predictions
└── 06_reporting/        # Analysis outputs
```

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

### 2. Run Data Ingestion

```bash
# Activate pipeline environment
conda activate tdsp_env

# Run the pipeline
kedro run

# Verify ingestion
cat data/01_raw/fetch_metadata.json | python -m json.tool
ls -lh data/02_primary/nyc_crashes.parquet
```

### 3. Visualize Pipeline

```bash
# Launch Kedro Viz
kedro viz --host 0.0.0.0 -p 4141

# Open browser to: http://localhost:4141
```

### 4. Analyze Data

```bash
# Switch to analysis environment
conda activate marimo_env

# Launch interactive notebook
marimo edit notebooks/analysis.py
```

## 📋 Configuration

### API Configuration

Edit `conf/base/parameters/nyc_crashes.yml`:

```yaml
socrata:
  base_url: "data.cityofnewyork.us"
  dataset_id: "h9gi-nx95"
  chunk_size: 50000
  # Optional: Add app token for higher rate limits
  # app_token: "YOUR_TOKEN_HERE"

incremental:
  initial_date: "2012-07-01T00:00:00"
```

### Get Socrata App Token (Optional)

For higher API rate limits:
1. Visit: https://data.cityofnewyork.us/profile/edit/developer_settings
2. Create an app token
3. Add to `conf/base/parameters/nyc_crashes.yml`

## 🔧 Development

### Project Structure

```
tdsp-navigator/
├── conf/
│   └── base/
│       ├── catalog.yml           # Data catalog definitions
│       ├── parameters/
│       │   └── nyc_crashes.yml   # API & pipeline config
│       └── logging.yml
├── src/
│   └── tdsp_navigator/
│       ├── pipelines/
│       │   └── data_ingestion/
│       │       ├── nodes.py      # Pipeline logic
│       │       └── pipeline.py   # Pipeline definition
│       └── pipeline_registry.py
├── data/                         # Data layers (gitignored)
├── notebooks/                    # Analysis notebooks
├── environment_tdsp.yml          # Pipeline environment
├── environment_marimo.yml        # Analysis environment
└── pyproject.toml
```

### Running Tests

```bash
conda activate tdsp_env
pytest src/tests/
```

### Adding New Pipelines

```bash
kedro pipeline create <pipeline_name>
```

## ⚠️ Critical Notes

### Environment Stability

**DO NOT** upgrade these packages in `tdsp_env`:
- `secure==0.3.0` (newer versions break Kedro framework)
- `pydantic<2.0` (Kedro-Viz requires 1.x)
- `kedro-viz==6.7.0`
- `strawberry-graphql<0.235.0`

### Troubleshooting

#### Error: `'Secure' object has no attribute 'framework'`
```bash
conda activate tdsp_env
pip uninstall secure
pip install secure==0.3.0
```

#### Error: `GraphQL.__init__() got unexpected keyword argument 'graphiql'`
```bash
pip install "strawberry-graphql<0.235.0"
```

#### Verify Environment Integrity
```bash
conda activate tdsp_env
python -c "import kedro; print(f'Kedro: {kedro.__version__}')"
python -c "import pydantic; print(f'Pydantic: {pydantic.VERSION}')"
python -c "import secure; print(f'Secure: {secure.__version__}')"
```

## 📊 Data Pipeline Details

### Incremental Loading Strategy

The pipeline uses watermark-based incremental loading:

1. **Load Watermark**: Read last update timestamp from metadata
2. **Fetch New Data**: Query Socrata API for records > watermark
3. **Merge & Deduplicate**: Combine with existing data, remove duplicates
4. **Update Watermark**: Save new max timestamp to metadata

### Node Implementation

Key nodes in `src/tdsp_navigator/pipelines/data_ingestion/nodes.py`:
- `fetch_and_store_nyc_crashes`: Main orchestration
- `_fetch_from_socrata`: API interaction with retry logic
- `_merge_and_deduplicate`: Data consolidation
- `_build_metadata`: Watermark management

## 📈 Usage Examples

### Check Last Ingestion

```bash
cat data/01_raw/fetch_metadata.json | python -m json.tool
```

Example output:
```json
{
  "last_update": "2026-04-21T18:30:45",
  "last_run_timestamp": "2026-04-21T19:00:12.345678",
  "records_added_this_run": 1523,
  "total_records_in_dataset": 2089456,
  "date_range": {
    "min": "2012-07-01T00:00:00",
    "max": "2026-04-21T18:30:45"
  }
}
```

### Run Specific Node

```bash
kedro run --node=fetch_nyc_crashes_incremental
```

### Run with Different Environment

```bash
kedro run --env=production
```

## 🐳 Docker Deployment

Build production image:

```bash
docker build -f Dockerfile.production -t tdsp-navigator:latest .
```

Run pipeline:

```bash
docker run -v $(pwd)/data:/app/data tdsp-navigator:latest
```

## 📚 Additional Resources

- [Kedro Documentation](https://docs.kedro.org/)
- [NYC Open Data Portal](https://opendata.cityofnewyork.us/)
- [Socrata API Docs](https://dev.socrata.com/)
- [Marimo Documentation](https://docs.marimo.io/)

## 🤝 Contributing

1. Create a feature branch: `git checkout -b feature/amazing-feature`
2. Commit changes: `git commit -m 'Add amazing feature'`
3. Push to branch: `git push origin feature/amazing-feature`
4. Open a Pull Request

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

## 📄 License

MIT License - see LICENSE file for details

## 👥 Authors

- **Remyn Dayizeye** - Initial work - [@rndayizeye](https://github.com/rndayizeye)

## 🙏 Acknowledgments

- NYC Open Data team for maintaining the collision dataset
- Kedro community for the excellent framework
- Lessons learned documented in `environment_stabilization_guide.md`

## 📝 Project Status

**Status**: Production Ready ✅  
**Last Updated**: April 2026  
**Kedro Version**: 0.19.15  
**Python Version**: 3.11

---

For troubleshooting and lessons learned, see:
- [Environment Stabilization Guide](docs/environment_stabilization_guide.md)
- [Kedro Data Loading Lessons](docs/kedro_data_loading_lessons.md)