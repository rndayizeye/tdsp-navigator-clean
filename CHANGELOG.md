# Changelog

All notable changes to this project will be documented in this file.

## [1.0.0] - 2026-04-21

### Added
- Initial production release
- Incremental data ingestion from Socrata API
- Dual-environment setup (pipeline + analysis)
- Watermark-based incremental loading
- Comprehensive error handling and logging
- Docker support for deployment
- Kedro Viz integration

### Fixed
- Dependency conflicts between Kedro and modern packages
- API token parameter handling
- Metadata persistence across runs

### Documentation
- Environment stabilization guide
- Data loading lessons learned
- Comprehensive README

## [1.1.0] - 2026-04-28

### Added
- H3 hexagonal binning (resolution 9, ~175m) for spatial hotspot analysis
- Corridor cluster vs. isolated hotspot classification using H3 neighbor counting
- Side-by-side H3 hotspot map for poster and notebook
- Deadliest streets analysis with highway/surface street segmentation
- Borough/street type dropdown filters with reactive Marimo cells
- Policy recommendation engine by contributing factor and road user type
- Vision Zero submission notebook (vision_zero_submission.py)
- NSDC research poster (48"x36" horizontal, python-pptx)
- Dual Dockerfile architecture (Dockerfile.pipeline + Dockerfile.notebook)
- Split requirements files (requirements_pipeline.txt + requirements_analysis.txt)
- Census tract geometry pipeline (TIGER/Line shapefiles + ACS5 API)

### Fixed
- Watermark input/output file mismatch causing full re-fetch on every run
- Column doubling (58 columns) from CSV/API schema mismatch — fixed via _normalize_column_names
- Catalog outputs not persisted to disk (MemoryDataset fallback)
- Module-level credential loading removed from nodes.py
- Choropleth mapbox GeoJSON feature ID mismatch
- CRS mismatch (EPSG:32617 → EPSG:4326) for Mapbox rendering
- Fatality rate calculation corrected to use all crashes as denominator

### Security
- API credentials removed from parameters.yml and conf/base/
- conf/local/ added to .gitignore and .dockerignore
- Credentials loaded at runtime via globals.yml resolver

### Documentation
- README fully updated with dual-environment architecture, data schema, Docker setup
- Prepared LinkedIn post, About section, experience entry, and skills guide
