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