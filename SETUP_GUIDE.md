# Atlan Data Extractor - Setup Guide

## Quick Start

1. **Configure your Atlan instance:**
   - Edit `configs/config.json`
   - Replace `https://your-org.atlan.com` with your actual Atlan URL
   - Set your authentication token in the `ATLAN_AUTH_TOKEN` environment variable

2. **Run the extractor:**
   ```bash
   python main.py
   ```

3. **Check outputs:**
   - `output/connections.csv` - All connections data
   - `output/databases.csv` - All databases data
   - `atlan_extractor.log` - Detailed execution logs

## Configuration

The extractor uses string replacement for the `PLACEHOLDER_TO_BE_REPLACED` field in database queries, allowing flexible connection-specific filtering.

### Directory Structure
```
├── configs/
│   ├── config.json          # Main configuration
│   └── config_template.json # Clean template
├── output/                  # Generated CSV files
├── main.py                  # Core extractor
└── test_atlan_extractor.py  # Test suite (17 tests, 91% coverage)
```

## Network Requirements

Your Atlan instance must allow API access from external IPs. If you encounter IP restriction errors, contact your IT administrator to allowlist the deployment environment.

## Authentication

The extractor prioritizes environment variables over config file settings:
1. `ATLAN_AUTH_TOKEN` environment variable (recommended)
2. `auth_token` field in config.json

## Supported Connection Types

The extractor handles multiple connector types with specific API mappings:
- databricks, oracle, snowflake, teradata → databases_api
- alteryx, api, app, file, mparticle, spark, tableau, thoughtspot → respective APIs

## Testing

Run the test suite to verify functionality:
```bash
python -m unittest test_atlan_extractor.py -v
```

All tests pass with comprehensive coverage of error handling, API interactions, and CSV export functionality.