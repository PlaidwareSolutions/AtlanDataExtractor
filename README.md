# Atlan Data Extractor

A Python script that extracts connections and databases data from Atlan APIs and exports the data to CSV files.

## Features

- Extract connection data from Atlan's GET CONNECTIONS API
- Export connections to `connections.csv` with detailed attributes
- Iteratively fetch database data for each connection using GET DATABASES API
- Export databases to `databases.csv` with comprehensive metadata
- Robust error handling and logging
- Configurable API endpoints and request payloads
- Support for Bearer token authentication

## Requirements

This script requires Python 3.6+ with the following standard library modules:
- `requests` (for HTTP API calls)
- `json` (for configuration and response parsing)
- `csv` (for CSV file operations)
- `logging` (for error tracking and debugging)
- `sys` and `os` (for system operations)

## Installation

1. Clone or download the script files to your local directory
2. Ensure you have Python 3.6+ installed
3. The script uses only standard library modules, so no additional installations are required

## Configuration

### Authentication

The script requires a Bearer token for Atlan API authentication. You can provide this in two ways:

**Option 1: Environment Variable (Recommended)**
```bash
export ATLAN_AUTH_TOKEN="your_actual_bearer_token_here"
