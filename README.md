# Atlan Data Extractor

A robust Python utility for extracting and exporting connection and database metadata from Atlan APIs, designed for enterprise network environments with advanced security and error management capabilities.

## Features

- **Subdomain-Prefixed File Generation**: All generated files include subdomain prefix extracted from base URL (e.g., `company.connections_2025-06-11-15-30-45.csv`)
- **Base URL Configuration**: Centralized URL management eliminates repetition across API endpoints
- **Sequential API Extraction**: Extract connection data from Atlan's GET CONNECTIONS API, then fetch databases for each connection
- **Timestamped Output Files**: All CSV files include timestamps for version tracking and historical data management
- **Combined Dataset Creation**: Creates merged CSV with left join logic ensuring all connections appear even without matching databases
- **Automated File Management**: 30-day automatic cleanup of old log and output files to prevent disk space issues
- **Comprehensive Logging**: Timestamped logs with subdomain prefix in dedicated logs/ directory
- **Flexible Authentication**: Support for environment variable or config-based Bearer token authentication
- **Multi-Connector Support**: Handles various connector types (databricks, snowflake, oracle, tableau, etc.) with dedicated API configurations
- **Robust Error Handling**: Comprehensive exception handling with detailed logging and graceful failure recovery
- **Production-Ready Testing**: 15 comprehensive unit tests covering all functionality including edge cases

## Project Structure

```
atlan-data-extractor/
├── main.py                          # Main extractor script with subdomain prefix functionality
├── configs/                         # Configuration directory
│   └── config.json                 # Base URL and API endpoints configuration
├── logs/                           # Timestamped log files with subdomain prefix
│   └── subdomain.atlan_extractor_YYYY-MM-DD-HH-MM-SS.log
├── output/                         # Timestamped CSV output files with subdomain prefix
│   ├── subdomain.connections_YYYY-MM-DD-HH-MM-SS.csv
│   ├── subdomain.databases_YYYY-MM-DD-HH-MM-SS.csv
│   └── subdomain.connections-databases_YYYY-MM-DD-HH-MM-SS.csv
├── test_atlan_extractor.py        # Comprehensive unit tests (15 test cases)
├── project_requirements.txt       # Project dependencies
├── .gitignore                     # Git ignore file
└── README.md                      # This file
```

## Requirements

- Python 3.6+
- Required packages (see Installation section)

## Installation

### 1. Clone or Download Files
Download all project files to your local directory.

### 2. Install Dependencies
Install the required Python packages:

```bash
pip install requests coverage
```

Or using the project requirements file:
```bash
pip install -r project_requirements.txt
```

### 3. Verify Installation
Test that all dependencies are properly installed:
```bash
python -c "import requests, coverage; print('All dependencies installed successfully')"
```

## Configuration

### Authentication

The script requires a Bearer token for Atlan API authentication. You can provide this in two ways:

**Option 1: Environment Variable (Recommended)**
```bash
export ATLAN_AUTH_TOKEN="your_actual_bearer_token_here"
```

**Option 2: Configuration File**
Add your token to the `configs/config.json` file:
```json
{
  "auth_token": "your_actual_bearer_token_here"
}
```

### Base URL and API Configuration

Update the `configs/config.json` file with your Atlan instance details:

```json
{
  "base_url": "https://your-company.atlan.com",
  "auth_token": "your_actual_bearer_token_here",
  "connections_api": {
    "url": "/api/getConnections",
    "payload": {
      "dsl": {
        "size": 400,
        "sort": [{"__timestamp.date": {"format": "epoch_second", "order": "desc"}}],
        "query": {},
        "post_filter": {},
        "aggs": {}
      },
      "attributes": ["connectorName", "isPartial"],
      "suppressLogs": true,
      "requestMetadata": {
        "utmTags": ["page_glossary", "project_webapp", "action_bootstrap", "action_fetch_connections"]
      }
    }
  },
  "api_map": {
    "databricks": "databases_api",
    "snowflake": "databases_api",
    "oracle": "databases_api",
    "tableau": "tableau_api",
    "alteryx": "alteryx_api"
  },
  "databases_api": {
    "url": "/api/getDatabases",
    "payload": {
      "dsl": {
        "sort": [{"name.keyword": {"order": "asc"}}],
        "size": 300,
        "query": {
          "bool": {
            "filter": {
              "bool": {
                "must": [
                  {"term": {"__state": "ACTIVE"}},
                  {"bool": {"should": [{"term": {"__typeName.keyword": "Database"}}]}},
                  {"bool": {"filter": {"term": {"connectionQualifiedName": "PLACEHOLDER_TO_BE_REPLACED"}}}}
                ]
              }
            }
          }
        }
      },
      "attributes": ["name", "displayName"],
      "suppressLogs": true
    }
  }
}
```

**Key Configuration Elements:**
- `base_url`: Your Atlan instance URL (subdomain automatically extracted for file prefixes)
- `api_map`: Maps connector types to their corresponding API configurations
- `PLACEHOLDER_TO_BE_REPLACED`: Automatically replaced with connection qualified names during execution

## Usage

### Running the Data Extractor

1. **Basic execution:**
```bash
python main.py
```

2. **With custom config file:**
```bash
python main.py custom_config.json
```

3. **With environment variable for authentication:**
```bash
ATLAN_AUTH_TOKEN="your_token" python main.py
```

### Expected Output

The script generates three timestamped CSV files with subdomain prefixes in the `output/` directory:

**subdomain.connections_YYYY-MM-DD-HH-MM-SS.csv** - Contains connection data with columns:
- name (connection display name)
- connection_qualified_name (unique identifier)
- connector_name (e.g., databricks, snowflake)
- updated_by
- created_by
- create_time
- update_time

**subdomain.databases_YYYY-MM-DD-HH-MM-SS.csv** - Contains database data with columns:
- connection_qualified_name (parent connection)
- type_name (entity type, typically "Database")
- qualified_name (unique database identifier)
- name (database display name)
- created_by
- updated_by
- create_time
- update_time

**subdomain.connections-databases_YYYY-MM-DD-HH-MM-SS.csv** - Combined dataset using left join:
- connector_name
- connection_name
- category
- type_name
- name (database name, empty if no databases found)

Example filenames:
- `company.connections_2025-06-11-15-30-45.csv`
- `company.databases_2025-06-11-15-30-45.csv`
- `company.connections-databases_2025-06-11-15-30-45.csv`

### Execution Flow

1. **Extract Subdomain**: Extracts subdomain from base_url for file prefixing
2. **Load Configuration**: Reads base URL and API endpoints from configs/config.json
3. **File Cleanup**: Automatically removes log and output files older than 30 days
4. **Fetch Connections**: Makes POST request to connections API using base_url + endpoint
5. **Export Connections**: Saves to subdomain.connections_timestamp.csv in output/ directory
6. **Process Each Connection**: For each connection, determines appropriate API using api_map
7. **Fetch Databases**: Makes POST requests with connection-specific payload replacement
8. **Export Databases**: Saves to subdomain.databases_timestamp.csv
9. **Create Combined File**: Generates left-joined dataset as subdomain.connections-databases_timestamp.csv
10. **Logging**: All operations logged to subdomain.atlan_extractor_timestamp.log in logs/ directory

## Testing

### Running Unit Tests

Execute the comprehensive test suite:

```bash
python -m unittest test_atlan_extractor.py -v
```

### Code Coverage Analysis

Generate and view code coverage report:

```bash
# Run tests with coverage
python -m coverage run -m unittest test_atlan_extractor.py

# Generate coverage report
python -m coverage report --show-missing

# Generate HTML coverage report
python -m coverage html
```

### Test Features

The updated test suite validates:
- **Subdomain Prefix Functionality**: Tests extraction of subdomain from URLs and file prefix generation
- **String Replacement Functionality**: Tests payload modification using PLACEHOLDER_TO_BE_REPLACED approach
- **Base URL Configuration**: Tests proper URL combination from centralized base URL and endpoints
- **Timestamped Filenames**: Tests generation of timestamped log and CSV files with subdomain prefixes
- **Combined CSV Left Join**: Tests joining of connections and databases ensuring all connections appear
- **File Cleanup**: Tests automatic deletion of files older than 30 days from logs/ and output/ directories
- **Multi-Connector Support**: Tests API mapping for different connector types (databricks, tableau, etc.)
- **Authentication Handling**: Tests environment variable vs config file token priority
- **Error Handling**: Tests network failures, invalid responses, and edge cases
- **CSV Export Operations**: Tests individual and combined file creation with proper column ordering
- **Directory Management**: Tests automatic creation of logs/ and output/ directories

### Test Coverage Results

The test suite achieves comprehensive coverage with 15 focused test cases covering:

- **Configuration Management**: Valid/invalid config files, authentication methods
- **API Request Handling**: Success scenarios, HTTP errors, network failures, JSON parsing
- **Data Processing**: Entity validation, malformed data handling, empty responses
- **CSV Export Operations**: Individual and combined file creation, permission errors, data integrity
- **Combined Data Processing**: Left join logic, column ordering, filename generation
- **File Management**: Timestamped naming, cleanup functionality, directory structure
- **Workflow Execution**: End-to-end processing, error scenarios, edge cases

### Test Categories

```bash
# Run specific test categories
python -m unittest test_atlan_extractor.TestAtlanExtractor.test_get_connections_success
python -m unittest test_atlan_extractor.TestAtlanExtractor.test_export_connections_to_csv_success
python -m unittest test_atlan_extractor.TestMainFunction
```

## Troubleshooting

### Common Issues

**Authentication Errors (403 Forbidden)**
- Verify your Bearer token is valid and has proper permissions
- Ensure the token is correctly set in environment variable or config.json
- Check that your Atlan instance URL is correct

**Network Connection Issues**
- Verify connectivity to your Atlan instance
- Check firewall settings and network access
- Ensure API endpoints are accessible

**Configuration Errors**
- Validate JSON syntax in config.json
- Ensure all required fields are present
- Check API endpoint URLs match your Atlan instance

**Empty Results**
- Verify your token has access to connections and databases
- Check API endpoint paths are correct for your Atlan version
- Review Atlan instance permissions and data availability

### Logging

The script generates detailed logs with subdomain prefixes in:
- **Console output**: Real-time progress and status updates
- **logs/subdomain.atlan_extractor_timestamp.log**: Complete execution log with timestamps

Log levels include:
- INFO: Normal operation progress (URL requests, file operations, subdomain extraction)
- WARNING: Non-critical issues (empty data, cleanup operations)
- ERROR: Critical errors requiring attention (authentication failures, network issues)

Example log entries:
```
2025-06-11 15:30:45,123 - INFO - Using subdomain prefix: company
2025-06-11 15:30:45,124 - INFO - Making API request to URL: https://company.atlan.com/api/getConnections
2025-06-11 15:30:46,456 - INFO - Exporting 25 connections to company.connections_2025-06-11-15-30-45.csv
```

## Data Output Format

### Connections CSV Structure (subdomain.connections_timestamp.csv)
```csv
name,connection_qualified_name,connector_name,updated_by,created_by,create_time,update_time
odessa-dev,default/databricks/123,databricks,user@company.com,user@company.com,1748635725374,1748635725374
snowflake-prod,default/snowflake/456,snowflake,admin@company.com,admin@company.com,1748635725374,1748635725374
```

### Databases CSV Structure (subdomain.databases_timestamp.csv)
```csv
connection_qualified_name,type_name,qualified_name,name,created_by,updated_by,create_time,update_time
default/databricks/123,Database,default/databricks/123/db1,db1,user@company.com,user@company.com,1745543118290,1748449415976
default/databricks/123,Database,default/databricks/123/db2,db2,user@company.com,user@company.com,1745543118290,1748449415976
default/snowflake/456,Database,default/snowflake/456/analytics,analytics,admin@company.com,admin@company.com,1745543118290,1748449415976
```

### Combined CSV Structure (subdomain.connections-databases_timestamp.csv)
```csv
connector_name,connection_name,category,type_name,name
databricks,odessa-dev,lake,Database,db1
databricks,odessa-dev,lake,Database,db2
snowflake,snowflake-prod,warehouse,Database,analytics
tableau,tableau-server,visualization,,
```

**Note**: The combined CSV uses left join logic - connections without databases show empty database fields but are still included.

## Security Considerations

- Store authentication tokens securely using environment variables
- Avoid committing tokens to version control
- Use least-privilege access tokens
- Regularly rotate authentication credentials
- Monitor API usage and access logs

## Support

For issues related to:
- **Atlan API access**: Contact your Atlan administrator
- **Script functionality**: Review logs and test results
- **Authentication**: Verify token permissions with Atlan support
