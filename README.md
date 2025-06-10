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
- Comprehensive unit tests with 98% code coverage

## Project Structure

```
atlan-data-extractor/
├── main.py                    # Main extractor script
├── configs/                   # Configuration directory
│   └── config.json           # Configuration file for API endpoints
├── test_atlan_extractor.py   # Comprehensive unit tests
├── project_requirements.txt  # Project dependencies
├── README.md                 # This file
├── atlan_extractor.log       # Execution log file
└── output/                   # Output directory for CSV files
    ├── connections.csv       # Generated connections output
    └── databases.csv        # Generated databases output
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

### API Endpoints Configuration

Update the `configs/config.json` file with your Atlan instance details:

```json
{
  "connections_api": {
    "url": "https://your-company.atlan.com/api/meta/search/indexsearch",
    "payload": {
      "dsl": {
        "size": 400,
        "sort": [{"__timestamp.date": {"format": "epoch_second", "order": "desc"}}],
        "query": {},
        "post_filter": {},
        "aggs": {}
      },
      "attributes": ["connectorName", "isPartial"],
      "suppressLogs": true
    }
  },
  "databases_api": {
    "url": "https://your-company.atlan.com/api/meta/search/indexsearch",
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

The script will generate two CSV files in the `output/` directory:

**output/connections.csv** - Contains connection data with columns:
- connection_name
- connection_qualified_name
- connector_name
- category
- created_by
- updated_by
- create_time
- update_time

**output/databases.csv** - Contains database data with columns:
- type_name
- qualified_name
- name
- created_by
- updated_by
- create_time
- update_time
- connection_qualified_name

### Execution Flow

1. **Load Configuration**: Reads API endpoints and authentication from configs/config.json
2. **Create Output Directory**: Creates `output/` directory if it doesn't exist
3. **Fetch Connections**: Makes POST request to connections API (URL logged)
4. **Export Connections**: Saves connection data to output/connections.csv
5. **Fetch Databases**: For each connection, fetches associated databases (URLs logged)
6. **Export Databases**: Saves all database data to output/databases.csv
7. **Logging**: All operations, including API URLs, are logged to console and atlan_extractor.log

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
- **String Replacement Functionality**: Tests the new payload modification approach using string search and replace
- **URL Logging**: Verifies that API URLs are properly logged during requests
- Configuration loading from configs directory
- Authentication token handling (environment vs config)
- API request success and failure scenarios
- Data extraction and processing with proper field mapping
- CSV export functionality to output directory
- Error handling and edge cases
```

### Test Coverage Results

The test suite achieves **98% code coverage** with 30 comprehensive test cases covering:

- **Configuration Management**: Valid/invalid config files, authentication methods
- **API Request Handling**: Success scenarios, HTTP errors, network failures, JSON parsing
- **Data Processing**: Entity validation, malformed data handling, empty responses
- **CSV Export Operations**: File creation, permission errors, data integrity
- **Workflow Execution**: End-to-end processing, error scenarios, edge cases
- **Main Function**: Normal execution, interrupts, unexpected errors

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

The script generates detailed logs in:
- **Console output**: Real-time progress and status updates
- **atlan_extractor.log**: Complete execution log with timestamps

Log levels include:
- INFO: Normal operation progress
- WARNING: Non-critical issues (empty data, API retries)
- ERROR: Critical errors requiring attention

## Data Output Format

### Connections CSV Structure (output/connections.csv)
```csv
connection_name,connection_qualified_name,connector_name,category,created_by,updated_by,create_time,update_time
odessa-dev,default/databricks/123,databricks,lake,user@company.com,user@company.com,1748635725374,1748635725374
```

### Databases CSV Structure (output/databases.csv)
```csv
type_name,qualified_name,name,created_by,updated_by,create_time,update_time,connection_qualified_name
Database,default/databricks/123/db1,db1,user@company.com,user@company.com,1745543118290,1748449415976,default/databricks/123
```

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
