# Atlan Data Extractor - Multi-Subdomain Version

A robust Python utility for extracting and exporting connection and database metadata from multiple Atlan instances simultaneously, designed for enterprise environments with advanced security and error management capabilities.

## Features

- **Multi-Subdomain Processing**: Process multiple Atlan instances (xyz, abc, lmn) in a single execution using subdomain_auth_token_map
- **Individual Authentication**: Each subdomain uses its own authentication token for secure isolated access
- **Subdomain-Prefixed File Generation**: All files include subdomain prefix (e.g., `xyz.connections_2025-06-11-15-30-45.csv`)
- **Base URL Template System**: Flexible `https://{subdomain}.atlan.com` template supports any number of subdomains
- **Enhanced Combined Dataset**: Creates merged CSV with subdomain as first column plus left join logic ensuring all connections appear
- **Subdomain-Specific Logging**: Individual log files per subdomain with comprehensive processing details
- **Sequential API Extraction**: Extract connections from GET CONNECTIONS API, then fetch databases for each connection
- **Timestamped Output Management**: All CSV files include timestamps for version tracking and historical data management
- **Automated File Cleanup**: 30-day automatic cleanup of old log and output files to prevent disk space issues
- **Multi-Connector Support**: Handles databricks, snowflake, oracle, tableau, etc. with dedicated API configurations
- **Robust Error Handling**: Comprehensive exception handling with detailed logging and graceful failure recovery
- **Production-Ready Testing**: 17 comprehensive unit tests covering all functionality including multi-subdomain scenarios

## Project Structure

```
atlan-data-extractor/
├── main.py                          # Multi-subdomain extractor script
├── configs/                         # Configuration directory
│   └── config.json                 # Multi-subdomain configuration with auth token mapping
├── logs/                           # Subdomain-specific timestamped log files
│   ├── xyz.atlan_extractor_YYYY-MM-DD-HH-MM-SS.log
│   ├── abc.atlan_extractor_YYYY-MM-DD-HH-MM-SS.log
│   └── lmn.atlan_extractor_YYYY-MM-DD-HH-MM-SS.log
├── output/                         # Subdomain-prefixed CSV output files
│   ├── xyz.connections_YYYY-MM-DD-HH-MM-SS.csv
│   ├── xyz.databases_YYYY-MM-DD-HH-MM-SS.csv
│   ├── xyz.connections-databases_YYYY-MM-DD-HH-MM-SS.csv
│   ├── abc.connections_YYYY-MM-DD-HH-MM-SS.csv
│   ├── abc.databases_YYYY-MM-DD-HH-MM-SS.csv
│   ├── abc.connections-databases_YYYY-MM-DD-HH-MM-SS.csv
│   └── ... (similar files for each subdomain)
├── test_atlan_extractor.py        # Comprehensive unit tests (17 test cases)
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

### Multi-Subdomain Authentication

The script supports multiple authentication methods for processing multiple Atlan subdomains:

**Option 1: Subdomain-Specific Tokens (Recommended for Multi-Subdomain)**
Configure individual tokens for each subdomain in `configs/config.json`:
```json
{
  "subdomain_auth_token_map": {
    "xyz": "xyz_authentication_token_here",
    "abc": "abc_authentication_token_here",
    "lmn": "lmn_authentication_token_here"
  }
}
```

**Option 2: Environment Variable (Single Token for All Subdomains)**
```bash
export ATLAN_AUTH_TOKEN="your_universal_bearer_token_here"
```

### Multi-Subdomain Configuration

Update the `configs/config.json` file with your multiple Atlan instance details:

```json
{
  "base_url_template": "https://{subdomain}.atlan.com",
  "subdomain_auth_token_map": {
    "xyz": "xyz_authentication_token_here",
    "abc": "abc_authentication_token_here",
    "lmn": "lmn_authentication_token_here"
  },
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
- `base_url_template`: URL template with `{subdomain}` placeholder for dynamic subdomain insertion
- `subdomain_auth_token_map`: Maps each subdomain to its corresponding authentication token
- `api_map`: Maps connector types to their corresponding API configurations
- `PLACEHOLDER_TO_BE_REPLACED`: Automatically replaced with connection qualified names during execution

**Adding New Subdomains:**
To process additional Atlan instances, add new entries to the `subdomain_auth_token_map`:
```json
"subdomain_auth_token_map": {
  "production": "prod_token_here",
  "staging": "staging_token_here",
  "development": "dev_token_here"
}
```

## Usage

### Running the Multi-Subdomain Data Extractor

1. **Standard multi-subdomain execution:**
```bash
python main.py
```
This processes all subdomains defined in `subdomain_auth_token_map` configuration.

2. **With environment variable override:**
```bash
ATLAN_AUTH_TOKEN="universal_token" python main.py
```
This uses a single token for all subdomains (useful for testing or when all instances share the same authentication).

3. **Example execution output:**
```
2025-06-11 15:30:45 - INFO - Starting multi-subdomain Atlan data extraction process
2025-06-11 15:30:45 - INFO - Found 3 subdomains to process: ['xyz', 'abc', 'lmn']
2025-06-11 15:30:45 - INFO - Starting data extraction for subdomain: xyz
2025-06-11 15:30:46 - INFO - Completed processing for subdomain: xyz
2025-06-11 15:30:46 - INFO - Multi-subdomain extraction completed!
2025-06-11 15:30:46 - INFO - Processed 3/3 subdomains successfully
2025-06-11 15:30:46 - INFO - Total connections across all subdomains: 127
2025-06-11 15:30:46 - INFO - Total databases across all subdomains: 543
```

### Expected Multi-Subdomain Output

The script generates timestamped CSV files with subdomain prefixes for each configured subdomain in the `output/` directory:

**Per Subdomain Files Generated:**

**subdomain.connections_YYYY-MM-DD-HH-MM-SS.csv** - Connection data:
- name (connection display name)
- connection_qualified_name (unique identifier)
- connector_name (e.g., databricks, snowflake)
- updated_by
- created_by
- create_time
- update_time

**subdomain.databases_YYYY-MM-DD-HH-MM-SS.csv** - Database data:
- connection_qualified_name (parent connection)
- type_name (entity type, typically "Database")
- qualified_name (unique database identifier)
- name (database display name)
- created_by
- updated_by
- create_time
- update_time

**subdomain.connections-databases_YYYY-MM-DD-HH-MM-SS.csv** - Enhanced combined dataset:
- **subdomain** (first column - identifies the Atlan instance)
- connector_name
- connection_name
- category
- type_name
- name (database name, empty if no databases found)

**Example Multi-Subdomain Output:**
```
output/
├── xyz.connections_2025-06-11-15-30-45.csv
├── xyz.databases_2025-06-11-15-30-45.csv
├── xyz.connections-databases_2025-06-11-15-30-45.csv
├── abc.connections_2025-06-11-15-30-45.csv
├── abc.databases_2025-06-11-15-30-45.csv
├── abc.connections-databases_2025-06-11-15-30-45.csv
├── lmn.connections_2025-06-11-15-30-45.csv
├── lmn.databases_2025-06-11-15-30-45.csv
└── lmn.connections-databases_2025-06-11-15-30-45.csv
```

**Enhanced Combined CSV with Subdomain Tracking:**
```csv
subdomain,connector_name,connection_name,category,type_name,name
xyz,databricks,prod-cluster,lake,Database,analytics_db
xyz,snowflake,warehouse-01,warehouse,Database,reporting_db
abc,tableau,visualization-server,visualization,,
lmn,databricks,dev-cluster,lake,Database,test_db
```

### Multi-Subdomain Execution Flow

1. **Load Multi-Subdomain Configuration**: Reads base_url_template and subdomain_auth_token_map from configs/config.json
2. **Initialize Processing**: Creates output/ and logs/ directories, performs 30-day file cleanup
3. **Process Each Subdomain Sequentially**:
   - Extract subdomain name from subdomain_auth_token_map
   - Generate subdomain-specific base URL using template
   - Create dedicated log file: `subdomain.atlan_extractor_timestamp.log`
   - Fetch connections using subdomain-specific authentication
   - Export connections to: `subdomain.connections_timestamp.csv`
   - For each connection, fetch associated databases using connector-specific APIs
   - Export databases to: `subdomain.databases_timestamp.csv`
   - Create enhanced combined CSV with subdomain column: `subdomain.connections-databases_timestamp.csv`
4. **Completion Summary**: Log overall statistics across all processed subdomains

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

The comprehensive test suite validates:
- **Multi-Subdomain Configuration**: Tests subdomain_auth_token_map parsing and URL template functionality
- **Subdomain-Specific Processing**: Tests individual subdomain processing with dedicated authentication
- **Enhanced Combined CSV**: Tests subdomain column as first field in combined dataset
- **String Replacement Functionality**: Tests payload modification using PLACEHOLDER_TO_BE_REPLACED approach
- **Base URL Template System**: Tests dynamic URL generation from template and subdomain mapping
- **Timestamped Filenames**: Tests generation of timestamped log and CSV files with subdomain prefixes
- **Combined CSV Left Join**: Tests joining of connections and databases ensuring all connections appear
- **File Cleanup**: Tests automatic deletion of files older than 30 days from logs/ and output/ directories
- **Multi-Connector Support**: Tests API mapping for different connector types (databricks, tableau, etc.)
- **Authentication Handling**: Tests subdomain-specific tokens vs environment variable priority
- **Error Handling**: Tests network failures, invalid responses, and edge cases
- **CSV Export Operations**: Tests individual and combined file creation with proper column ordering
- **Directory Management**: Tests automatic creation of logs/ and output/ directories

### Test Coverage Results

The test suite achieves comprehensive coverage with 17 focused test cases covering:

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
