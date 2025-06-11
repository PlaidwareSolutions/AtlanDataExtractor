# Atlan Data Extractor - Multi-Subdomain Version

A robust Python utility for extracting and exporting connection and database metadata from multiple Atlan subdomains simultaneously, designed for enterprise environments with advanced security and error management capabilities.

## Features

- **Multi-Subdomain Processing**: Simultaneously processes multiple Atlan instances (xyz, abc, lmn, etc.)
- **Individual Authentication**: Supports separate authentication tokens for each subdomain
- **Automated Metadata Extraction**: Fetches connections and databases data from Atlan APIs
- **Subdomain-Prefixed Exports**: Creates timestamped CSV files with subdomain identification
- **Combined Cross-Instance Analysis**: Generates unified CSV with subdomain tracking as first column
- **Advanced Error Handling**: Comprehensive logging and error management per subdomain
- **Flexible API Configuration**: Supports multiple connection types and API endpoints
- **Secure Authentication**: Bearer token authentication with environment variable support
- **Enterprise-Ready**: Designed for production environments with proper logging and monitoring
- **Automatic File Cleanup**: Removes files older than 30 days to prevent disk space issues
- **Backward Compatibility**: Supports legacy single-subdomain configurations

## Project Structure

```
atlan-data-extractor/
├── main.py                          # Multi-subdomain extractor script
├── configs/                         # Configuration directory
│   └── config.json                 # Multi-subdomain configuration with auth mapping
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
│   └── abc.connections-databases_YYYY-MM-DD-HH-MM-SS.csv
├── test_atlan_extractor.py        # Comprehensive unit tests (15 test cases)
├── project_requirements.txt       # Project dependencies
├── .gitignore                     # Git ignore file
└── README.md                      # This file
```

## Requirements

- Python 3.7+
- Required packages: `requests`

## Installation

### 1. Clone or Download Files
Download all project files to your local directory.

### 2. Install Dependencies
Install the required Python packages:

```bash
pip install requests
```

### 3. Verify Installation
Test that all dependencies are properly installed:
```bash
python -c "import requests; print('All dependencies installed successfully')"
```

## Configuration

### Multi-Subdomain Setup

Create `configs/config.json` with your Atlan subdomain details:

```json
{
  "base_url_template": "https://{subdomain}.atlan.com",
  "subdomain_auth_token_map": {
    "xyz": "xyz_auth_token_here",
    "abc": "abc_auth_token_here", 
    "lmn": "lmn_auth_token_here"
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
    "alteryx": "alteryx_api",
    "databricks": "databases_api",
    "oracle": "databases_api",
    "snowflake": "databases_api",
    "tableau": "tableau_api",
    "thoughtspot": "thoughtspot_api"
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
      "relationAttributes": [],
      "suppressLogs": true,
      "requestMetadata": {},
      "async": {
        "isCallAsync": true,
        "searchContextId": "f53f9d56-976e-4ec9-8cbc-a8c1d931a12a",
        "searchContextSequenceNo": 1,
        "requestTimeoutInSecs": 30
      }
    }
  }
}
```

### Single Subdomain (Backward Compatibility)

For single subdomain setups, you can still use the legacy format:

```json
{
  "base_url": "https://your-instance.atlan.com",
  "auth_token": "your_bearer_token_here"
}
```

### Environment Variables (Optional)

You can also set authentication tokens as environment variables:

```bash
export ATLAN_AUTH_TOKEN="your_bearer_token_here"
```

## Usage

### Multi-Subdomain Execution

Run the extractor to process all configured subdomains:

```bash
python main.py
```

### Expected Output

The script generates subdomain-prefixed timestamped files in the `output/` directory:

**Per Subdomain Files:**
- `xyz.connections_2025-06-11-05-42-22.csv` - Connection metadata for xyz subdomain
- `xyz.databases_2025-06-11-05-42-22.csv` - Database metadata for xyz subdomain
- `xyz.connections-databases_2025-06-11-05-42-22.csv` - Combined data with left join logic

**Combined Analysis:**
Each combined CSV includes `subdomain` as the first column for cross-instance analysis.

### Log Files

Execution logs are saved per subdomain in the `logs/` directory:

- `xyz.atlan_extractor_2025-06-11-05-42-22.log` - Detailed execution logs for xyz
- `abc.atlan_extractor_2025-06-11-05-42-22.log` - Detailed execution logs for abc
- `lmn.atlan_extractor_2025-06-11-05-42-22.log` - Detailed execution logs for lmn

## CSV Output Structure

### Connections CSV
- `name` - Connection name
- `connection_qualified_name` - Unique connection identifier
- `connector_name` - Type of connector (e.g., databricks, snowflake)
- `updated_by` - Last updated by user
- `created_by` - Created by user
- `create_time` - Creation timestamp
- `update_time` - Last update timestamp

### Databases CSV
- `connection_qualified_name` - Parent connection identifier
- `type_name` - Database type
- `qualified_name` - Unique database identifier
- `name` - Database name
- `created_by` - Created by user
- `updated_by` - Last updated by user
- `create_time` - Creation timestamp
- `update_time` - Last update timestamp

### Combined CSV (Multi-Subdomain Analysis)
- `subdomain` - Source subdomain (xyz, abc, lmn)
- `connector_name` - Type of connector
- `connection_name` - Connection name
- `category` - Categorized type (lake, warehouse, visualization)
- `type_name` - Database type
- `name` - Database name

## Multi-Subdomain Processing

The extractor processes each subdomain independently:

1. **Sequential Processing**: Processes xyz, abc, lmn subdomains in order
2. **Individual Authentication**: Uses subdomain-specific tokens from the mapping
3. **Isolated Error Handling**: Failures in one subdomain don't affect others
4. **Comprehensive Logging**: Separate log files track each subdomain's processing
5. **Cross-Instance Analysis**: Combined CSV files include subdomain identification

### Processing Summary

After completion, the console displays:
- Total subdomains processed successfully
- Total connections across all subdomains
- Total databases across all subdomains
- Any failed subdomains with error details

### Execution Flow

1. **Load Multi-Subdomain Configuration**: Reads base_url_template and subdomain_auth_token_map
2. **Create Output Directories**: Creates `logs/` and `output/` directories if they don't exist
3. **Clean Old Files**: Removes files older than 30 days to prevent disk space issues
4. **Process Each Subdomain**:
   - Create subdomain-specific base URL (e.g., https://xyz.atlan.com)
   - Set up subdomain-specific logging
   - Fetch connections for the subdomain
   - Export connections to subdomain.connections_timestamp.csv
   - Fetch databases for each connection
   - Export databases to subdomain.databases_timestamp.csv
   - Create combined CSV with subdomain as first column
5. **Processing Summary**: Log overall completion statistics

## Testing

### Running Unit Tests

Execute the comprehensive test suite (15 test cases):

```bash
python -m unittest test_atlan_extractor.py -v
```

### Test Coverage

The test suite validates:
- **Multi-Subdomain Configuration**: Tests subdomain_auth_token_map parsing
- **Subdomain-Specific File Generation**: Tests prefixed log and CSV files
- **Combined CSV Creation**: Tests left join logic with subdomain column
- **Base URL Template Processing**: Tests dynamic URL generation
- **Authentication Token Logic**: Tests Bearer token formatting
- **Error Handling Scenarios**: Tests network failures and API errors
- **File Cleanup Functionality**: Tests automated cleanup of old files
- **Directory Structure**: Tests configs/ and output/ directory creation
- **Timestamped Filename Generation**: Tests filename patterns with timestamps
- **CSV Export Operations**: Tests individual and combined file creation
- **URL Logging Format**: Tests API URL logging
- **JSON Processing**: Tests data extraction and processing
- **String Replacement**: Tests placeholder replacement in payloads
- **Subdomain Extraction**: Tests subdomain parsing from URLs
- **Column Order Validation**: Tests CSV column ordering

### Test Categories

```bash
# Run all tests
python -m unittest test_atlan_extractor.py -v

# Run specific test methods
python -m unittest test_atlan_extractor.TestAtlanExtractorSimple.test_combined_csv_left_join_functionality
python -m unittest test_atlan_extractor.TestAtlanExtractorSimple.test_subdomain_extraction
```

## Error Handling

The extractor includes comprehensive error handling for:

- Network connectivity issues per subdomain
- Authentication failures with specific tokens
- API endpoint changes
- Invalid JSON responses
- File system permissions
- DNS resolution failures
- Missing configuration parameters

All errors are logged with detailed context for troubleshooting, with separate error tracking per subdomain.

## File Cleanup

The script automatically removes log and output files older than 30 days to prevent disk space issues, including both prefixed and non-prefixed files from legacy single-subdomain runs.

## Troubleshooting

### Common Issues

**Authentication Errors (401/403)**
- Verify bearer tokens for each subdomain are correct and have proper permissions
- Ensure tokens are correctly set in subdomain_auth_token_map
- Check that subdomain URLs are accessible

**Network Connection Issues**
- Verify connectivity to each Atlan subdomain
- Check DNS resolution for subdomain URLs
- Ensure API endpoints are accessible per subdomain

**Configuration Errors**
- Validate JSON syntax in config.json
- Ensure base_url_template uses {subdomain} placeholder
- Check subdomain_auth_token_map structure
- Verify api_map connector mappings

**Empty Results**
- Verify tokens have access to connections and databases on each subdomain
- Check API endpoint paths are correct for your Atlan version
- Review per-subdomain permissions and data availability

### Debug Mode

For additional debugging information:
- Check subdomain-specific log files in the `logs/` directory
- Review console output for processing summary
- Monitor individual subdomain error messages
- Verify base URL template formatting

### Multi-Subdomain Debugging

- Check individual subdomain log files for specific errors
- Verify base_url_template formatting: `https://{subdomain}.atlan.com`
- Ensure all required subdomains have authentication tokens
- Monitor processing summary for failed subdomains
- Test individual subdomain connectivity

## Security Considerations

- Never commit authentication tokens to version control
- Use environment variables for sensitive configuration
- Regularly rotate authentication tokens for all subdomains
- Monitor log files for suspicious activity across all instances
- Implement proper access controls for multi-subdomain deployments
- Store tokens securely using enterprise secret management systems

## Production Deployment

### Multi-Subdomain Best Practices

1. **Token Management**: Implement secure token rotation across all subdomains
2. **Monitoring**: Set up alerts for failed subdomain processing
3. **Backup**: Regular backup of configuration and output files
4. **Access Control**: Limit access to subdomain-specific authentication tokens
5. **Audit Trail**: Monitor log files for cross-subdomain activity patterns
6. **Scheduling**: Use cron jobs or task schedulers for automated execution
7. **Storage Management**: Monitor disk usage due to multi-subdomain file generation

## License

This project is provided as-is for educational and enterprise use.

## Support

For issues and questions, refer to the comprehensive logging output and error messages provided by the application. Each subdomain's processing is tracked separately for efficient troubleshooting.

### Support Resources

- **Configuration Issues**: Review subdomain_auth_token_map structure
- **Authentication Problems**: Verify per-subdomain token permissions
- **Network Issues**: Test subdomain connectivity individually
- **Data Issues**: Check per-subdomain log files for detailed error information