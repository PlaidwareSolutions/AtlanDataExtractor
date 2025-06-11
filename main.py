#!/usr/bin/env python3
"""
Atlan Data Extractor - Multi-Subdomain Version

This script extracts connections and databases data from multiple Atlan subdomains
and exports the data to CSV files with subdomain prefixes.

The script performs the following operations:
1. Loads configuration from config.json file with subdomain_auth_token_map
2. Processes each subdomain with its specific authentication token
3. Fetches connections data from each Atlan subdomain
4. Exports connections data to subdomain.connections_timestamp.csv
5. For each connection, fetches associated databases
6. Exports all databases data to subdomain.databases_timestamp.csv
7. Creates combined CSV with subdomain column as first field

Author: Enhanced for multi-subdomain support
Version: 2.0
Dependencies: requests, json, csv, logging, sys, os
"""

import json
import csv
import requests
import logging
import sys
import os
import glob
from datetime import datetime, timedelta

# Create logs directory for timestamped log files if it doesn't exist
LOGS_DIR = 'logs'
if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)

# Generate timestamp for all files
timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

# Load configuration first to get base URL
with open('configs/config.json', 'r') as f:
    config = json.load(f)

# Extract subdomain from base URL for file prefixes
def extract_subdomain(url):
    """Extract subdomain from URL for use as file prefix"""
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        hostname = parsed.hostname or ''
        # Extract first part before .atlan.com or similar
        parts = hostname.split('.')
        return parts[0] if parts and len(parts) > 0 and parts[0] else 'atlan'
    except Exception:
        return 'atlan'

BASE_URL = config.get('base_url', '')
if not BASE_URL:
    print("ERROR: Base URL not found in configuration")
    sys.exit(1)

SUBDOMAIN_PREFIX = extract_subdomain(BASE_URL)

# Generate prefixed log filename
log_filename = os.path.join(LOGS_DIR, f'{SUBDOMAIN_PREFIX}.atlan_extractor_{timestamp}.log')

# Configure logging to both file and console for comprehensive monitoring
# Log level INFO provides detailed execution flow without debug verbosity
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),  # Prefixed timestamped log file in logs directory
        logging.StreamHandler(sys.stdout)  # Real-time console output
    ])

logger = logging.getLogger(__name__)

# Create output directory for CSV files if it doesn't exist
OUTPUT_DIR = 'output'
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
    logger.info(f"Created output directory: {OUTPUT_DIR}")

logger.info(f"Using subdomain prefix: {SUBDOMAIN_PREFIX}")


def cleanup_old_files():
    """
    Remove log files and output files older than 30 days to prevent disk space issues.
    
    This function automatically cleans up:
    - Log files in logs/ directory older than 30 days
    - CSV output files in output/ directory older than 30 days
    """
    cutoff_date = datetime.now() - timedelta(days=30)
    files_deleted = 0
    
    # Clean up old log files (both prefixed and non-prefixed for backward compatibility)
    log_patterns = [
        os.path.join(LOGS_DIR, 'atlan_extractor_*.log'),
        os.path.join(LOGS_DIR, '*.atlan_extractor_*.log')
    ]
    
    for pattern in log_patterns:
        for log_file in glob.glob(pattern):
            try:
                file_time = datetime.fromtimestamp(os.path.getmtime(log_file))
                if file_time < cutoff_date:
                    os.remove(log_file)
                    files_deleted += 1
                    logger.info(f"Deleted old log file: {log_file}")
            except OSError as e:
                logger.warning(f"Could not delete log file {log_file}: {e}")
    
    # Clean up old output files (both prefixed and non-prefixed for backward compatibility)
    output_patterns = [
        os.path.join(OUTPUT_DIR, 'connections_*.csv'),
        os.path.join(OUTPUT_DIR, 'databases_*.csv'),
        os.path.join(OUTPUT_DIR, 'connections-databases_*.csv'),
        os.path.join(OUTPUT_DIR, '*.connections_*.csv'),
        os.path.join(OUTPUT_DIR, '*.databases_*.csv'),
        os.path.join(OUTPUT_DIR, '*.connections-databases_*.csv')
    ]
    
    for pattern in output_patterns:
        for output_file in glob.glob(pattern):
            try:
                file_time = datetime.fromtimestamp(os.path.getmtime(output_file))
                if file_time < cutoff_date:
                    os.remove(output_file)
                    files_deleted += 1
                    logger.info(f"Deleted old output file: {output_file}")
            except OSError as e:
                logger.warning(f"Could not delete output file {output_file}: {e}")
    
    if files_deleted > 0:
        logger.info(f"Cleanup completed: {files_deleted} old files removed")
    else:
        logger.info("Cleanup completed: No old files found to remove")


def get_auth_token():
    """
    Retrieve authentication token from environment variable or config file.
    
    Priority order:
    1. ATLAN_AUTH_TOKEN environment variable (recommended for security)
    2. auth_token field from config.json file
    
    Returns:
        str: Bearer token formatted for API authentication
        
    Raises:
        SystemExit: If no token is found in either location
    """
    # Check environment variable first (more secure)
    token = os.getenv('ATLAN_AUTH_TOKEN')
    if not token:
        # Fallback to config file
        token = config.get('auth_token', '')

    # Validate token exists
    if not token:
        logger.error(
            "No authentication token found. Please set ATLAN_AUTH_TOKEN environment variable or add auth_token to config.json"
        )
        sys.exit(1)

    # Ensure proper Bearer format
    if token.lower().startswith('bearer '):
        return token
    return f'Bearer {token}'


def make_api_request(endpoint_path, payload):
    """
    Make HTTP POST request to Atlan API with authentication and error handling.
    
    Args:
        endpoint_path (str): API endpoint path (relative to base URL)
        payload (dict): JSON payload for the POST request
        
    Returns:
        dict or None: JSON response from API, None if request fails
        
    Raises:
        None: All exceptions are caught and logged
    """
    # Combine base URL with endpoint path to create full URL
    full_url = f"{BASE_URL.rstrip('/')}{endpoint_path}"
    
    # Prepare headers with authentication and content type
    headers = {
        'Authorization': get_auth_token(),
        'Content-Type': 'application/json'
    }

    response = None  # Initialize response variable for proper scope
    try:
        # Log the URL being used for the API request
        logger.info(f"Making API request to URL: {full_url}")
        
        # Make POST request with timeout to prevent hanging
        response = requests.post(full_url,
                                 headers=headers,
                                 data=json.dumps(payload),
                                 timeout=30)
        # Raise exception for HTTP error status codes (4xx, 5xx)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        # Handle network, timeout, and HTTP errors
        logger.error(f"API request failed for {full_url}: {e}")
        if response is not None:
            logger.error(f"Response status: {response.status_code}")
            logger.error(f"Response content: {response.text}")
        return None
    except json.JSONDecodeError as e:
        # Handle invalid JSON response
        logger.error(f"Failed to parse JSON response from {full_url}: {e}")
        if response is not None:
            logger.error(f"Response content: {response.text}")
        return None


def get_connections():
    """
    Fetch connections data from Atlan connections API.
    
    Makes a POST request to the connections endpoint configured in config.json
    and extracts relevant connection metadata from the response.
    
    Returns:
        list: List of dictionaries containing connection data with keys:
              - name: Connection display name
              - connection_qualified_name: Unique identifier for the connection
              - connector_name: Type of connector (e.g., databricks, snowflake)
              - updated_by: User who last updated the connection
              - created_by: User who created the connection
              - create_time: Timestamp when connection was created
              - update_time: Timestamp when connection was last updated
    """
    logger.info("Fetching connections from Atlan API")

    # Get API endpoint and payload from configuration
    url = config['connections_api']['url']
    payload = config['connections_api']['payload'].copy()

    # Make API request to fetch connections
    response = make_api_request(url, payload)
    if not response:
        logger.warning("Failed to fetch connections from Atlan API")
        return []

    # Extract entities from response
    entities = response.get('entities', [])
    logger.info(f"Retrieved {len(entities)} connections from Atlan API")

    # Process each connection entity
    connections = []
    for entity in entities:
        try:
            # Extract attributes and metadata from entity
            attributes = entity.get('attributes', {})
            connection_data = {
                'connection_name': attributes.get('name', ''),
                'connection_qualified_name':
                attributes.get('qualifiedName', ''),
                'connector_name': attributes.get('connectorName', ''),
                'category': attributes.get('category', ''),
                'updated_by': entity.get('updatedBy', ''),
                'created_by': entity.get('createdBy', ''),
                'create_time': entity.get('createTime', ''),
                'update_time': entity.get('updateTime', '')
            }
            connections.append(connection_data)
            logger.debug(
                f"Processed connection: {connection_data['connection_name']}")
        except Exception as e:
            # Log warning but continue processing other connections
            logger.warning(f"Failed to process connection entity: {e}")
            continue

    return connections


def get_databases(connection_qualified_name, connector_name):
    """
    Fetch databases associated with a specific connection from Atlan databases API.
    
    Args:
        connection_qualified_name (str): Unique identifier of the connection
                                       to fetch databases for
    
    Returns:
        list: List of dictionaries containing database data with keys:
              - connection_qualified_name: Parent connection identifier
              - type_name: Entity type (typically "Database")
              - qualified_name: Unique identifier for the database
              - name: Database display name
              - created_by: User who created the database
              - updated_by: User who last updated the database
              - create_time: Timestamp when database was created
              - update_time: Timestamp when database was last updated
    """
    logger.info(
        f"Fetching databases for connection: {connection_qualified_name}")

    # Get API endpoint and payload template from configuration
    url = config[config["databases_api_map"][connector_name]]['url']
    payload_template = config[config["databases_api_map"][connector_name]]['payload']

    # Convert payload to JSON string and replace placeholder with actual connection qualified name
    try:
        payload_json_str = json.dumps(payload_template)
        # Replace the placeholder with the actual connection qualified name
        updated_payload_str = payload_json_str.replace("PLACEHOLDER_TO_BE_REPLACED", connection_qualified_name)
        # Convert back to dictionary
        payload = json.loads(updated_payload_str)
    except json.JSONDecodeError as e:
        logger.error(
            f"Failed to update payload with connection qualified name using string replacement: {e}")
        return []

    # Make API request to fetch databases for this connection
    response = make_api_request(url, payload)
    if not response:
        logger.warning(
            f"Failed to fetch databases for connection: {connection_qualified_name}"
        )
        return []

    # Extract entities from response
    entities = response.get('entities', [])
    logger.info(
        f"Retrieved {len(entities)} databases for connection: {connection_qualified_name}"
    )

    # Process each database entity
    databases = []
    for entity in entities:
        try:
            # Extract attributes and metadata from entity
            attributes = entity.get('attributes', {})
            database_data = {
                'connection_qualified_name': connection_qualified_name,
                'type_name': entity.get('typeName', ''),
                'qualified_name': attributes.get('qualifiedName', ''),
                'name': attributes.get('name', ''),
                'created_by': entity.get('createdBy', ''),
                'updated_by': entity.get('updatedBy', ''),
                'create_time': entity.get('createTime', ''),
                'update_time': entity.get('updateTime', '')
            }
            databases.append(database_data)
            logger.debug(f"Processed database: {database_data['name']}")
        except Exception as e:
            # Log warning but continue processing other databases
            logger.warning(f"Failed to process database entity: {e}")
            continue

    return databases


def export_connections_to_csv(connections, filename=None):
    """
    Export connections data to CSV file with proper formatting and error handling.
    
    Args:
        connections (list): List of connection dictionaries to export
        filename (str): Output CSV filename (default: timestamped connections file)
        
    Returns:
        None: Function writes to file and logs results
        
    Raises:
        None: All exceptions are caught and logged
    """
    # Check if there's data to export
    if not connections:
        logger.warning("No connections data to export")
        return

    # Generate timestamped filename with subdomain prefix if not provided
    if filename is None:
        filename = f'{SUBDOMAIN_PREFIX}.connections_{timestamp}.csv'

    logger.info(f"Exporting {len(connections)} connections to {filename}")

    # Define CSV column headers matching the connection data structure
    fieldnames = [
        'connection_name', 'connection_qualified_name', 'connector_name',
        'category', 'created_by', 'updated_by', 'create_time', 'update_time'
    ]

    try:
        # Create full path to output directory
        filepath = os.path.join(OUTPUT_DIR, filename)
        # Write connections data to CSV file with UTF-8 encoding
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()  # Write column headers
            writer.writerows(connections)  # Write data rows
            logger.info(f"Successfully exported connections to {filepath}")
    except IOError as e:
        # Handle file permission or disk space errors
        logger.error(f"Failed to write connections CSV file: {e}")


def export_databases_to_csv(databases, filename=None):
    """
    Export databases data to CSV file with proper formatting and error handling.
    
    Args:
        databases (list): List of database dictionaries to export
        filename (str): Output CSV filename (default: timestamped databases file)
        
    Returns:
        None: Function writes to file and logs results
        
    Raises:
        None: All exceptions are caught and logged
    """
    # Check if there's data to export
    if not databases:
        logger.warning("No databases data to export")
        return

    # Generate timestamped filename with subdomain prefix if not provided
    if filename is None:
        filename = f'{SUBDOMAIN_PREFIX}.databases_{timestamp}.csv'

    logger.info(f"Exporting {len(databases)} databases to {filename}")

    # Define CSV column headers matching the database data structure
    fieldnames = [
        'type_name', 'qualified_name', 'name', 'created_by', 'updated_by',
        'create_time', 'update_time', 'connection_qualified_name'
    ]

    try:
        # Create full path to output directory
        filepath = os.path.join(OUTPUT_DIR, filename)
        # Write databases data to CSV file with UTF-8 encoding
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()  # Write column headers
            writer.writerows(databases)  # Write data rows
            logger.info(f"Successfully exported databases to {filepath}")
    except IOError as e:
        # Handle file permission or disk space errors
        logger.error(f"Failed to write databases CSV file: {e}")


def create_combined_csv(connections, databases, filename=None):
    """
    Create a combined CSV file by joining connections and databases data.
    
    Performs a left join based on connection_qualified_name to ensure all 
    connections appear in the result even when there are no matching databases.
    
    Args:
        connections (list): List of connection dictionaries
        databases (list): List of database dictionaries
        filename (str): Output CSV filename (default: timestamped combined file)
        
    Returns:
        None: Function writes to file and logs results
        
    Raises:
        None: All exceptions are caught and logged
    """
    # Check if there's connections data to export
    if not connections:
        logger.warning("No connections data available for combined file")
        return

    # Generate timestamped filename with subdomain prefix if not provided
    if filename is None:
        filename = f'{SUBDOMAIN_PREFIX}.connections-databases_{timestamp}.csv'

    logger.info(f"Creating combined file with {len(connections)} connections and {len(databases)} databases")

    # Create a lookup dictionary for databases by connection_qualified_name
    databases_by_connection = {}
    for db in databases:
        conn_qualified_name = db.get('connection_qualified_name', '')
        if conn_qualified_name not in databases_by_connection:
            databases_by_connection[conn_qualified_name] = []
        databases_by_connection[conn_qualified_name].append(db)

    # Define CSV column headers in the specified order
    fieldnames = ['connector_name', 'connection_name', 'category', 'type_name', 'name']
    
    combined_data = []
    
    # Perform left join: iterate through connections and add matching databases
    for connection in connections:
        conn_qualified_name = connection.get('connection_qualified_name', '')
        matching_databases = databases_by_connection.get(conn_qualified_name, [])
        
        if matching_databases:
            # Add a row for each matching database
            for database in matching_databases:
                combined_row = {
                    'connector_name': connection.get('connector_name', ''),
                    'connection_name': connection.get('connection_name', ''),
                    'category': connection.get('category', ''),
                    'type_name': database.get('type_name', ''),
                    'name': database.get('name', '')
                }
                combined_data.append(combined_row)
        else:
            # Add connection row with empty database fields (left join behavior)
            combined_row = {
                'connector_name': connection.get('connector_name', ''),
                'connection_name': connection.get('connection_name', ''),
                'category': connection.get('category', ''),
                'type_name': '',
                'name': ''
            }
            combined_data.append(combined_row)

    try:
        # Create full path to output directory
        filepath = os.path.join(OUTPUT_DIR, filename)
        # Write combined data to CSV file with UTF-8 encoding
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()  # Write column headers
            writer.writerows(combined_data)  # Write data rows
            logger.info(f"Successfully created combined file: {filepath} with {len(combined_data)} rows")
    except IOError as e:
        # Handle file permission or disk space errors
        logger.error(f"Failed to write combined CSV file: {e}")


def main():
    """
    Main execution function that orchestrates the complete data extraction workflow.
    
    Workflow steps:
    1. Clean up old files (logs and outputs older than 30 days)
    2. Fetch all connections from Atlan connections API
    3. Export connections data to timestamped CSV file
    4. For each connection, fetch associated databases
    5. Export all databases data to timestamped CSV file
    6. Create combined CSV file by joining connections and databases
    7. Log completion summary
    
    Raises:
        SystemExit: If no connections are found or critical errors occur
    """
    logger.info("Starting Atlan data extraction process")
    
    # Step 1: Clean up old files
    cleanup_old_files()

    # Step 2: Fetch connections from Atlan API
    connections = get_connections()
    if not connections:
        logger.error("No connections found. Exiting.")
        sys.exit(1)

    # Step 3: Export connections to CSV file
    export_connections_to_csv(connections)

    # Step 4: Fetch databases for each connection
    all_databases = []
    for connection in connections:
        connection_qualified_name = connection.get('connection_qualified_name')
        connector_name = connection.get('connector_name')
        if connection_qualified_name:
            # Fetch databases associated with this connection
            databases = get_databases(connection_qualified_name,
                                      connector_name)
            all_databases.extend(databases)
        else:
            # Log warning for connections without qualified names
            logger.warning(f"Connection missing qualified name: {connection}")

    # Step 5: Export all databases to CSV file
    export_databases_to_csv(all_databases)

    # Step 6: Create combined CSV file by joining connections and databases
    create_combined_csv(connections, all_databases)

    # Step 7: Log completion summary
    logger.info(
        f"Data extraction completed successfully. "
        f"Processed {len(connections)} connections and {len(all_databases)} databases."
    )


if __name__ == '__main__':
    """
    Entry point for script execution.
    
    This block ensures the main() function is only called when the script
    is executed directly, not when imported as a module.
    """
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
