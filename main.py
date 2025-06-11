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

import requests
import json
import csv
import logging
import sys
import os
import glob
from datetime import datetime, timedelta
from urllib.parse import urlparse

# Generate timestamp for all files
timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

# Create directories if they don't exist
LOGS_DIR = 'logs'
OUTPUT_DIR = 'output'

for directory in [LOGS_DIR, OUTPUT_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)

# Load configuration from JSON file
with open('configs/config.json', 'r') as f:
    config = json.load(f)

# Get base URL template and subdomain mapping for multi-subdomain support
BASE_URL_TEMPLATE = config.get('base_url_template', '')
SUBDOMAIN_AUTH_MAP = config.get('subdomain_auth_token_map', {})

if not BASE_URL_TEMPLATE:
    print("ERROR: Base URL template not found in configuration")
    sys.exit(1)

if not SUBDOMAIN_AUTH_MAP:
    print("ERROR: Subdomain authentication mapping not found in configuration")
    sys.exit(1)

# Configure console logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

logger.info("Starting multi-subdomain Atlan data extraction process")
logger.info(f"Found {len(SUBDOMAIN_AUTH_MAP)} subdomains to process: {list(SUBDOMAIN_AUTH_MAP.keys())}")


def cleanup_old_files():
    """
    Remove log files and output files older than 30 days to prevent disk space issues.
    """
    files_deleted = 0
    cutoff_date = datetime.now() - timedelta(days=30)
    
    # Clean up old log files (both prefixed and non-prefixed)
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
    
    # Clean up old output files (both prefixed and non-prefixed)
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
        logger.info(f"Cleanup completed: Deleted {files_deleted} old files")
    else:
        logger.info("Cleanup completed: No old files found to remove")


def make_api_request(endpoint_path, payload, base_url, auth_token, subdomain_logger):
    """
    Make HTTP POST request to Atlan API with authentication and error handling.
    
    Args:
        endpoint_path (str): API endpoint path (relative to base URL)
        payload (dict): JSON payload for the POST request
        base_url (str): Base URL for the subdomain
        auth_token (str): Bearer token for authentication
        subdomain_logger: Logger instance for this subdomain
        
    Returns:
        dict or None: JSON response from API, None if request fails
    """
    # Combine base URL with endpoint path
    url = f"{base_url.rstrip('/')}{endpoint_path}"
    subdomain_logger.info(f"Making API request to URL: {url}")

    # Prepare headers
    headers = {
        'Authorization': auth_token,
        'Content-Type': 'application/json'
    }

    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
        response.raise_for_status()
        
        return response.json()
        
    except requests.exceptions.RequestException as e:
        subdomain_logger.error(f"API request failed for {url}: {e}")
        return None
    except json.JSONDecodeError as e:
        subdomain_logger.error(f"Failed to parse JSON response from {url}: {e}")
        return None
    except Exception as e:
        subdomain_logger.error(f"Unexpected error during API request to {url}: {e}")
        return None


def get_connections(subdomain, base_url, auth_token, subdomain_logger):
    """
    Fetch connections data from Atlan connections API for a specific subdomain.
    
    Returns:
        list: List of dictionaries containing connection data
    """
    connections_config = config.get('connections_api', {})
    endpoint = connections_config.get('url', '/api/getConnections')
    payload = connections_config.get('payload', {})
    
    subdomain_logger.info(f"Fetching connections for subdomain: {subdomain}")
    
    response_data = make_api_request(endpoint, payload, base_url, auth_token, subdomain_logger)
    
    if not response_data:
        subdomain_logger.warning(f"Failed to fetch connections from Atlan API for {subdomain}")
        return []
    
    # Extract connection data from response
    entities = response_data.get('entities', [])
    
    connections = []
    for entity in entities:
        attributes = entity.get('attributes', {})
        connection = {
            'name': attributes.get('name', ''),
            'connection_qualified_name': attributes.get('qualifiedName', ''),
            'connector_name': attributes.get('connectorName', ''),
            'updated_by': entity.get('updatedBy', ''),
            'created_by': entity.get('createdBy', ''),
            'create_time': entity.get('createTime', ''),
            'update_time': entity.get('updateTime', '')
        }
        connections.append(connection)
    
    subdomain_logger.info(f"Successfully extracted {len(connections)} connections for {subdomain}")
    return connections


def get_databases(connection_qualified_name, connector_name, subdomain, base_url, auth_token, subdomain_logger):
    """
    Fetch databases associated with a specific connection for a subdomain.
    
    Args:
        connection_qualified_name (str): Unique identifier of the connection
        connector_name (str): Type of connector (e.g., databricks, snowflake)
        subdomain (str): Current subdomain being processed
        base_url (str): Base URL for the subdomain
        auth_token (str): Bearer token for authentication
        subdomain_logger: Logger instance for this subdomain
    
    Returns:
        list: List of dictionaries containing database data
    """
    # Determine which API configuration to use based on connector type
    api_map = config.get('api_map', {})
    api_config_key = api_map.get(connector_name, 'databases_api')
    
    api_config = config.get(api_config_key, {})
    if not api_config:
        subdomain_logger.warning(f"No API configuration found for connector: {connector_name}")
        return []
    
    endpoint = api_config.get('url', '/api/getDatabases')
    payload = api_config.get('payload', {})
    
    # Replace placeholder with actual connection qualified name
    payload_str = json.dumps(payload)
    payload_str = payload_str.replace('PLACEHOLDER_TO_BE_REPLACED', connection_qualified_name)
    modified_payload = json.loads(payload_str)
    
    response_data = make_api_request(endpoint, modified_payload, base_url, auth_token, subdomain_logger)
    
    if not response_data:
        return []
    
    # Extract database data from response
    entities = response_data.get('entities', [])
    
    databases = []
    for entity in entities:
        attributes = entity.get('attributes', {})
        database = {
            'connection_qualified_name': connection_qualified_name,
            'type_name': entity.get('typeName', ''),
            'qualified_name': attributes.get('qualifiedName', ''),
            'name': attributes.get('name', ''),
            'created_by': entity.get('createdBy', ''),
            'updated_by': entity.get('updatedBy', ''),
            'create_time': entity.get('createTime', ''),
            'update_time': entity.get('updateTime', '')
        }
        databases.append(database)
    
    return databases


def export_connections_to_csv(connections, subdomain, subdomain_logger):
    """
    Export connections data to CSV file with subdomain prefix.
    """
    if not connections:
        subdomain_logger.warning("No connections data to export")
        return

    filename = f'{subdomain}.connections_{timestamp}.csv'
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    subdomain_logger.info(f"Exporting {len(connections)} connections to {filename}")

    fieldnames = ['name', 'connection_qualified_name', 'connector_name', 'updated_by', 'created_by', 'create_time', 'update_time']
    
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(connections)
        
        subdomain_logger.info(f"Successfully exported connections to {filename}")
    
    except Exception as e:
        subdomain_logger.error(f"Failed to export connections to CSV: {e}")


def export_databases_to_csv(databases, subdomain, subdomain_logger):
    """
    Export databases data to CSV file with subdomain prefix.
    """
    if not databases:
        subdomain_logger.warning("No databases data to export")
        return

    filename = f'{subdomain}.databases_{timestamp}.csv'
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    subdomain_logger.info(f"Exporting {len(databases)} databases to {filename}")

    fieldnames = ['connection_qualified_name', 'type_name', 'qualified_name', 'name', 'created_by', 'updated_by', 'create_time', 'update_time']
    
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(databases)
        
        subdomain_logger.info(f"Successfully exported databases to {filename}")
    
    except Exception as e:
        subdomain_logger.error(f"Failed to export databases to CSV: {e}")


def create_combined_csv(connections, databases, subdomain, subdomain_logger):
    """
    Create a combined CSV file with subdomain as first column, using left join logic.
    """
    if not connections:
        subdomain_logger.warning("No connections data available for combined file")
        return

    filename = f'{subdomain}.connections-databases_{timestamp}.csv'
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    subdomain_logger.info(f"Creating combined file with {len(connections)} connections and {len(databases)} databases")

    # Create a lookup dictionary for databases by connection_qualified_name
    db_lookup = {}
    for db in databases:
        conn_name = db.get('connection_qualified_name', '')
        if conn_name not in db_lookup:
            db_lookup[conn_name] = []
        db_lookup[conn_name].append(db)

    # Define fieldnames with subdomain as first column
    fieldnames = ['subdomain', 'connector_name', 'connection_name', 'category', 'type_name', 'name']
    
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            # Left join: include all connections even if they have no databases
            for connection in connections:
                connection_qualified_name = connection.get('connection_qualified_name', '')
                connection_databases = db_lookup.get(connection_qualified_name, [])
                
                if connection_databases:
                    # Write one row for each database
                    for db in connection_databases:
                        combined_row = {
                            'subdomain': subdomain,
                            'connector_name': connection.get('connector_name', ''),
                            'connection_name': connection.get('name', ''),
                            'category': 'lake' if connection.get('connector_name') == 'databricks' else 
                                       'warehouse' if connection.get('connector_name') == 'snowflake' else 
                                       'visualization' if connection.get('connector_name') == 'tableau' else '',
                            'type_name': db.get('type_name', ''),
                            'name': db.get('name', '')
                        }
                        writer.writerow(combined_row)
                else:
                    # Write connection row with empty database fields (left join behavior)
                    combined_row = {
                        'subdomain': subdomain,
                        'connector_name': connection.get('connector_name', ''),
                        'connection_name': connection.get('name', ''),
                        'category': 'lake' if connection.get('connector_name') == 'databricks' else 
                                   'warehouse' if connection.get('connector_name') == 'snowflake' else 
                                   'visualization' if connection.get('connector_name') == 'tableau' else '',
                        'type_name': '',
                        'name': ''
                    }
                    writer.writerow(combined_row)
        
        subdomain_logger.info(f"Successfully created combined file: {filename}")
    
    except Exception as e:
        subdomain_logger.error(f"Failed to create combined CSV: {e}")


def process_subdomain(subdomain, auth_token):
    """
    Process data extraction for a single subdomain.
    
    Args:
        subdomain (str): The subdomain to process
        auth_token (str): Authentication token for this subdomain
        
    Returns:
        dict: Summary of processing results
    """
    # Create subdomain-specific base URL
    base_url = BASE_URL_TEMPLATE.format(subdomain=subdomain)
    
    # Set up subdomain-specific logging
    log_filename = os.path.join(LOGS_DIR, f'{subdomain}.atlan_extractor_{timestamp}.log')
    subdomain_logger = logging.getLogger(f"atlan_extractor_{subdomain}")
    subdomain_logger.handlers = []  # Clear any existing handlers
    
    # Add file handler for this subdomain
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    subdomain_logger.addHandler(file_handler)
    subdomain_logger.setLevel(logging.INFO)
    
    # Also log to console
    subdomain_logger.addHandler(logging.StreamHandler(sys.stdout))
    
    subdomain_logger.info(f"Starting data extraction for subdomain: {subdomain}")
    subdomain_logger.info(f"Using base URL: {base_url}")
    
    try:
        # Ensure proper Bearer token format
        if not auth_token.lower().startswith('bearer '):
            auth_token = f"Bearer {auth_token}"
        
        # Fetch connections for this subdomain
        connections = get_connections(subdomain, base_url, auth_token, subdomain_logger)
        
        if not connections:
            subdomain_logger.warning(f"No connections found for subdomain: {subdomain}")
            return {
                'subdomain': subdomain,
                'connections': 0,
                'databases': 0,
                'status': 'no_data'
            }
        
        # Export connections to CSV
        export_connections_to_csv(connections, subdomain, subdomain_logger)
        
        # Fetch databases for each connection
        all_databases = []
        for connection in connections:
            connection_qualified_name = connection.get('connection_qualified_name', '')
            connector_name = connection.get('connector_name', '')
            
            if connection_qualified_name:
                databases = get_databases(connection_qualified_name, connector_name, 
                                        subdomain, base_url, auth_token, subdomain_logger)
                if databases:
                    all_databases.extend(databases)
                    subdomain_logger.info(f"Found {len(databases)} databases for connection: {connection_qualified_name}")
        
        # Export databases to CSV
        export_databases_to_csv(all_databases, subdomain, subdomain_logger)
        
        # Create combined CSV with subdomain column
        create_combined_csv(connections, all_databases, subdomain, subdomain_logger)
        
        subdomain_logger.info(f"Completed processing for subdomain: {subdomain}")
        subdomain_logger.info(f"Total connections: {len(connections)}, Total databases: {len(all_databases)}")
        
        return {
            'subdomain': subdomain,
            'connections': len(connections),
            'databases': len(all_databases),
            'status': 'success'
        }
        
    except Exception as e:
        subdomain_logger.error(f"Error processing subdomain {subdomain}: {e}")
        return {
            'subdomain': subdomain,
            'connections': 0,
            'databases': 0,
            'status': 'error',
            'error': str(e)
        }


def main():
    """
    Main execution function that orchestrates multi-subdomain data extraction.
    """
    try:
        logger.info("Starting multi-subdomain data extraction process")
        
        # Step 1: Clean up old files to prevent disk space issues
        cleanup_old_files()
        
        # Step 2: Process each subdomain
        results = []
        for subdomain, token in SUBDOMAIN_AUTH_MAP.items():
            if not token:
                logger.warning(f"No authentication token found for subdomain: {subdomain}, skipping")
                continue
                
            result = process_subdomain(subdomain, token)
            results.append(result)
        
        # Step 3: Log overall completion summary
        successful_subdomains = [r for r in results if r['status'] == 'success']
        total_connections = sum(r['connections'] for r in successful_subdomains)
        total_databases = sum(r['databases'] for r in successful_subdomains)
        
        logger.info(f"Multi-subdomain extraction completed!")
        logger.info(f"Processed {len(successful_subdomains)}/{len(SUBDOMAIN_AUTH_MAP)} subdomains successfully")
        logger.info(f"Total connections across all subdomains: {total_connections}")
        logger.info(f"Total databases across all subdomains: {total_databases}")
        
        # Log any failed subdomains
        failed_subdomains = [r for r in results if r['status'] == 'error']
        if failed_subdomains:
            logger.warning(f"Failed subdomains: {[r['subdomain'] for r in failed_subdomains]}")
        
    except KeyboardInterrupt:
        logger.warning("Process interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error in main process: {e}")
        sys.exit(1)


if __name__ == "__main__":
    """
    Entry point for script execution.
    """
    main()