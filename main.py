#!/usr/bin/env python3
"""
Atlan Data Extractor

This script extracts connections and databases data from Atlan APIs
and exports the data to CSV files.

The script performs the following operations:
1. Loads configuration from config.json file
2. Authenticates using Bearer token (from environment variable or config)
3. Fetches connections data from Atlan connections API
4. Exports connections data to connections.csv
5. For each connection, fetches associated databases
6. Exports all databases data to databases.csv

Author: AI Assistant
Version: 1.0
Dependencies: requests, json, csv, logging, sys, os
"""

import json
import csv
import requests
import logging
import sys
import os

# Configure logging to both file and console for comprehensive monitoring
# Log level INFO provides detailed execution flow without debug verbosity
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler('atlan_extractor.log'),  # Persistent log file
                        logging.StreamHandler(sys.stdout)           # Real-time console output
                    ])

logger = logging.getLogger(__name__)

# Load configuration from JSON file containing API endpoints and payloads
# This file must exist in the same directory as the script
with open('config.json', 'r') as f:
    config = json.load(f)


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
        logger.error("No authentication token found. Please set ATLAN_AUTH_TOKEN environment variable or add auth_token to config.json")
        sys.exit(1)
    
    # Ensure proper Bearer format
    if token.lower().startswith('bearer '):
        return token
    return f'Bearer {token}'


def make_api_request(url, payload):
    """
    Make HTTP POST request to Atlan API with authentication and error handling.
    
    Args:
        url (str): API endpoint URL
        payload (dict): JSON payload for the POST request
        
    Returns:
        dict or None: JSON response from API, None if request fails
        
    Raises:
        None: All exceptions are caught and logged
    """
    # Prepare headers with authentication and content type
    headers = {
        'Authorization': get_auth_token(),
        'Content-Type': 'application/json'
    }
    
    response = None  # Initialize response variable for proper scope
    try:
        # Make POST request with timeout to prevent hanging
        response = requests.post(url,
                                 headers=headers,
                                 data=json.dumps(payload),
                                 timeout=30)
        # Raise exception for HTTP error status codes (4xx, 5xx)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        # Handle network, timeout, and HTTP errors
        logger.error(f"API request failed for {url}: {e}")
        if response is not None:
            logger.error(f"Response status: {response.status_code}")
            logger.error(f"Response content: {response.text}")
        return None
    except json.JSONDecodeError as e:
        # Handle invalid JSON response
        logger.error(f"Failed to parse JSON response from {url}: {e}")
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
                'connection_qualified_name': attributes.get('qualifiedName', ''),
                'connector_name': attributes.get('connectorName', ''),
                'category': attributes.get('category', ''),
                'updated_by': entity.get('updatedBy', ''),
                'created_by': entity.get('createdBy', ''),
                'create_time': entity.get('createTime', ''),
                'update_time': entity.get('updateTime', '')
            }
            connections.append(connection_data)
            logger.debug(f"Processed connection: {connection_data['connection_name']}")
        except Exception as e:
            # Log warning but continue processing other connections
            logger.warning(f"Failed to process connection entity: {e}")
            continue
    
    return connections


def get_databases(connection_qualified_name):
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
    logger.info(f"Fetching databases for connection: {connection_qualified_name}")
    
    # Get API endpoint and payload template from configuration
    url = config['databases_api']['url']
    payload = config['databases_api']['payload'].copy()
    
    # Update payload with specific connection qualified name
    # Navigate through nested dictionary structure to set the filter value
    try:
        payload['dsl']['query']['bool']['filter']['bool']['must'][2]['bool']['filter']['term']['connectionQualifiedName'] = connection_qualified_name
    except (KeyError, IndexError) as e:
        logger.error(f"Failed to update payload with connection qualified name: {e}")
        return []

    # Make API request to fetch databases for this connection
    response = make_api_request(url, payload)
    if not response:
        logger.warning(f"Failed to fetch databases for connection: {connection_qualified_name}")
        return []

    # Extract entities from response
    entities = response.get('entities', [])
    logger.info(f"Retrieved {len(entities)} databases for connection: {connection_qualified_name}")
    
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


def export_connections_to_csv(connections, filename='connections.csv'):
    """
    Export connections data to CSV file with proper formatting and error handling.
    
    Args:
        connections (list): List of connection dictionaries to export
        filename (str): Output CSV filename (default: 'connections.csv')
        
    Returns:
        None: Function writes to file and logs results
        
    Raises:
        None: All exceptions are caught and logged
    """
    # Check if there's data to export
    if not connections:
        logger.warning("No connections data to export")
        return
    
    logger.info(f"Exporting {len(connections)} connections to {filename}")
    
    # Define CSV column headers matching the connection data structure
    fieldnames = [
        'connection_name', 'connection_qualified_name', 'connector_name',
        'category', 'created_by', 'updated_by', 'create_time', 'update_time'
    ]
    
    try:
        # Write connections data to CSV file with UTF-8 encoding
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()  # Write column headers
            writer.writerows(connections)  # Write data rows
            logger.info(f"Successfully exported connections to {filename}")
    except IOError as e:
        # Handle file permission or disk space errors
        logger.error(f"Failed to write connections CSV file: {e}")


def export_databases_to_csv(databases, filename='databases.csv'):
    if not databases:
        logger.warning("No databases data to export")
        return
    logger.info(f"Exporting {len(databases)} databases to {filename}")
    fieldnames = [
        'type_name', 'qualified_name', 'name', 'created_by', 'updated_by',
        'create_time', 'update_time', 'connection_qualified_name'
    ]
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(databases)
            logger.info(f"Successfully exported databases to {filename}")
    except IOError as e:
        logger.error(f"Failed to write databases CSV file: {e}")


def main():
    connections = get_connections()
    if not connections:
        logger.error("No connections found. Exiting.")
        sys.exit(1)
    export_connections_to_csv(connections)

    all_databases = []
    for connection in connections:
        connection_qualified_name = connection.get('connection_qualified_name')
        if connection_qualified_name:
            databases = get_databases(connection_qualified_name)
            all_databases.extend(databases)
        else:
            logger.warning(f"Connection missing qualified name: {connection}")
    export_databases_to_csv(all_databases)
    logger.info(
        f"Data extraction completed successfully. Processed {len(connections)} connections and {len(all_databases)} databases."
    )


if __name__ == '__main__':
    main()
