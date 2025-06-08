#!/usr/bin/env python3
"""
Atlan Data Extractor

This script extracts connections and databases data from Atlan APIs
and exports the data to CSV files.
"""

import json
import csv
import requests
import logging
import sys
import os

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler('atlan_extractor.log'),
                        logging.StreamHandler(sys.stdout)
                    ])

logger = logging.getLogger(__name__)

# Load config
with open('config.json', 'r') as f:
    config = json.load(f)


def get_auth_token():
    token = os.getenv('ATLAN_AUTH_TOKEN')
    if not token:
        token = config.get('auth_token', '')
    if token.lower().startswith('bearer '):
        return token
    return f'Bearer {token}'


def make_api_request(url, payload):
    headers = {
        'Authorization': get_auth_token(),
        'Content-Type': 'application/json'
    }
    try:
        response = requests.post(url,
                                 headers=headers,
                                 data=json.dumps(payload),
                                 timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"API request failed for {url}: {e}")
        logger.error(f"Response content: {getattr(response, 'text', '')}")
        return None


def get_connections():
    logger.info("Fetching connections from Atlan API")
    url = config['connections_api']['url']
    payload = config['connections_api']['payload'].copy()

    response = make_api_request(url, payload)
    if not response:
        logger.warning("Failed to fetch connections from Atlan API")
        return []

    entities = response.get('entities', [])
    logger.info(f"Retrieved {len(entities)} connections from Atlan API")
    connections = []
    for entity in entities:
        try:
            attributes = entity.get('attributes', {})
            connection_data = {
                'name': attributes.get('name', ''),
                'connection_qualified_name':
                attributes.get('qualifiedName', ''),
                'connector_name': attributes.get('connectorName', ''),
                'updated_by': entity.get('updatedBy', ''),
                'created_by': entity.get('createdBy', ''),
                'create_time': entity.get('createTime', ''),
                'update_time': entity.get('updateTime', '')
            }
            connections.append(connection_data)
            logger.debug(
                f"Processed connection: {connection_data['connection_name']}")
        except Exception as e:
            logger.warning(f"Failed to process connection entity: {e}")
            continue
    return connections


def get_databases(connection_qualified_name):
    logger.info(
        f"Fetching databases for connection: {connection_qualified_name}")
    url = config['databases_api']['url']
    payload = config['databases_api']['payload'].copy()
    try:
        payload['dsl']['query']['bool']['filter']['bool']['must'][2]['bool'][
            'filter']['term'][
                'connectionQualifiedName'] = connection_qualified_name
    except (KeyError, IndexError) as e:
        logger.error(
            f"Failed to update payload with connection qualified name: {e}")
        return []

    response = make_api_request(url, payload)
    if not response:
        logger.warning(
            f"Failed to fetch databases for connection: {connection_qualified_name}"
        )
        return []

    entities = response.get('entities', [])
    logger.info(
        f"Retrieved {len(entities)} databases for connection: {connection_qualified_name}"
    )
    databases = []
    for entity in entities:
        try:
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
            logger.warning(f"Processed database: {database_data['name']}")
        except Exception as e:
            logger.warning(f"Failed to process database entity: {e}")
            continue
    return databases


def export_connections_to_csv(connections, filename='connections.csv'):
    if not connections:
        logger.warning("No connections data to export")
        return
    logger.info(f"Exporting {len(connections)} connections to {filename}")
    fieldnames = [
        'connection_name', 'connection_qualified_name', 'connector_name',
        'created_by', 'updated_by', 'create_time', 'update_time'
    ]
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(connections)
            logger.info(f"Successfully exported connections to {filename}")
    except IOError as e:
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
