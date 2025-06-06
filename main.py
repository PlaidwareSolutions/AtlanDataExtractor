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
from typing import List, Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('atlan_extractor.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class AtlanExtractor:
    """Main class for extracting data from Atlan APIs"""
    
    def __init__(self, config_file: str = 'config.json'):
        """Initialize the extractor with configuration"""
        self.config = self._load_config(config_file)
        self.session = requests.Session()
        self._setup_session()
        
    def _load_config(self, config_file: str) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            logger.info(f"Configuration loaded from {config_file}")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file {config_file} not found")
            sys.exit(1)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in configuration file: {e}")
            sys.exit(1)
            
    def _setup_session(self):
        """Setup requests session with headers"""
        auth_token = os.getenv('ATLAN_AUTH_TOKEN', self.config.get('auth_token', ''))
        if not auth_token:
            logger.error("No authentication token provided. Set ATLAN_AUTH_TOKEN environment variable or add auth_token to config.json")
            sys.exit(1)
            
        self.session.headers.update({
            'Authorization': f"Bearer {auth_token}",
            'Content-Type': 'application/json'
        })
        logger.info("Session configured with authentication headers")
        
    def _make_api_request(self, url: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Make a POST API request and return JSON response"""
        try:
            logger.info(f"Making API request to {url}")
            response = self.session.post(url, json=payload, timeout=30)
            response.raise_for_status()
            
            json_response = response.json()
            logger.info(f"API request successful. Response contains {len(json_response.get('entities', []))} entities")
            return json_response
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {url}: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response from {url}: {e}")
            return None
            
    def get_connections(self) -> List[Dict[str, Any]]:
        """Fetch connections from Atlan API"""
        logger.info("Fetching connections from Atlan API")
        
        url = self.config['connections_api']['url']
        payload = self.config['connections_api']['payload']
        
        response = self._make_api_request(url, payload)
        if not response:
            logger.error("Failed to fetch connections from Atlan API")
            logger.error("Please verify your configuration:")
            logger.error(f"  - API URL: {url}")
            logger.error("  - Authentication token (ATLAN_AUTH_TOKEN environment variable)")
            logger.error("  - Network connectivity to your Atlan instance")
            return []
            
        entities = response.get('entities', [])
        logger.info(f"Retrieved {len(entities)} connections")
        
        # Extract connection data
        connections = []
        for entity in entities:
            try:
                attributes = entity.get('attributes', {})
                connection_data = {
                    'connection_name': attributes.get('name', ''),
                    'connection_qualified_name': attributes.get('qualifiedName', ''),
                    'connector_name': attributes.get('connectorName', ''),
                    'category': attributes.get('category', ''),
                    'created_by': entity.get('createdBy', ''),
                    'updated_by': entity.get('updatedBy', ''),
                    'create_time': entity.get('createTime', ''),
                    'update_time': entity.get('updateTime', '')
                }
                connections.append(connection_data)
                logger.debug(f"Processed connection: {connection_data['connection_name']}")
                
            except Exception as e:
                logger.warning(f"Failed to process connection entity: {e}")
                continue
                
        return connections
        
    def get_databases(self, connection_qualified_name: str) -> List[Dict[str, Any]]:
        """Fetch databases for a specific connection"""
        logger.info(f"Fetching databases for connection: {connection_qualified_name}")
        
        url = self.config['databases_api']['url']
        payload = self.config['databases_api']['payload'].copy()
        
        # Replace the connectionQualifiedName in the payload
        try:
            payload['dsl']['query']['bool']['filter']['bool']['must'][2]['bool']['filter']['term']['connectionQualifiedName'] = connection_qualified_name
        except (KeyError, IndexError) as e:
            logger.error(f"Failed to update payload with connection qualified name: {e}")
            return []
            
        response = self._make_api_request(url, payload)
        if not response:
            logger.warning(f"Failed to fetch databases for connection: {connection_qualified_name}")
            return []
            
        entities = response.get('entities', [])
        logger.info(f"Retrieved {len(entities)} databases for connection: {connection_qualified_name}")
        
        # Extract database data
        databases = []
        for entity in entities:
            try:
                attributes = entity.get('attributes', {})
                database_data = {
                    'type_name': entity.get('typeName', ''),
                    'qualified_name': attributes.get('qualifiedName', ''),
                    'name': attributes.get('name', ''),
                    'created_by': entity.get('createdBy', ''),
                    'updated_by': entity.get('updatedBy', ''),
                    'create_time': entity.get('createTime', ''),
                    'update_time': entity.get('updateTime', ''),
                    'connection_qualified_name': connection_qualified_name
                }
                databases.append(database_data)
                logger.debug(f"Processed database: {database_data['name']}")
                
            except Exception as e:
                logger.warning(f"Failed to process database entity: {e}")
                continue
                
        return databases
        
    def export_connections_to_csv(self, connections: List[Dict[str, Any]], filename: str = 'connections.csv'):
        """Export connections data to CSV file"""
        if not connections:
            logger.warning("No connections data to export")
            return
            
        logger.info(f"Exporting {len(connections)} connections to {filename}")
        
        fieldnames = [
            'connection_name', 'connection_qualified_name', 'connector_name', 
            'category', 'created_by', 'updated_by', 'create_time', 'update_time'
        ]
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(connections)
            logger.info(f"Successfully exported connections to {filename}")
            
        except IOError as e:
            logger.error(f"Failed to write connections CSV file: {e}")
            
    def export_databases_to_csv(self, databases: List[Dict[str, Any]], filename: str = 'databases.csv'):
        """Export databases data to CSV file"""
        if not databases:
            logger.warning("No databases data to export")
            return
            
        logger.info(f"Exporting {len(databases)} databases to {filename}")
        
        fieldnames = [
            'type_name', 'qualified_name', 'name', 'created_by', 
            'updated_by', 'create_time', 'update_time', 'connection_qualified_name'
        ]
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(databases)
            logger.info(f"Successfully exported databases to {filename}")
            
        except IOError as e:
            logger.error(f"Failed to write databases CSV file: {e}")
            
    def run(self):
        """Main execution method"""
        logger.info("Starting Atlan data extraction process")
        
        # Step 1: Get connections
        connections = self.get_connections()
        if not connections:
            logger.error("No connections found. Exiting.")
            sys.exit(1)
            
        # Step 2: Export connections to CSV
        self.export_connections_to_csv(connections)
        
        # Step 3: Get databases for each connection
        all_databases = []
        for connection in connections:
            connection_qualified_name = connection.get('connection_qualified_name')
            if connection_qualified_name:
                databases = self.get_databases(connection_qualified_name)
                all_databases.extend(databases)
            else:
                logger.warning(f"Connection missing qualified name: {connection}")
                
        # Step 4: Export databases to CSV
        self.export_databases_to_csv(all_databases)
        
        logger.info(f"Data extraction completed successfully. Processed {len(connections)} connections and {len(all_databases)} databases.")


def main():
    """Main entry point"""
    try:
        extractor = AtlanExtractor()
        extractor.run()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
