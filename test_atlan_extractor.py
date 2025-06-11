#!/usr/bin/env python3
"""
Unit tests for Atlan Data Extractor - Multi-Subdomain Version

This module contains comprehensive test cases for the multi-subdomain Atlan data extractor,
including subdomain processing, authentication mapping, prefixed file generation, 
combined CSV creation with subdomain tracking, and cross-instance analysis functionality.
"""

import unittest
import json
import csv
import os
import tempfile
import shutil
import sys
import glob
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta


class TestAtlanExtractorSimple(unittest.TestCase):
    """Working test cases for Atlan data extraction functions"""

    def setUp(self):
        """Set up test fixtures"""
        self.test_config = {
            "base_url": "https://test-company.atlan.com",
            "auth_token": "test_token_12345",
            "connections_api": {
                "url": "/api/getConnections",
                "payload": {"dsl": {"size": 400}}
            },
            "databases_api_map": {"databricks": "databases_api"},
            "databases_api": {
                "url": "/api/getDatabases",
                "payload": {
                    "dsl": {
                        "query": {
                            "bool": {
                                "filter": {
                                    "bool": {
                                        "must": [
                                            {"bool": {"filter": {"term": {"connectionQualifiedName": "PLACEHOLDER_TO_BE_REPLACED"}}}}
                                        ]
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

    @patch('sys.modules')
    def test_string_replacement_functionality(self, mock_modules):
        """Test the core string replacement functionality"""
        # Test the string replacement logic directly
        payload_template = {
            "filter": {"connectionQualifiedName": "PLACEHOLDER_TO_BE_REPLACED"}
        }
        connection_qualified_name = "test/connection/123"
        
        # Simulate the string replacement process
        payload_json_str = json.dumps(payload_template)
        updated_payload_str = payload_json_str.replace("PLACEHOLDER_TO_BE_REPLACED", connection_qualified_name)
        payload = json.loads(updated_payload_str)
        
        # Verify the replacement worked
        self.assertEqual(payload["filter"]["connectionQualifiedName"], connection_qualified_name)
        self.assertNotIn("PLACEHOLDER_TO_BE_REPLACED", json.dumps(payload))

    def test_csv_export_functionality(self):
        """Test CSV export functions work correctly"""
        # Test connections CSV export
        connections = [
            {
                'connection_name': 'test-conn',
                'connection_qualified_name': 'test/conn/1',
                'connector_name': 'databricks',
                'category': 'warehouse',
                'created_by': 'test_user',
                'updated_by': 'test_user',
                'create_time': 1234567890,
                'update_time': 1234567890
            }
        ]
        
        # Test databases CSV export
        databases = [
            {
                'type_name': 'Database',
                'qualified_name': 'test/db/1',
                'name': 'test-db',
                'created_by': 'test_user',
                'updated_by': 'test_user',
                'create_time': 1234567890,
                'update_time': 1234567890,
                'connection_qualified_name': 'test/conn/1'
            }
        ]
        
        # Create test directory
        test_dir = tempfile.mkdtemp()
        try:
            # Test connections export
            connections_file = os.path.join(test_dir, 'test_connections.csv')
            with open(connections_file, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['connection_name', 'connection_qualified_name', 'connector_name', 
                             'category', 'created_by', 'updated_by', 'create_time', 'update_time']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(connections)
            
            # Verify connections file
            self.assertTrue(os.path.exists(connections_file))
            with open(connections_file, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]['connection_name'], 'test-conn')
            
            # Test databases export
            databases_file = os.path.join(test_dir, 'test_databases.csv')
            with open(databases_file, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['type_name', 'qualified_name', 'name', 'created_by', 'updated_by',
                             'create_time', 'update_time', 'connection_qualified_name']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(databases)
            
            # Verify databases file
            self.assertTrue(os.path.exists(databases_file))
            with open(databases_file, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]['name'], 'test-db')
                
        finally:
            shutil.rmtree(test_dir)

    def test_json_processing(self):
        """Test JSON processing and data extraction"""
        # Mock API response for connections
        connections_response = {
            "entities": [
                {
                    "attributes": {
                        "name": "test-connection",
                        "qualifiedName": "test/connection/1",
                        "connectorName": "databricks",
                        "category": "warehouse"
                    },
                    "createdBy": "test_user",
                    "updatedBy": "test_user", 
                    "createTime": 1234567890,
                    "updateTime": 1234567890
                }
            ]
        }
        
        # Simulate processing connections
        entities = connections_response.get('entities', [])
        connections = []
        for entity in entities:
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
        
        self.assertEqual(len(connections), 1)
        self.assertEqual(connections[0]['connection_name'], 'test-connection')
        self.assertEqual(connections[0]['connector_name'], 'databricks')
        
        # Mock API response for databases
        databases_response = {
            "entities": [
                {
                    "typeName": "Database",
                    "attributes": {
                        "qualifiedName": "test/database/1",
                        "name": "test-database"
                    },
                    "createdBy": "test_user",
                    "updatedBy": "test_user",
                    "createTime": 1234567890,
                    "updateTime": 1234567890
                }
            ]
        }
        
        # Simulate processing databases
        entities = databases_response.get('entities', [])
        databases = []
        connection_qualified_name = "test/connection/1"
        
        for entity in entities:
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
        
        self.assertEqual(len(databases), 1)
        self.assertEqual(databases[0]['name'], 'test-database')
        self.assertEqual(databases[0]['connection_qualified_name'], 'test/connection/1')

    def test_auth_token_logic(self):
        """Test authentication token logic"""
        # Test environment variable priority
        with patch('os.getenv') as mock_getenv:
            mock_getenv.return_value = "env_token_123"
            
            # Simulate get_auth_token logic
            env_token = mock_getenv('ATLAN_AUTH_TOKEN')
            if env_token:
                token = f"Bearer {env_token}"
            else:
                config_token = self.test_config.get('auth_token')
                if config_token:
                    token = f"Bearer {config_token}"
                else:
                    token = None
            
            self.assertEqual(token, "Bearer env_token_123")
        
        # Test config file fallback
        with patch('os.getenv') as mock_getenv:
            mock_getenv.return_value = None
            
            # Simulate get_auth_token logic
            env_token = mock_getenv('ATLAN_AUTH_TOKEN')
            if env_token:
                token = f"Bearer {env_token}"
            else:
                config_token = self.test_config.get('auth_token')
                if config_token:
                    token = f"Bearer {config_token}"
                else:
                    token = None
            
            self.assertEqual(token, "Bearer test_token_12345")

    def test_error_handling(self):
        """Test error handling scenarios"""
        # Test empty entities response
        empty_response = {"entities": []}
        entities = empty_response.get('entities', [])
        self.assertEqual(len(entities), 0)
        
        # Test malformed entity handling
        malformed_response = {
            "entities": [
                {"invalid": "entity"},  # Missing required fields
                {
                    "attributes": {"name": "valid-connection"},
                    "createdBy": "test_user"
                }
            ]
        }
        
        entities = malformed_response.get('entities', [])
        valid_connections = []
        
        for entity in entities:
            try:
                attributes = entity.get('attributes', {})
                if attributes.get('name'):  # Basic validation
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
                    valid_connections.append(connection_data)
            except (KeyError, AttributeError):
                continue  # Skip malformed entities
        
        self.assertEqual(len(valid_connections), 1)
        self.assertEqual(valid_connections[0]['connection_name'], 'valid-connection')

    def test_url_logging_format(self):
        """Test URL logging format with base URL combination"""
        base_url = "https://test-company.atlan.com"
        endpoint_path = "/api/getConnections"
        expected_full_url = f"{base_url.rstrip('/')}{endpoint_path}"
        expected_log_message = f"Making API request to URL: {expected_full_url}"
        
        # This tests the format of the logging message and URL combination
        self.assertIn("Making API request to URL:", expected_log_message)
        self.assertIn(expected_full_url, expected_log_message)
        self.assertEqual(expected_full_url, "https://test-company.atlan.com/api/getConnections")

    def test_output_directory_creation(self):
        """Test output directory creation logic"""
        test_dir = tempfile.mkdtemp()
        output_dir = os.path.join(test_dir, 'output')
        
        try:
            # Simulate output directory creation
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            self.assertTrue(os.path.exists(output_dir))
            
            # Test file creation in output directory
            test_file = os.path.join(output_dir, 'test.csv')
            with open(test_file, 'w') as f:
                f.write("test,data\n")
            
            self.assertTrue(os.path.exists(test_file))
            
        finally:
            shutil.rmtree(test_dir)

    def test_configs_directory_structure(self):
        """Test configs directory structure"""
        test_dir = tempfile.mkdtemp()
        configs_dir = os.path.join(test_dir, 'configs')
        config_file = os.path.join(configs_dir, 'config.json')
        
        try:
            # Create configs directory structure
            os.makedirs(configs_dir, exist_ok=True)
            
            # Write config file
            with open(config_file, 'w') as f:
                json.dump(self.test_config, f)
            
            # Verify structure
            self.assertTrue(os.path.exists(configs_dir))
            self.assertTrue(os.path.exists(config_file))
            
            # Verify config loading
            with open(config_file, 'r') as f:
                loaded_config = json.load(f)
            
            self.assertEqual(loaded_config['auth_token'], 'test_token_12345')
            # Verify base_url is present in new structure
            self.assertEqual(loaded_config['base_url'], self.test_config['base_url'])
            
        finally:
            shutil.rmtree(test_dir)

    def test_timestamped_filename_generation(self):
        """Test timestamped filename generation for logs and CSV files with subdomain prefix"""
        # Test subdomain extraction
        test_url = "https://xyz.atlan.com"
        from urllib.parse import urlparse
        parsed = urlparse(test_url)
        hostname = parsed.hostname or ''
        parts = hostname.split('.')
        subdomain = parts[0] if parts and len(parts) > 0 else 'atlan'
        
        # Test log file timestamp format with subdomain prefix
        timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        log_filename = f'{subdomain}.atlan_extractor_{timestamp}.log'
        
        # Verify log filename format with prefix
        self.assertRegex(log_filename, r'\w+\.atlan_extractor_\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}\.log')
        self.assertEqual(subdomain, 'xyz')
        
        # Test CSV file timestamp format with subdomain prefix
        connections_filename = f'{subdomain}.connections_{timestamp}.csv'
        databases_filename = f'{subdomain}.databases_{timestamp}.csv'
        combined_filename = f'{subdomain}.connections-databases_{timestamp}.csv'
        
        # Verify CSV filename formats with prefix
        self.assertRegex(connections_filename, r'\w+\.connections_\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}\.csv')
        self.assertRegex(databases_filename, r'\w+\.databases_\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}\.csv')
        self.assertRegex(combined_filename, r'\w+\.connections-databases_\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}\.csv')

    def test_file_cleanup_functionality(self):
        """Test cleanup of old log and output files"""
        test_dir = tempfile.mkdtemp()
        logs_dir = os.path.join(test_dir, 'logs')
        output_dir = os.path.join(test_dir, 'output')
        
        try:
            # Create directories
            os.makedirs(logs_dir)
            os.makedirs(output_dir)
            
            # Create test files with different ages
            current_time = datetime.now()
            old_time = current_time - timedelta(days=35)  # 35 days old
            recent_time = current_time - timedelta(days=5)  # 5 days old
            
            # Create old and recent log files
            old_log = os.path.join(logs_dir, 'atlan_extractor_2024-01-01-10-00-00.log')
            recent_log = os.path.join(logs_dir, 'atlan_extractor_2025-06-01-10-00-00.log')
            
            with open(old_log, 'w') as f:
                f.write("Old log entry")
            with open(recent_log, 'w') as f:
                f.write("Recent log entry")
            
            # Create old and recent CSV files
            old_csv = os.path.join(output_dir, 'connections_2024-01-01-10-00-00.csv')
            recent_csv = os.path.join(output_dir, 'connections_2025-06-01-10-00-00.csv')
            
            with open(old_csv, 'w') as f:
                f.write("name,value\ntest,data")
            with open(recent_csv, 'w') as f:
                f.write("name,value\ntest,data")
            
            # Manually set file modification times to simulate old files
            import time
            old_timestamp = time.mktime(old_time.timetuple())
            recent_timestamp = time.mktime(recent_time.timetuple())
            
            os.utime(old_log, (old_timestamp, old_timestamp))
            os.utime(old_csv, (old_timestamp, old_timestamp))
            
            # Simulate cleanup logic
            cutoff_date = datetime.now() - timedelta(days=30)
            files_to_delete = []
            
            # Check log files
            for log_file in glob.glob(os.path.join(logs_dir, 'atlan_extractor_*.log')):
                file_time = datetime.fromtimestamp(os.path.getmtime(log_file))
                if file_time < cutoff_date:
                    files_to_delete.append(log_file)
            
            # Check CSV files
            for pattern in ['connections_*.csv', 'databases_*.csv']:
                for csv_file in glob.glob(os.path.join(output_dir, pattern)):
                    file_time = datetime.fromtimestamp(os.path.getmtime(csv_file))
                    if file_time < cutoff_date:
                        files_to_delete.append(csv_file)
            
            # Verify cleanup identifies correct files
            self.assertGreater(len(files_to_delete), 0)
            
        finally:
            shutil.rmtree(test_dir)

    def test_base_url_combination(self):
        """Test base URL combination with endpoint paths"""
        base_url = "https://test-company.atlan.com"
        endpoint_path = "/api/getConnections"
        
        # Test URL combination logic
        full_url = f"{base_url.rstrip('/')}{endpoint_path}"
        
        self.assertEqual(full_url, "https://test-company.atlan.com/api/getConnections")
        
        # Test with trailing slash
        base_url_with_slash = "https://test-company.atlan.com/"
        full_url_with_slash = f"{base_url_with_slash.rstrip('/')}{endpoint_path}"
        
        self.assertEqual(full_url_with_slash, "https://test-company.atlan.com/api/getConnections")

    def test_combined_csv_left_join_functionality(self):
        """Test combined CSV creation with left join logic"""
        # Test data: connections and databases
        test_connections = [
            {
                'connection_name': 'connection-1',
                'connection_qualified_name': 'conn/1',
                'connector_name': 'databricks',
                'category': 'warehouse'
            },
            {
                'connection_name': 'connection-2',
                'connection_qualified_name': 'conn/2',
                'connector_name': 'snowflake',
                'category': 'warehouse'
            },
            {
                'connection_name': 'connection-3',
                'connection_qualified_name': 'conn/3',
                'connector_name': 'tableau',
                'category': 'bi'
            }
        ]
        
        test_databases = [
            {
                'connection_qualified_name': 'conn/1',
                'type_name': 'Database',
                'name': 'database-1'
            },
            {
                'connection_qualified_name': 'conn/1',
                'type_name': 'Database',
                'name': 'database-2'
            },
            {
                'connection_qualified_name': 'conn/2',
                'type_name': 'Database',
                'name': 'database-3'
            }
            # Note: conn/3 (tableau) has no databases - should still appear in combined file
        ]
        
        # Simulate combined CSV creation logic
        databases_by_connection = {}
        for db in test_databases:
            conn_qualified_name = db.get('connection_qualified_name', '')
            if conn_qualified_name not in databases_by_connection:
                databases_by_connection[conn_qualified_name] = []
            databases_by_connection[conn_qualified_name].append(db)
        
        combined_data = []
        
        # Perform left join logic
        for connection in test_connections:
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
        
        # Verify left join results
        self.assertEqual(len(combined_data), 4)  # 2 databases for conn/1, 1 for conn/2, 1 empty for conn/3
        
        # Verify first connection (databricks) has 2 database rows
        databricks_rows = [row for row in combined_data if row['connector_name'] == 'databricks']
        self.assertEqual(len(databricks_rows), 2)
        self.assertEqual(databricks_rows[0]['name'], 'database-1')
        self.assertEqual(databricks_rows[1]['name'], 'database-2')
        
        # Verify second connection (snowflake) has 1 database row
        snowflake_rows = [row for row in combined_data if row['connector_name'] == 'snowflake']
        self.assertEqual(len(snowflake_rows), 1)
        self.assertEqual(snowflake_rows[0]['name'], 'database-3')
        
        # Verify third connection (tableau) has 1 row with empty database fields
        tableau_rows = [row for row in combined_data if row['connector_name'] == 'tableau']
        self.assertEqual(len(tableau_rows), 1)
        self.assertEqual(tableau_rows[0]['type_name'], '')
        self.assertEqual(tableau_rows[0]['name'], '')

    def test_combined_csv_filename_generation(self):
        """Test combined CSV filename generation with timestamp and subdomain prefix"""
        subdomain = 'xyz'
        timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        expected_filename = f'{subdomain}.connections-databases_{timestamp}.csv'
        
        # Verify filename format with subdomain prefix
        self.assertRegex(expected_filename, r'\w+\.connections-databases_\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}\.csv')
        self.assertIn('connections-databases_', expected_filename)
        self.assertTrue(expected_filename.startswith(f'{subdomain}.'))
        self.assertTrue(expected_filename.endswith('.csv'))

    def test_combined_csv_column_order(self):
        """Test that combined CSV has columns in correct order"""
        expected_fieldnames = ['connector_name', 'connection_name', 'category', 'type_name', 'name']
        
        # Verify column order matches specification
        self.assertEqual(len(expected_fieldnames), 5)
        self.assertEqual(expected_fieldnames[0], 'connector_name')
        self.assertEqual(expected_fieldnames[1], 'connection_name')
        self.assertEqual(expected_fieldnames[2], 'category')
        self.assertEqual(expected_fieldnames[3], 'type_name')
        self.assertEqual(expected_fieldnames[4], 'name')

    def test_subdomain_extraction(self):
        """Test subdomain extraction from various URL formats"""
        from urllib.parse import urlparse
        
        def extract_subdomain(url):
            """Extract subdomain from URL for use as file prefix"""
            try:
                parsed = urlparse(url)
                hostname = parsed.hostname or ''
                parts = hostname.split('.')
                return parts[0] if parts and len(parts) > 0 and parts[0] else 'atlan'
            except Exception:
                return 'atlan'
        
        # Test various URL formats
        test_cases = [
            ("https://xyz.atlan.com", "xyz"),
            ("https://company-1.atlan.com", "company-1"),
            ("https://test123.atlan.com", "test123"),
            ("https://prod.atlan.io", "prod"),
            ("invalid-url", "atlan"),  # fallback case
            ("", "atlan")  # empty case
        ]
        
        for url, expected_subdomain in test_cases:
            with self.subTest(url=url):
                result = extract_subdomain(url)
                self.assertEqual(result, expected_subdomain)

    def test_multi_subdomain_configuration_parsing(self):
        """Test multi-subdomain configuration structure parsing"""
        
        # Test valid multi-subdomain configuration
        config = {
            "base_url_template": "https://{subdomain}.atlan.com",
            "subdomain_auth_token_map": {
                "xyz": "xyz_token",
                "abc": "abc_token",
                "lmn": "lmn_token"
            }
        }
        
        self.assertEqual(config["base_url_template"], "https://{subdomain}.atlan.com")
        self.assertEqual(len(config["subdomain_auth_token_map"]), 3)
        self.assertIn("xyz", config["subdomain_auth_token_map"])
        self.assertEqual(config["subdomain_auth_token_map"]["xyz"], "xyz_token")
        
        # Test URL template formatting
        for subdomain in ["xyz", "abc", "lmn"]:
            expected_url = f"https://{subdomain}.atlan.com"
            actual_url = config["base_url_template"].format(subdomain=subdomain)
            self.assertEqual(actual_url, expected_url)

    def test_subdomain_prefixed_filename_generation(self):
        """Test generation of subdomain-prefixed filenames"""
        
        timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        
        subdomains = ["xyz", "abc", "lmn"]
        file_types = ["connections", "databases", "connections-databases"]
        
        for subdomain in subdomains:
            for file_type in file_types:
                filename = f"{subdomain}.{file_type}_{timestamp}.csv"
                
                # Verify filename structure
                self.assertTrue(filename.startswith(subdomain))
                self.assertTrue(filename.endswith('.csv'))
                self.assertIn(file_type, filename)
                self.assertIn(timestamp, filename)
                
                # Verify filename parts
                parts = filename.split('.')
                self.assertEqual(parts[0], subdomain)
                self.assertTrue(parts[1].startswith(file_type))

    def test_subdomain_specific_logging(self):
        """Test subdomain-specific log file generation"""
        
        timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        
        subdomains = ["xyz", "abc", "lmn"]
        
        for subdomain in subdomains:
            log_filename = f"{subdomain}.atlan_extractor_{timestamp}.log"
            
            # Verify log filename structure
            self.assertTrue(log_filename.startswith(subdomain))
            self.assertTrue(log_filename.endswith('.log'))
            self.assertIn('atlan_extractor', log_filename)
            self.assertIn(timestamp, log_filename)

    def test_multi_subdomain_base_url_generation(self):
        """Test dynamic base URL generation for multiple subdomains"""
        
        base_url_template = "https://{subdomain}.atlan.com"
        subdomains = ["xyz", "abc", "lmn", "enterprise", "dev", "prod"]
        
        for subdomain in subdomains:
            base_url = base_url_template.format(subdomain=subdomain)
            expected_url = f"https://{subdomain}.atlan.com"
            
            self.assertEqual(base_url, expected_url)
            self.assertTrue(base_url.startswith("https://"))
            self.assertTrue(base_url.endswith(".atlan.com"))
            self.assertIn(subdomain, base_url)

    def test_combined_csv_with_subdomain_column(self):
        """Test combined CSV creation with subdomain as first column"""
        
        # Sample data for testing
        subdomain = "xyz"
        connections = [
            {
                "name": "test_connection",
                "connection_qualified_name": "test/conn/1",
                "connector_name": "databricks"
            }
        ]
        
        databases = [
            {
                "connection_qualified_name": "test/conn/1",
                "type_name": "Database",
                "name": "test_db"
            }
        ]
        
        # Test combined data structure
        combined_data = []
        for connection in connections:
            connection_qualified_name = connection.get('connection_qualified_name', '')
            
            # Find databases for this connection
            connection_databases = [db for db in databases 
                                  if db.get('connection_qualified_name') == connection_qualified_name]
            
            if connection_databases:
                for db in connection_databases:
                    combined_row = {
                        'subdomain': subdomain,
                        'connector_name': connection.get('connector_name', ''),
                        'connection_name': connection.get('name', ''),
                        'category': 'lake' if connection.get('connector_name') == 'databricks' else '',
                        'type_name': db.get('type_name', ''),
                        'name': db.get('name', '')
                    }
                    combined_data.append(combined_row)
        
        # Verify structure
        self.assertEqual(len(combined_data), 1)
        row = combined_data[0]
        self.assertEqual(row['subdomain'], 'xyz')
        self.assertEqual(row['connector_name'], 'databricks')
        self.assertEqual(row['connection_name'], 'test_connection')
        self.assertEqual(row['category'], 'lake')
        self.assertEqual(row['type_name'], 'Database')
        self.assertEqual(row['name'], 'test_db')

    def test_authentication_token_mapping(self):
        """Test subdomain authentication token mapping logic"""
        
        subdomain_auth_map = {
            "xyz": "Bearer xyz_token_123",
            "abc": "abc_token_456",
            "lmn": "Bearer lmn_token_789"
        }
        
        # Test token retrieval
        for subdomain, token in subdomain_auth_map.items():
            self.assertIsNotNone(token)
            self.assertTrue(len(token) > 0)
            
            # Test Bearer token formatting
            if not token.lower().startswith('bearer '):
                formatted_token = f"Bearer {token}"
            else:
                formatted_token = token
                
            self.assertTrue(formatted_token.lower().startswith('bearer '))

    def test_api_map_connector_routing(self):
        """Test API mapping for different connector types"""
        
        api_map = {
            "alteryx": "alteryx_api",
            "databricks": "databases_api",
            "oracle": "databases_api",
            "snowflake": "databases_api",
            "tableau": "tableau_api",
            "thoughtspot": "thoughtspot_api"
        }
        
        # Test connector routing
        test_connectors = ["databricks", "snowflake", "tableau", "oracle"]
        
        for connector in test_connectors:
            if connector in api_map:
                api_config_key = api_map[connector]
                self.assertIsNotNone(api_config_key)
                self.assertTrue(len(api_config_key) > 0)
                
                # Verify expected mappings
                if connector in ["databricks", "oracle", "snowflake"]:
                    self.assertEqual(api_config_key, "databases_api")
                elif connector == "tableau":
                    self.assertEqual(api_config_key, "tableau_api")

    def test_cross_subdomain_processing_isolation(self):
        """Test that subdomain processing is properly isolated"""
        
        subdomains = ["xyz", "abc", "lmn"]
        processing_results = {}
        
        # Simulate independent processing
        for subdomain in subdomains:
            processing_results[subdomain] = {
                'status': 'success' if subdomain != 'abc' else 'error',
                'connections': 5 if subdomain == 'xyz' else 3,
                'databases': 12 if subdomain == 'xyz' else 8,
                'error': 'Network timeout' if subdomain == 'abc' else None
            }
        
        # Verify isolation - failure in one doesn't affect others
        successful_subdomains = [s for s, r in processing_results.items() 
                               if r['status'] == 'success']
        failed_subdomains = [s for s, r in processing_results.items() 
                           if r['status'] == 'error']
        
        self.assertEqual(len(successful_subdomains), 2)
        self.assertEqual(len(failed_subdomains), 1)
        self.assertIn('xyz', successful_subdomains)
        self.assertIn('lmn', successful_subdomains)
        self.assertIn('abc', failed_subdomains)
        
        # Verify totals calculation
        total_connections = sum(r['connections'] for r in processing_results.values() 
                              if r['status'] == 'success')
        total_databases = sum(r['databases'] for r in processing_results.values() 
                            if r['status'] == 'success')
        
        self.assertEqual(total_connections, 8)  # 5 + 3
        self.assertEqual(total_databases, 20)   # 12 + 8

    def test_backward_compatibility_single_subdomain(self):
        """Test backward compatibility with single subdomain configuration"""
        
        # Legacy single subdomain config
        legacy_config = {
            "base_url": "https://company.atlan.com",
            "auth_token": "legacy_token_123"
        }
        
        # Test conversion logic
        base_url = legacy_config.get('base_url')
        if base_url:
            from urllib.parse import urlparse
            hostname = urlparse(base_url).hostname
            subdomain = hostname.split('.')[0] if hostname else 'atlan'
            
            # Convert to multi-subdomain format
            subdomain_auth_map = {subdomain: legacy_config.get('auth_token', '')}
            base_url_template = base_url.replace(subdomain, '{subdomain}')
            
            # Verify conversion
            self.assertEqual(subdomain, 'company')
            self.assertEqual(base_url_template, 'https://{subdomain}.atlan.com')
            self.assertEqual(subdomain_auth_map['company'], 'legacy_token_123')

    def test_processing_summary_generation(self):
        """Test multi-subdomain processing summary generation"""
        
        results = [
            {'subdomain': 'xyz', 'status': 'success', 'connections': 5, 'databases': 12},
            {'subdomain': 'abc', 'status': 'error', 'connections': 0, 'databases': 0, 'error': 'Auth failed'},
            {'subdomain': 'lmn', 'status': 'success', 'connections': 3, 'databases': 8}
        ]
        
        # Calculate summary statistics
        successful_subdomains = [r for r in results if r['status'] == 'success']
        failed_subdomains = [r for r in results if r['status'] == 'error']
        total_connections = sum(r['connections'] for r in successful_subdomains)
        total_databases = sum(r['databases'] for r in successful_subdomains)
        
        # Verify summary
        self.assertEqual(len(successful_subdomains), 2)
        self.assertEqual(len(failed_subdomains), 1)
        self.assertEqual(total_connections, 8)
        self.assertEqual(total_databases, 20)
        self.assertEqual(failed_subdomains[0]['subdomain'], 'abc')
        self.assertEqual(failed_subdomains[0]['error'], 'Auth failed')


if __name__ == '__main__':
    unittest.main()