#!/usr/bin/env python3
"""
Unit tests for Atlan Data Extractor

This module contains comprehensive test cases for the AtlanExtractor class
to ensure proper functionality and error handling.
"""

import unittest
import json
import csv
import os
import tempfile
import shutil
from unittest.mock import patch, mock_open, MagicMock
from main import AtlanExtractor


class TestAtlanExtractor(unittest.TestCase):
    """Test cases for AtlanExtractor class"""

    def setUp(self):
        """Set up test fixtures before each test method"""
        self.test_dir = tempfile.mkdtemp()
        self.config_file = os.path.join(self.test_dir, 'test_config.json')
        self.test_config = {
            "connections_api": {
                "url": "https://test.atlan.com/api/getConnections",
                "payload": {
                    "dsl": {"size": 400, "query": {}},
                    "attributes": ["connectorName", "isPartial"]
                }
            },
            "databases_api": {
                "url": "https://test.atlan.com/api/getDatabases",
                "payload": {
                    "dsl": {
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
                    "attributes": ["name", "displayName"]
                }
            },
            "auth_token": "test_token"
        }
        
        with open(self.config_file, 'w') as f:
            json.dump(self.test_config, f)

    def tearDown(self):
        """Clean up after each test method"""
        shutil.rmtree(self.test_dir)

    @patch.dict(os.environ, {'ATLAN_AUTH_TOKEN': 'env_token'})
    def test_init_with_valid_config(self):
        """Test initialization with valid configuration file"""
        extractor = AtlanExtractor(self.config_file)
        self.assertEqual(extractor.config, self.test_config)
        self.assertIsNotNone(extractor.session)

    def test_init_with_missing_config_file(self):
        """Test initialization with missing configuration file"""
        with self.assertRaises(SystemExit):
            AtlanExtractor('nonexistent_config.json')

    def test_init_with_invalid_json(self):
        """Test initialization with invalid JSON configuration"""
        invalid_config = os.path.join(self.test_dir, 'invalid.json')
        with open(invalid_config, 'w') as f:
            f.write('{"invalid": json}')
        
        with self.assertRaises(SystemExit):
            AtlanExtractor(invalid_config)

    @patch.dict(os.environ, {'ATLAN_AUTH_TOKEN': 'env_token'})
    def test_setup_session_with_env_token(self):
        """Test session setup with environment variable token"""
        extractor = AtlanExtractor(self.config_file)
        self.assertEqual(extractor.session.headers['Authorization'], 'Bearer env_token')
        self.assertEqual(extractor.session.headers['Content-Type'], 'application/json')

    @patch.dict(os.environ, {}, clear=True)
    def test_setup_session_with_config_token(self):
        """Test session setup with configuration file token"""
        extractor = AtlanExtractor(self.config_file)
        self.assertEqual(extractor.session.headers['Authorization'], 'Bearer test_token')

    @patch.dict(os.environ, {}, clear=True)
    def test_setup_session_without_token(self):
        """Test session setup without any authentication token"""
        config_without_token = self.test_config.copy()
        del config_without_token['auth_token']
        
        config_file = os.path.join(self.test_dir, 'no_token_config.json')
        with open(config_file, 'w') as f:
            json.dump(config_without_token, f)
        
        with self.assertRaises(SystemExit):
            AtlanExtractor(config_file)

    @patch('main.AtlanExtractor._make_api_request')
    def test_get_connections_success(self, mock_request):
        """Test successful connections retrieval"""
        mock_response = {
            "entities": [
                {
                    "typeName": "Connection",
                    "attributes": {
                        "qualifiedName": "test/connection/1",
                        "name": "test-connection",
                        "connectorName": "test-connector",
                        "category": "test-category"
                    },
                    "createdBy": "test_user",
                    "updatedBy": "test_user",
                    "createTime": 1234567890,
                    "updateTime": 1234567890
                }
            ]
        }
        mock_request.return_value = mock_response
        
        extractor = AtlanExtractor(self.config_file)
        connections = extractor.get_connections()
        
        self.assertEqual(len(connections), 1)
        self.assertEqual(connections[0]['connection_name'], 'test-connection')
        self.assertEqual(connections[0]['connection_qualified_name'], 'test/connection/1')

    @patch('main.AtlanExtractor._make_api_request')
    def test_get_connections_api_failure(self, mock_request):
        """Test connections retrieval with API failure"""
        mock_request.return_value = None
        
        extractor = AtlanExtractor(self.config_file)
        connections = extractor.get_connections()
        
        # Should return empty list when API fails
        self.assertEqual(len(connections), 0)

    @patch('main.AtlanExtractor._make_api_request')
    def test_get_connections_empty_response(self, mock_request):
        """Test connections retrieval with empty response"""
        mock_request.return_value = {"entities": []}
        
        extractor = AtlanExtractor(self.config_file)
        connections = extractor.get_connections()
        
        self.assertEqual(len(connections), 0)

    @patch('main.AtlanExtractor._make_api_request')
    def test_get_connections_malformed_entity(self, mock_request):
        """Test connections retrieval with malformed entity data"""
        mock_response = {
            "entities": [
                {"invalid": "entity"},  # Missing required fields
                {
                    "attributes": {
                        "name": "valid-connection",
                        "qualifiedName": "test/connection/2"
                    },
                    "createdBy": "test_user"
                }
            ]
        }
        mock_request.return_value = mock_response
        
        extractor = AtlanExtractor(self.config_file)
        connections = extractor.get_connections()
        
        # Should process valid entity and skip invalid one - only one valid connection should be processed
        valid_connections = [c for c in connections if c['connection_name'] == 'valid-connection']
        self.assertEqual(len(valid_connections), 1)
        self.assertEqual(valid_connections[0]['connection_name'], 'valid-connection')

    @patch('main.AtlanExtractor._make_api_request')
    def test_get_databases_success(self, mock_request):
        """Test successful databases retrieval"""
        mock_response = {
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
        mock_request.return_value = mock_response
        
        extractor = AtlanExtractor(self.config_file)
        databases = extractor.get_databases("test/connection/1")
        
        self.assertEqual(len(databases), 1)
        self.assertEqual(databases[0]['name'], 'test-database')
        self.assertEqual(databases[0]['connection_qualified_name'], 'test/connection/1')

    @patch('main.AtlanExtractor._make_api_request')
    def test_get_databases_api_failure_databricks(self, mock_request):
        """Test databases retrieval with API failure for databricks connection"""
        mock_request.return_value = None
        
        extractor = AtlanExtractor(self.config_file)
        databases = extractor.get_databases("default/databricks/12345")
        
        # Should return empty list when API fails
        self.assertEqual(len(databases), 0)

    @patch('main.AtlanExtractor._make_api_request')
    def test_get_databases_api_failure_non_databricks(self, mock_request):
        """Test databases retrieval with API failure for non-databricks connection"""
        mock_request.return_value = None
        
        extractor = AtlanExtractor(self.config_file)
        databases = extractor.get_databases("default/api/12345")
        
        # Should return empty list when API fails
        self.assertEqual(len(databases), 0)

    def test_get_databases_payload_update_failure(self):
        """Test databases retrieval with payload update failure"""
        # Create config with malformed databases payload
        malformed_config = self.test_config.copy()
        malformed_config['databases_api']['payload'] = {"invalid": "structure"}
        
        config_file = os.path.join(self.test_dir, 'malformed_config.json')
        with open(config_file, 'w') as f:
            json.dump(malformed_config, f)
        
        extractor = AtlanExtractor(config_file)
        databases = extractor.get_databases("test/connection/1")
        
        self.assertEqual(len(databases), 0)

    @patch('main.AtlanExtractor._make_api_request')
    def test_get_databases_malformed_entity(self, mock_request):
        """Test databases retrieval with malformed entity data"""
        mock_response = {
            "entities": [
                {"invalid": "entity"},  # Missing required fields
                {
                    "typeName": "Database",
                    "attributes": {"name": "valid-database"},
                    "createdBy": "test_user"
                }
            ]
        }
        mock_request.return_value = mock_response
        
        extractor = AtlanExtractor(self.config_file)
        databases = extractor.get_databases("test/connection/1")
        
        # Should process valid entity and skip invalid one
        valid_databases = [d for d in databases if d['name'] == 'valid-database']
        self.assertEqual(len(valid_databases), 1)
        self.assertEqual(valid_databases[0]['name'], 'valid-database')

    @patch('requests.Session.post')
    def test_make_api_request_success(self, mock_post):
        """Test successful API request"""
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"success": True}
        mock_post.return_value = mock_response
        
        extractor = AtlanExtractor(self.config_file)
        result = extractor._make_api_request("https://test.com", {"test": "data"})
        
        self.assertEqual(result, {"success": True})

    @patch('requests.Session.post')
    def test_make_api_request_http_error(self, mock_post):
        """Test API request with HTTP error"""
        import requests
        mock_post.side_effect = requests.exceptions.RequestException("HTTP Error")
        
        extractor = AtlanExtractor(self.config_file)
        result = extractor._make_api_request("https://test.com", {"test": "data"})
        
        self.assertIsNone(result)

    @patch('requests.Session.post')
    def test_make_api_request_json_decode_error(self, mock_post):
        """Test API request with JSON decode error"""
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        mock_post.return_value = mock_response
        
        extractor = AtlanExtractor(self.config_file)
        result = extractor._make_api_request("https://test.com", {"test": "data"})
        
        self.assertIsNone(result)

    def test_export_connections_to_csv_success(self):
        """Test successful connections CSV export"""
        connections = [
            {
                'connection_name': 'test-conn',
                'connection_qualified_name': 'test/conn/1',
                'connector_name': 'test-connector',
                'category': 'test-category',
                'created_by': 'test_user',
                'updated_by': 'test_user',
                'create_time': 1234567890,
                'update_time': 1234567890
            }
        ]
        
        extractor = AtlanExtractor(self.config_file)
        csv_file = os.path.join(self.test_dir, 'test_connections.csv')
        extractor.export_connections_to_csv(connections, csv_file)
        
        self.assertTrue(os.path.exists(csv_file))
        
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['connection_name'], 'test-conn')

    def test_export_connections_to_csv_empty_data(self):
        """Test connections CSV export with empty data"""
        extractor = AtlanExtractor(self.config_file)
        csv_file = os.path.join(self.test_dir, 'empty_connections.csv')
        extractor.export_connections_to_csv([], csv_file)
        
        self.assertFalse(os.path.exists(csv_file))

    @patch('builtins.open', side_effect=IOError("Permission denied"))
    def test_export_connections_to_csv_io_error(self, mock_open):
        """Test connections CSV export with IO error"""
        connections = [{'connection_name': 'test'}]
        
        extractor = AtlanExtractor(self.config_file)
        # Should not raise exception, just log error
        extractor.export_connections_to_csv(connections, '/invalid/path/file.csv')

    def test_export_databases_to_csv_success(self):
        """Test successful databases CSV export"""
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
        
        extractor = AtlanExtractor(self.config_file)
        csv_file = os.path.join(self.test_dir, 'test_databases.csv')
        extractor.export_databases_to_csv(databases, csv_file)
        
        self.assertTrue(os.path.exists(csv_file))
        
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['name'], 'test-db')

    def test_export_databases_to_csv_empty_data(self):
        """Test databases CSV export with empty data"""
        extractor = AtlanExtractor(self.config_file)
        csv_file = os.path.join(self.test_dir, 'empty_databases.csv')
        extractor.export_databases_to_csv([], csv_file)
        
        self.assertFalse(os.path.exists(csv_file))

    @patch('builtins.open', side_effect=IOError("Permission denied"))
    def test_export_databases_to_csv_io_error(self, mock_open):
        """Test databases CSV export with IO error"""
        databases = [{'name': 'test'}]
        
        extractor = AtlanExtractor(self.config_file)
        # Should not raise exception, just log error
        extractor.export_databases_to_csv(databases, '/invalid/path/file.csv')

    @patch('main.AtlanExtractor.get_connections')
    @patch('main.AtlanExtractor.get_databases')
    @patch('main.AtlanExtractor.export_connections_to_csv')
    @patch('main.AtlanExtractor.export_databases_to_csv')
    def test_run_success(self, mock_export_db, mock_export_conn, mock_get_db, mock_get_conn):
        """Test successful complete workflow execution"""
        mock_connections = [
            {'connection_qualified_name': 'test/conn/1'},
            {'connection_qualified_name': 'test/conn/2'}
        ]
        mock_databases = [{'name': 'db1'}, {'name': 'db2'}]
        
        mock_get_conn.return_value = mock_connections
        mock_get_db.return_value = mock_databases
        
        extractor = AtlanExtractor(self.config_file)
        extractor.run()
        
        mock_export_conn.assert_called_once_with(mock_connections)
        mock_export_db.assert_called_once_with(mock_databases * 2)  # 2 databases per connection
        self.assertEqual(mock_get_db.call_count, 2)  # Called for each connection

    @patch('main.AtlanExtractor.get_connections')
    def test_run_no_connections(self, mock_get_conn):
        """Test workflow execution with no connections"""
        mock_get_conn.return_value = []
        
        extractor = AtlanExtractor(self.config_file)
        with self.assertRaises(SystemExit):
            extractor.run()

    @patch('main.AtlanExtractor.get_connections')
    @patch('main.AtlanExtractor.get_databases')
    @patch('main.AtlanExtractor.export_connections_to_csv')
    @patch('main.AtlanExtractor.export_databases_to_csv')
    def test_run_connection_without_qualified_name(self, mock_export_db, mock_export_conn, mock_get_db, mock_get_conn):
        """Test workflow execution with connection missing qualified name"""
        mock_connections = [
            {'connection_qualified_name': 'test/conn/1'},
            {'connection_name': 'invalid_conn'}  # Missing qualified_name
        ]
        mock_databases = [{'name': 'db1'}]
        
        mock_get_conn.return_value = mock_connections
        mock_get_db.return_value = mock_databases
        
        extractor = AtlanExtractor(self.config_file)
        extractor.run()
        
        # Should only call get_databases once for the valid connection
        mock_get_db.assert_called_once_with('test/conn/1')
        mock_export_db.assert_called_once_with(mock_databases)


class TestMainFunction(unittest.TestCase):
    """Test cases for main function"""

    @patch('main.AtlanExtractor')
    def test_main_success(self, mock_extractor_class):
        """Test successful main function execution"""
        mock_extractor = MagicMock()
        mock_extractor_class.return_value = mock_extractor
        
        from main import main
        main()
        
        mock_extractor_class.assert_called_once()
        mock_extractor.run.assert_called_once()

    @patch('main.AtlanExtractor')
    def test_main_keyboard_interrupt(self, mock_extractor_class):
        """Test main function with keyboard interrupt"""
        mock_extractor = MagicMock()
        mock_extractor.run.side_effect = KeyboardInterrupt()
        mock_extractor_class.return_value = mock_extractor
        
        from main import main
        with self.assertRaises(SystemExit):
            main()

    @patch('main.AtlanExtractor')
    def test_main_unexpected_error(self, mock_extractor_class):
        """Test main function with unexpected error"""
        mock_extractor = MagicMock()
        mock_extractor.run.side_effect = Exception("Unexpected error")
        mock_extractor_class.return_value = mock_extractor
        
        from main import main
        with self.assertRaises(SystemExit):
            main()


if __name__ == '__main__':
    unittest.main()