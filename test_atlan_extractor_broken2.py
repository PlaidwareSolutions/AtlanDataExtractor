#!/usr/bin/env python3
"""
Unit tests for Atlan Data Extractor - Completely Isolated

This module contains comprehensive test cases that are fully isolated
from external dependencies and configuration files.
"""

import unittest
import json
import csv
import os
import tempfile
import shutil
import sys
from unittest.mock import patch, mock_open, MagicMock


class TestAtlanExtractorIsolated(unittest.TestCase):
    """Isolated test cases for Atlan data extraction functions"""

    def setUp(self):
        """Set up test fixtures before each test method"""
        self.test_config = {
            "auth_token": "test_token_12345",
            "connections_api": {
                "url": "https://test-company.atlan.com/api/getConnections",
                "payload": {
                    "dsl": {"size": 400, "query": {}},
                    "attributes": ["connectorName", "isPartial"]
                }
            },
            "api_map": {
                "databricks": "databases_api",
                "snowflake": "databases_api"
            },
            "databases_api": {
                "url": "https://test-company.atlan.com/api/getDatabases",
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
            }
        }

    @patch('os.getenv')
    def test_get_auth_token_from_env(self, mock_getenv):
        """Test authentication token retrieval from environment variable"""
        mock_getenv.return_value = "env_token_123"
        
        # Mock the config loading to avoid file dependency
        with patch('builtins.open', mock_open(read_data=json.dumps(self.test_config))):
            with patch('json.load', return_value=self.test_config):
                with patch('os.path.exists', return_value=True):
                    with patch('os.makedirs'):
                        # Import and reload main to get fresh config
                        import main
                        token = main.get_auth_token()
                        
                        self.assertEqual(token, "Bearer env_token_123")

    @patch('os.getenv')
    def test_get_auth_token_from_config(self, mock_getenv):
        """Test authentication token retrieval from config file"""
        mock_getenv.return_value = None
        
        with patch('builtins.open', mock_open(read_data=json.dumps(self.test_config))):
            with patch('json.load', return_value=self.test_config):
                with patch('os.path.exists', return_value=True):
                    with patch('os.makedirs'):
                        import main
                        token = main.get_auth_token()
                        
                        self.assertEqual(token, "Bearer test_token_12345")

    @patch('os.getenv')
    @patch('sys.exit')
    def test_get_auth_token_missing(self, mock_exit, mock_getenv):
        """Test authentication token when none available"""
        mock_getenv.return_value = None
        config_no_token = self.test_config.copy()
        del config_no_token['auth_token']
        
        with patch('builtins.open', mock_open(read_data=json.dumps(config_no_token))):
            with patch('json.load', return_value=config_no_token):
                with patch('os.path.exists', return_value=True):
                    with patch('os.makedirs'):
                        import main
                        main.get_auth_token()
                        
                        mock_exit.assert_called_with(1)

    @patch('requests.post')
    def test_make_api_request_success_with_url_logging(self, mock_post):
        """Test successful API request with URL logging"""
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"success": True}
        mock_post.return_value = mock_response
        
        with patch('builtins.open', mock_open(read_data=json.dumps(self.test_config))):
            with patch('json.load', return_value=self.test_config):
                with patch('os.path.exists', return_value=True):
                    with patch('os.makedirs'):
                        with patch('main.get_auth_token', return_value="Bearer test_token"):
                            import main
                            
                            with patch('main.logger') as mock_logger:
                                result = main.make_api_request("https://test.com/api", {"test": "data"})
                                
                                # Verify URL logging
                                mock_logger.info.assert_called_with("Making API request to URL: https://test.com/api")
                            
                            self.assertEqual(result, {"success": True})

    @patch('requests.post')
    def test_make_api_request_http_error(self, mock_post):
        """Test API request with HTTP error"""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = Exception("HTTP Error")
        mock_response.status_code = 403
        mock_response.text = "Forbidden"
        mock_post.return_value = mock_response
        
        with patch('builtins.open', mock_open(read_data=json.dumps(self.test_config))):
            with patch('json.load', return_value=self.test_config):
                with patch('os.path.exists', return_value=True):
                    with patch('os.makedirs'):
                        with patch('main.get_auth_token', return_value="Bearer test_token"):
                            import main
                            result = main.make_api_request("https://test.com/api", {"test": "data"})
                            
                            self.assertIsNone(result)

    def test_get_connections_success(self):
        """Test successful connections retrieval"""
        mock_response = {
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
        
        with patch('builtins.open', mock_open(read_data=json.dumps(self.test_config))):
            with patch('json.load', return_value=self.test_config):
                with patch('os.path.exists', return_value=True):
                    with patch('os.makedirs'):
                        with patch('main.make_api_request', return_value=mock_response):
                            import main
                            connections = main.get_connections()
                            
                            self.assertEqual(len(connections), 1)
                            self.assertEqual(connections[0]['connection_name'], 'test-connection')
                            self.assertEqual(connections[0]['connector_name'], 'databricks')

    def test_get_connections_empty_response(self):
        """Test connections retrieval with empty response"""
        with patch('builtins.open', mock_open(read_data=json.dumps(self.test_config))):
            with patch('json.load', return_value=self.test_config):
                with patch('os.path.exists', return_value=True):
                    with patch('os.makedirs'):
                        with patch('main.make_api_request', return_value={"entities": []}):
                            import main
                            connections = main.get_connections()
                            
                            self.assertEqual(len(connections), 0)

    def test_get_databases_success_with_string_replacement(self):
        """Test successful databases retrieval using string replacement"""
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
        
        with patch('builtins.open', mock_open(read_data=json.dumps(self.test_config))):
            with patch('json.load', return_value=self.test_config):
                with patch('os.path.exists', return_value=True):
                    with patch('os.makedirs'):
                        with patch('main.make_api_request', return_value=mock_response) as mock_request:
                            import main
                            databases = main.get_databases("test/connection/1", "databricks")
                            
                            self.assertEqual(len(databases), 1)
                            self.assertEqual(databases[0]['name'], 'test-database')
                            self.assertEqual(databases[0]['connection_qualified_name'], 'test/connection/1')
                            
                            # Verify the API was called with the correctly replaced payload
                            call_args = mock_request.call_args
                            payload = call_args[0][1]  # Second argument (payload)
                            
                            # Check that the placeholder was replaced correctly
                            self.assertNotIn("PLACEHOLDER_TO_BE_REPLACED", json.dumps(payload))

    def test_get_databases_json_error(self):
        """Test databases retrieval with JSON serialization error"""
        with patch('builtins.open', mock_open(read_data=json.dumps(self.test_config))):
            with patch('json.load', return_value=self.test_config):
                with patch('os.path.exists', return_value=True):
                    with patch('os.makedirs'):
                        with patch('json.dumps', side_effect=TypeError("Object is not JSON serializable")):
                            import main
                            databases = main.get_databases("test/connection/1", "databricks")
                            
                            self.assertEqual(len(databases), 0)

    def test_get_databases_api_failure(self):
        """Test databases retrieval with API failure"""
        with patch('builtins.open', mock_open(read_data=json.dumps(self.test_config))):
            with patch('json.load', return_value=self.test_config):
                with patch('os.path.exists', return_value=True):
                    with patch('os.makedirs'):
                        with patch('main.make_api_request', return_value=None):
                            import main
                            databases = main.get_databases("test/connection/1", "databricks")
                            
                            self.assertEqual(len(databases), 0)

    def test_export_connections_to_csv_success(self):
        """Test successful connections CSV export"""
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
        
        test_dir = tempfile.mkdtemp()
        try:
            with patch('builtins.open', mock_open(read_data=json.dumps(self.test_config))):
                with patch('json.load', return_value=self.test_config):
                    with patch('os.path.exists', return_value=True):
                        with patch('os.makedirs'):
                            with patch('main.OUTPUT_DIR', test_dir):
                                import main
                                main.export_connections_to_csv(connections, 'test_connections.csv')
                                
                                csv_file = os.path.join(test_dir, 'test_connections.csv')
                                self.assertTrue(os.path.exists(csv_file))
                                
                                with open(csv_file, 'r') as f:
                                    reader = csv.DictReader(f)
                                    rows = list(reader)
                                    self.assertEqual(len(rows), 1)
                                    self.assertEqual(rows[0]['connection_name'], 'test-conn')
        finally:
            shutil.rmtree(test_dir)

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
        
        test_dir = tempfile.mkdtemp()
        try:
            with patch('builtins.open', mock_open(read_data=json.dumps(self.test_config))):
                with patch('json.load', return_value=self.test_config):
                    with patch('os.path.exists', return_value=True):
                        with patch('os.makedirs'):
                            with patch('main.OUTPUT_DIR', test_dir):
                                import main
                                main.export_databases_to_csv(databases, 'test_databases.csv')
                                
                                csv_file = os.path.join(test_dir, 'test_databases.csv')
                                self.assertTrue(os.path.exists(csv_file))
                                
                                with open(csv_file, 'r') as f:
                                    reader = csv.DictReader(f)
                                    rows = list(reader)
                                    self.assertEqual(len(rows), 1)
                                    self.assertEqual(rows[0]['name'], 'test-db')
        finally:
            shutil.rmtree(test_dir)

    def test_export_connections_to_csv_empty_data(self):
        """Test connections CSV export with empty data"""
        with patch('builtins.open', mock_open(read_data=json.dumps(self.test_config))):
            with patch('json.load', return_value=self.test_config):
                with patch('os.path.exists', return_value=True):
                    with patch('os.makedirs'):
                        import main
                        # Should not raise exception, just log warning
                        main.export_connections_to_csv([])

    def test_export_databases_to_csv_empty_data(self):
        """Test databases CSV export with empty data"""
        with patch('builtins.open', mock_open(read_data=json.dumps(self.test_config))):
            with patch('json.load', return_value=self.test_config):
                with patch('os.path.exists', return_value=True):
                    with patch('os.makedirs'):
                        import main
                        # Should not raise exception, just log warning
                        main.export_databases_to_csv([])

    @patch('main.export_databases_to_csv')
    @patch('main.export_connections_to_csv')
    @patch('main.get_databases')
    @patch('main.get_connections')
    def test_main_success(self, mock_get_conn, mock_get_db, mock_export_conn, mock_export_db):
        """Test successful main function execution"""
        mock_connections = [
            {
                'connection_qualified_name': 'test/conn/1',
                'connector_name': 'databricks'
            }
        ]
        mock_databases = [{'name': 'test-db'}]
        
        mock_get_conn.return_value = mock_connections
        mock_get_db.return_value = mock_databases
        
        with patch('builtins.open', mock_open(read_data=json.dumps(self.test_config))):
            with patch('json.load', return_value=self.test_config):
                with patch('os.path.exists', return_value=True):
                    with patch('os.makedirs'):
                        import main
                        main.main()
                        
                        mock_get_conn.assert_called_once()
                        mock_export_conn.assert_called_once_with(mock_connections)
                        mock_get_db.assert_called_once_with('test/conn/1', 'databricks')
                        mock_export_db.assert_called_once()

    @patch('main.get_connections')
    @patch('sys.exit')
    def test_main_no_connections(self, mock_exit, mock_get_conn):
        """Test main function with no connections"""
        mock_get_conn.return_value = []
        
        with patch('builtins.open', mock_open(read_data=json.dumps(self.test_config))):
            with patch('json.load', return_value=self.test_config):
                with patch('os.path.exists', return_value=True):
                    with patch('os.makedirs'):
                        import main
                        main.main()
                        
                        mock_exit.assert_called_with(1)


if __name__ == '__main__':
    unittest.main()