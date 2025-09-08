import unittest
from unittest.mock import patch
from click.testing import CliRunner
from omop2neo4j_lpg.cli import cli

class TestCli(unittest.TestCase):

    def setUp(self):
        self.runner = CliRunner()

    @patch('omop2neo4j_lpg.extraction.export_tables_to_csv')
    def test_extract_command(self, mock_export):
        result = self.runner.invoke(cli, ['extract'])
        self.assertEqual(result.exit_code, 0)
        mock_export.assert_called_once()

    @patch('omop2neo4j_lpg.loading.clear_database')
    @patch('omop2neo4j_lpg.loading.get_driver')
    def test_clear_db_command(self, mock_get_driver, mock_clear):
        result = self.runner.invoke(cli, ['clear-db'])
        self.assertEqual(result.exit_code, 0)
        mock_get_driver.assert_called_once()
        mock_clear.assert_called_once()

    @patch('omop2neo4j_lpg.loading.run_load_csv')
    def test_load_csv_command(self, mock_run_load):
        # Test without option
        result = self.runner.invoke(cli, ['load-csv'])
        self.assertEqual(result.exit_code, 0)
        mock_run_load.assert_called_with(batch_size=None)

        # Test with option
        result = self.runner.invoke(cli, ['load-csv', '--batch-size', '5000'])
        self.assertEqual(result.exit_code, 0)
        mock_run_load.assert_called_with(batch_size=5000)

    @patch('omop2neo4j_lpg.transformation.prepare_for_bulk_import')
    def test_prepare_bulk_command(self, mock_prepare_bulk):
        mock_prepare_bulk.return_value = "neo4j-admin command"
        result = self.runner.invoke(cli, ['prepare-bulk', '--chunk-size', '50000'])
        self.assertEqual(result.exit_code, 0)
        self.assertIn("neo4j-admin command", result.output)
        mock_prepare_bulk.assert_called_with(chunk_size=50000, import_dir='bulk_import')

    @patch('omop2neo4j_lpg.loading.create_constraints_and_indexes')
    @patch('omop2neo4j_lpg.loading.get_driver')
    def test_create_indexes_command(self, mock_get_driver, mock_create_indexes):
        result = self.runner.invoke(cli, ['create-indexes'])
        self.assertEqual(result.exit_code, 0)
        mock_get_driver.assert_called_once()
        mock_create_indexes.assert_called_once()

    @patch('omop2neo4j_lpg.validation.verify_sample_concept')
    @patch('omop2neo4j_lpg.validation.get_relationship_counts')
    @patch('omop2neo4j_lpg.validation.get_node_counts')
    @patch('omop2neo4j_lpg.loading.get_driver')
    def test_validate_command(self, mock_get_driver, mock_get_nodes, mock_get_rels, mock_verify_sample):
        # Mock the return values of the validation functions
        mock_get_nodes.return_value = {"Concept:Standard": 100}
        mock_get_rels.return_value = {"IS_A": 200}
        mock_verify_sample.return_value = {
            "name": "Test Concept",
            "labels": ["Concept"],
            "synonym_count": 2,
            "relationships_summary": {},
            "ancestors_summary": {"count": 0, "sample_ancestors": []}
        }

        result = self.runner.invoke(cli, ['validate', '--concept-id', '123'])
        self.assertEqual(result.exit_code, 0)
        mock_get_driver.assert_called_once()
        mock_get_nodes.assert_called_once()
        mock_get_rels.assert_called_once()
        mock_verify_sample.assert_called_once_with(mock_get_driver.return_value, concept_id=123)
        self.assertIn("Concept:Standard: 100", result.output)
        self.assertIn("IS_A: 200", result.output)
        self.assertIn("Test Concept", result.output)

if __name__ == '__main__':
    unittest.main()
