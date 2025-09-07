import unittest
from unittest.mock import MagicMock, patch
from src.omop2neo4j_lpg import validation

class TestValidation(unittest.TestCase):

    @patch('src.omop2neo4j_lpg.validation.get_driver')
    def test_get_node_counts(self, mock_get_driver):
        # Arrange
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_result = MagicMock()

        # Simulate the result of the Cypher query
        mock_records = [
            {"label": "Concept", "count": 1500},
            {"label": "Domain", "count": 10},
            {"label": "Standard", "count": 500},
        ]
        mock_result.__iter__.return_value = iter(mock_records)
        mock_session.run.return_value = mock_result
        mock_driver.session.return_value.__enter__.return_value = mock_session
        mock_get_driver.return_value = mock_driver

        # Act
        counts = validation.get_node_counts(mock_driver)

        # Assert
        self.assertEqual(len(counts), 3)
        self.assertEqual(counts["Concept"], 1500)
        self.assertEqual(counts["Domain"], 10)
        self.assertTrue(mock_session.run.called)

    @patch('src.omop2neo4j_lpg.validation.get_driver')
    def test_get_relationship_counts(self, mock_get_driver):
        # Arrange
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_result = MagicMock()

        mock_records = [
            {"relationshipType": "IS_A", "count": 2000},
            {"relationshipType": "HAS_ANCESTOR", "count": 50000},
        ]
        mock_result.__iter__.return_value = iter(mock_records)
        mock_session.run.return_value = mock_result
        mock_driver.session.return_value.__enter__.return_value = mock_session
        mock_get_driver.return_value = mock_driver

        # Act
        counts = validation.get_relationship_counts(mock_driver)

        # Assert
        self.assertEqual(len(counts), 2)
        self.assertEqual(counts["HAS_ANCESTOR"], 50000)
        self.assertTrue(mock_session.run.called)

    @patch('src.omop2neo4j_lpg.validation.get_driver')
    def test_verify_sample_concept_found(self, mock_get_driver):
        # Arrange
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_result = MagicMock()

        # Simulate a single record returned by session.run().single()
        mock_record_data = {
            "concept_id": 1177480,
            "name": "Enalapril",
            "labels": ["Concept", "Drug", "Standard"],
            "synonym_count": 5,
            "relationships": [
                {"rel_type": "IS_A", "neighbors": [{"name": "ACE Inhibitor", "id": 123}]},
                {"rel_type": "FROM_VOCABULARY", "neighbors": [{"name": "RxNorm", "id": "RxNorm"}]}
            ]
        }
        mock_result.single.return_value = mock_record_data
        mock_session.run.return_value = mock_result
        mock_driver.session.return_value.__enter__.return_value = mock_session
        mock_get_driver.return_value = mock_driver

        # Act
        data = validation.verify_sample_concept(mock_driver, concept_id=1177480)

        # Assert
        self.assertIsNotNone(data)
        self.assertEqual(data['name'], 'Enalapril')
        self.assertIn('Drug', data['labels'])
        self.assertEqual(data['relationships']['IS_A']['count'], 1)
        self.assertEqual(data['relationships']['IS_A']['sample_neighbors'][0], 'ACE Inhibitor')

    @patch('src.omop2neo4j_lpg.validation.get_driver')
    def test_verify_sample_concept_not_found(self, mock_get_driver):
        # Arrange
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_result = MagicMock()

        # Simulate the case where no record is found
        mock_result.single.return_value = None
        mock_session.run.return_value = mock_result
        mock_driver.session.return_value.__enter__.return_value = mock_session
        mock_get_driver.return_value = mock_driver

        # Act
        data = validation.verify_sample_concept(mock_driver, concept_id=999)

        # Assert
        self.assertIsNone(data)

if __name__ == '__main__':
    unittest.main()
