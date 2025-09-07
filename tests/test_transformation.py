import unittest
import os
import shutil
import pandas as pd
from omop2neo4j_lpg.transformation import prepare_for_bulk_import
from omop2neo4j_lpg.config import settings

class TestTransformation(unittest.TestCase):

    def setUp(self):
        """Set up a temporary directory with dummy CSV files for testing."""
        self.test_export_dir = os.path.join(settings.EXPORT_DIR, "test_temp")
        self.test_import_dir = os.path.join("bulk_import", "test_temp")
        os.makedirs(self.test_export_dir, exist_ok=True)
        os.makedirs(self.test_import_dir, exist_ok=True)

        # --- Create Dummy Input CSVs ---
        # domains
        pd.DataFrame({
            'domain_id': ['Drug', 'Condition'],
            'domain_name': ['Drug', 'Condition'],
            'domain_concept_id': ['1', '2']
        }).to_csv(os.path.join(self.test_export_dir, 'domain.csv'), index=False)

        # vocabularies
        pd.DataFrame({
            'vocabulary_id': ['RxNorm', 'SNOMED'],
            'vocabulary_name': ['RxNorm', 'SNOMED'],
            'vocabulary_reference': ['ref1', 'ref2'],
            'vocabulary_version': ['v1', 'v2'],
            'vocabulary_concept_id': ['101', '102']
        }).to_csv(os.path.join(self.test_export_dir, 'vocabulary.csv'), index=False)

        # concepts_optimized
        pd.DataFrame({
            'concept_id': [1001, 1002, 1003],
            'concept_name': ['Aspirin', 'Headache', 'Pain Killer'],
            'domain_id': ['Drug', 'Condition', 'Drug/Device'],
            'vocabulary_id': ['RxNorm', 'SNOMED', 'RxNorm'],
            'concept_class_id': ['Ingredient', 'Finding', 'Ingredient'],
            'standard_concept': ['S', 'S', ''],
            'concept_code': ['A1', 'B2', 'C3'],
            'valid_start_date': ['2000-01-01', '2000-01-01', '2000-01-01'],
            'valid_end_date': ['2099-12-31', '2099-12-31', '2099-12-31'],
            'invalid_reason': ['', '', ''],
            'synonyms': ['acetylsalicylic acid', '', 'pain reliever|analgesic']
        }).to_csv(os.path.join(self.test_export_dir, 'concepts_optimized.csv'), index=False)

        # concept_relationship
        pd.DataFrame({
            'concept_id_1': [1001, 1003],
            'concept_id_2': [1002, 1001],
            'relationship_id': ['treats', 'maps to'],
            'valid_start_date': ['2000-01-01', '2000-01-01'],
            'valid_end_date': ['2099-12-31', '2099-12-31'],
            'invalid_reason': ['', '']
        }).to_csv(os.path.join(self.test_export_dir, 'concept_relationship.csv'), index=False)

        # concept_ancestor
        pd.DataFrame({
            'descendant_concept_id': [1001],
            'ancestor_concept_id': [1003],
            'min_levels_of_separation': [1],
            'max_levels_of_separation': [1]
        }).to_csv(os.path.join(self.test_export_dir, 'concept_ancestor.csv'), index=False)

        # Override settings to use test directory
        self.original_export_dir = settings.EXPORT_DIR
        settings.EXPORT_DIR = self.test_export_dir

    def tearDown(self):
        """Clean up temporary directories."""
        shutil.rmtree(self.test_export_dir)
        shutil.rmtree(self.test_import_dir)
        settings.EXPORT_DIR = self.original_export_dir

    def test_prepare_for_bulk_import(self):
        """Test the main transformation function."""
        # Execute the function
        command = prepare_for_bulk_import(chunk_size=2, import_dir=self.test_import_dir)

        # --- Assert Command ---
        self.assertIn("neo4j-admin database import full", command)
        self.assertIn(f"--nodes='{self.test_import_dir}/nodes_concept_header.csv'", command)
        self.assertIn(f"--relationships='{self.test_import_dir}/rels_semantic_header.csv'", command)

        # --- Assert File Creation ---
        self.assertTrue(os.path.exists(os.path.join(self.test_import_dir, 'nodes_concept.csv')))
        self.assertTrue(os.path.exists(os.path.join(self.test_import_dir, 'rels_semantic.csv')))
        self.assertTrue(os.path.exists(os.path.join(self.test_import_dir, 'rels_ancestor.csv')))

        # --- Assert Content of Key Files ---
        # Concept Nodes
        df_nodes = pd.read_csv(os.path.join(self.test_import_dir, 'nodes_concept.csv'))
        self.assertEqual(len(df_nodes), 3)
        self.assertIn(':ID(Concept)', df_nodes.columns)
        self.assertIn(':LABEL', df_nodes.columns)
        # Check standard concept label
        aspirin_row = df_nodes[df_nodes[':ID(Concept)'] == 1001]
        self.assertEqual(aspirin_row[':LABEL'].iloc[0], 'Concept;Drug;Standard')
        # Check sanitized label
        painkiller_row = df_nodes[df_nodes[':ID(Concept)'] == 1003]
        self.assertEqual(painkiller_row[':LABEL'].iloc[0], 'Concept;DrugDevice')
        # Check synonyms format
        self.assertEqual(painkiller_row['synonyms:string[]'].iloc[0], 'pain reliever|analgesic')

        # Semantic Relationships
        df_rels = pd.read_csv(os.path.join(self.test_import_dir, 'rels_semantic.csv'))
        self.assertEqual(len(df_rels), 2)
        self.assertIn(':START_ID(Concept)', df_rels.columns)
        self.assertIn(':END_ID(Concept)', df_rels.columns)
        self.assertIn(':TYPE', df_rels.columns)
        # Check standardized reltype
        maps_to_row = df_rels[df_rels[':START_ID(Concept)'] == 1003]
        self.assertEqual(maps_to_row[':TYPE'].iloc[0], 'MAPS_TO')

if __name__ == '__main__':
    unittest.main()
