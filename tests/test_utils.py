import unittest
import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from omop2neo4j_lpg.utils import standardize_label, standardize_reltype

class TestUtils(unittest.TestCase):

    def test_standardize_label(self):
        self.assertEqual(standardize_label("Hello World"), "HelloWorld")
        self.assertEqual(standardize_label("Drug/Ingredient"), "DrugIngredient")
        self.assertEqual(standardize_label("SpecAnatomicSite"), "SpecAnatomicSite")
        self.assertEqual(standardize_label("Observation 2"), "Observation2")
        self.assertEqual(standardize_label("  leading spaces"), "LeadingSpaces")
        self.assertEqual(standardize_label("trailing spaces  "), "TrailingSpaces")
        self.assertEqual(standardize_label(""), "")
        self.assertEqual(standardize_label(None), "")
        self.assertEqual(standardize_label("special-chars!@#$"), "SpecialChars")

    def test_standardize_reltype(self):
        self.assertEqual(standardize_reltype("maps to"), "MAPS_TO")
        self.assertEqual(standardize_reltype("ATC - ATC"), "ATC_ATC")
        self.assertEqual(standardize_reltype("Has ancestor"), "HAS_ANCESTOR")
        self.assertEqual(standardize_reltype("RxNorm has ingredient"), "RXNORM_HAS_INGREDIENT")
        self.assertEqual(standardize_reltype("trailing_sep_"), "TRAILING_SEP")
        self.assertEqual(standardize_reltype("_leading_sep"), "LEADING_SEP")
        self.assertEqual(standardize_reltype("double__underscore"), "DOUBLE_UNDERSCORE")
        self.assertEqual(standardize_reltype(""), "")
        self.assertEqual(standardize_reltype(None), "")
        self.assertEqual(standardize_reltype("special-chars!@#$"), "SPECIAL_CHARS")

if __name__ == '__main__':
    unittest.main()
