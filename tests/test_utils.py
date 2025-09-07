import pytest
from omop2neo4j_lpg.utils import standardize_label, standardize_reltype

# Test cases for standardize_label
@pytest.mark.parametrize("input_str, expected_output", [
    ("Hello World", "HelloWorld"),
    ("Drug/Ingredient", "DrugIngredient"),
    ("SpecAnatomicSite", "SpecAnatomicSite"),
    ("Observation 2", "Observation2"),
    ("  leading spaces", "LeadingSpaces"),
    ("trailing spaces  ", "TrailingSpaces"),
    ("", ""),
    ("special-chars!@#$", "SpecialChars"),
    ("a b c", "ABC"), # Test for single letter words
    ("mixedCASE", "MixedCASE"), # Should only capitalize first letter
])
def test_standardize_label(input_str, expected_output):
    assert standardize_label(input_str) == expected_output

def test_standardize_label_none():
    assert standardize_label(None) == ""

# Test cases for standardize_reltype
@pytest.mark.parametrize("input_str, expected_output", [
    ("maps to", "MAPS_TO"),
    ("ATC - ATC", "ATC_ATC"),
    ("Has ancestor", "HAS_ANCESTOR"),
    ("RxNorm has ingredient", "RXNORM_HAS_INGREDIENT"),
    ("trailing_sep_", "TRAILING_SEP"),
    ("_leading_sep", "LEADING_SEP"),
    ("double__underscore", "DOUBLE_UNDERSCORE"),
    ("", ""),
    ("special-chars!@#$", "SPECIAL_CHARS"),
    ("a b c", "A_B_C"),
])
def test_standardize_reltype(input_str, expected_output):
    assert standardize_reltype(input_str) == expected_output

def test_standardize_reltype_none():
    assert standardize_reltype(None) == ""
