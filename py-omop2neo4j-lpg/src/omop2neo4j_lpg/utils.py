import re
from typing import Optional

def standardize_label(s: Optional[str]) -> str:
    """
    Sanitizes and converts a string to UpperCamelCase for use as a Neo4j label.
    - Replaces non-alphanumeric characters (except '_') with '_'.
    - Converts the string to UpperCamelCase from snake_case.

    Examples:
    - "spec_anatomic_site" -> "SpecAnatomicSite"
    - "Condition/Era" -> "Condition_Era" -> "ConditionEra"
    - "Drug" -> "Drug"
    """
    if not isinstance(s, str) or not s:
        return ""
    # Replace any character that is not a letter, number, or underscore with an underscore
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', s)
    # Split by underscore, capitalize each part, and join
    # The `if word` handles cases of multiple underscores leading to empty strings
    return "".join(word.capitalize() for word in sanitized.split('_') if word)

def standardize_reltype(s: Optional[str]) -> str:
    """
    Sanitizes and converts a string to UPPER_SNAKE_CASE for use as a Neo4j relationship type.
    - Replaces non-alphanumeric characters (except '_') with an underscore.
    - Converts the resulting string to uppercase.

    Examples:
    - "Maps to" -> "MAPS_TO"
    - "Subsumes" -> "SUBSUMES"
    - "Concept replaced by" -> "CONCEPT_REPLACED_BY"
    """
    if not isinstance(s, str) or not s:
        return ""
    # Replace any character that is not a letter, number, or underscore with an underscore
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', s)
    return sanitized.upper()
