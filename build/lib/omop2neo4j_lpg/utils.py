import re


def standardize_label(s: str) -> str:
    """
    Sanitizes a string to be a valid Neo4j label (UpperCamelCase).
    - Removes non-alphanumeric characters.
    - Converts to UpperCamelCase.

    Example: "Drug Exposure" -> "DrugExposure"
    Example: "SpecAnatomicSite" -> "SpecAnatomicSite"
    """
    if not s:
        return ""
    # First, sanitize by replacing anything that's not a letter or number with a space
    sanitized = re.sub(r'[^a-zA-Z0-9]+', ' ', s)
    # Split by spaces, capitalize each word, and join them
    parts = [part.capitalize() for part in sanitized.split()]
    # If the original string had no spaces, it might be already camelCased
    if not parts:
        return s
    return "".join(parts)


def standardize_reltype(s: str) -> str:
    """
    Sanitizes a string to be a valid Neo4j relationship type (UPPER_SNAKE_CASE).
    - Replaces non-alphanumeric characters with underscores.
    - Converts to UPPER_SNAKE_CASE.
    - Collapses multiple underscores.

    Example: "maps to" -> "MAPS_TO"
    Example: "Has ancestor" -> "HAS_ANCESTOR"
    Example: "relationship_id" -> "RELATIONSHIP_ID"
    """
    if not s:
        return ""
    # Replace non-alphanumeric characters with a space
    s = re.sub(r'[^a-zA-Z0-9_]+', ' ', str(s))
    # Replace spaces with underscores
    s = s.strip().replace(' ', '_')
    # Collapse multiple underscores
    s = re.sub(r'_+', '_', s)
    return s.upper()
