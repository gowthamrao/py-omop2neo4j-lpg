import re

def standardize_label(s: str) -> str:
    """
    Sanitizes a string to be a valid Neo4j Label (UpperCamelCase).
    - Splits the string by non-alphanumeric characters.
    - Capitalizes the first letter of each part.
    - Joins the parts.
    Example: "SpecAnatomicSite" -> "SpecAnatomicSite"
             "Drug/ingredient" -> "DrugIngredient"
    """
    if not s:
        return ""

    words = re.split(r'[^A-Za-z0-9]+', str(s))

    capitalized_words = [word[0].upper() + word[1:] if word else "" for word in words]

    return "".join(capitalized_words)

def standardize_reltype(s: str) -> str:
    """
    Sanitizes a string to be a valid Neo4j Relationship Type (UPPER_SNAKE_CASE).
    - Replaces non-alphanumeric characters with underscores.
    - Converts to uppercase.
    - Collapses multiple underscores into one.
    Example: "maps to" -> "MAPS_TO"
             "ATC - ATC" -> "ATC_ATC"
    """
    if not s:
        return ""
    # Replace non-alphanumeric chars with underscore, then uppercase
    s = re.sub(r'[^A-Za-z0-9]+', '_', str(s)).upper()
    # Collapse multiple underscores
    s = re.sub(r'_+', '_', s)
    # Remove leading/trailing underscores
    s = s.strip('_')
    return s
