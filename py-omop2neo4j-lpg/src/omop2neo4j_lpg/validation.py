from neo4j import Driver
from .config import settings, logger
from .loading import get_neo4j_driver

def _run_read_query(driver: Driver, query: str, **params):
    """Helper to run a read-only Cypher query and return the records."""
    try:
        # routing_='r' is not a standard parameter for execute_query
        # The session will determine if it's a read or write transaction.
        result, _, _ = driver.execute_query(query, params, database=settings.neo4j_database)
        return result
    except Exception as e:
        logger.error(f"Error executing validation query: {query[:80]}... : {e}")
        raise

def _print_header(title: str):
    """Prints a formatted header to the console."""
    print("\n" + "="*60)
    print(f" {title.upper()} ".center(60, "="))
    print("="*60)

def check_node_counts(driver: Driver):
    """Checks and prints counts of nodes by label."""
    _print_header("Node Count Validation")

    total_nodes = _run_read_query(driver, "MATCH (n) RETURN count(n) AS count")[0]['count']
    print(f"Total nodes in database: {total_nodes:,}")

    print("\n--- Counts by Core Label ---")
    for label in ["Concept", "Domain", "Vocabulary", "Standard"]:
        count = _run_read_query(driver, f"MATCH (n:{label}) RETURN count(n) AS count")[0]['count']
        print(f"- Nodes with label :{label}: {count:,}")

    print("\n--- Sample Counts for Dynamic Domain Labels ---")
    labels_in_use = [r['label'] for r in _run_read_query(driver, "CALL db.labels() YIELD label")]
    sample_dynamic_labels = sorted([l for l in labels_in_use if l not in ["Concept", "Domain", "Vocabulary", "Standard"]])

    if not sample_dynamic_labels:
        print("No dynamic domain labels found.")
    else:
        for label in sample_dynamic_labels[:10]: # Print up to 10 dynamic labels
            count = _run_read_query(driver, f"MATCH (n:`{label}`) RETURN count(n) AS count")[0]['count']
            print(f"- Nodes with label :{label}: {count:,}")

def check_relationship_counts(driver: Driver):
    """Checks and prints counts of relationships by type."""
    _print_header("Relationship Count Validation")

    total_rels = _run_read_query(driver, "MATCH ()-[r]->() RETURN count(r) AS count")[0]['count']
    print(f"Total relationships in database: {total_rels:,}")

    print("\n--- Counts by Relationship Type ---")
    # This query is more efficient than calling apoc.cypher.run in a loop
    rel_types_counts = _run_read_query(driver, """
        CALL db.relationshipTypes() YIELD relationshipType
        MATCH ()-[r]->() WHERE type(r) = relationshipType
        RETURN relationshipType, count(r) as count
        ORDER BY count DESC
    """)
    for record in rel_types_counts:
        print(f"- Relationships of type :{record['relationshipType']}: {record['count']:,}")

def check_sample_concept(driver: Driver, concept_id: int = 1177480):
    """Inspects a sample concept to verify its properties, labels, and relationships."""
    _print_header(f"Sample Concept Validation (Concept ID: {concept_id})")

    concept_data = _run_read_query(driver, "MATCH (c:Concept {concept_id: $id}) RETURN c, labels(c) as labels", id=concept_id)
    if not concept_data:
        print(f"ERROR: Concept with ID {concept_id} not found.")
        return

    record = concept_data[0]
    properties = record['c']
    labels = record['labels']

    print(f"Found Concept: '{properties.get('name')}'")
    print(f"Labels: {labels}")
    print("\n--- Properties ---")
    for key, value in sorted(properties.items()):
        print(f"- {key}: {value}")

    print("\n--- Sample Relationships (LIMIT 25) ---")
    relationships = _run_read_query(driver, """
        MATCH (c:Concept {concept_id: $id})-[r]-(n)
        RETURN type(r) as rel_type, n.name as other_name, labels(n) as other_labels, n.concept_id as other_id
        LIMIT 25
    """, id=concept_id)

    if not relationships:
        print("No relationships found for this concept.")
    else:
        for rel in relationships:
            print(f"- [:{rel['rel_type']}]->(name: '{rel['other_name']}', id: {rel['other_id']}, labels: {rel['other_labels']})")

    ancestors = _run_read_query(driver, """
        MATCH (c:Concept {concept_id: $id})-[:HAS_ANCESTOR]->(a)
        RETURN a.concept_id as ancestor_id, a.name as ancestor_name
        LIMIT 5
    """, id=concept_id)
    if ancestors:
        print("\n--- Sample Ancestors (LIMIT 5) ---")
        for ancestor in ancestors:
            print(f"- {ancestor['ancestor_name']} (ID: {ancestor['ancestor_id']})")

def validate_graph():
    """Main entry point function to run all validation checks."""
    logger.info("Starting graph validation...")
    driver = None
    try:
        driver = get_neo4j_driver()
        check_node_counts(driver)
        check_relationship_counts(driver)
        check_sample_concept(driver) # Use default Aspirin concept
        logger.info("Graph validation complete.")
    except Exception as e:
        logger.error(f"An error occurred during validation: {e}", exc_info=True)
    finally:
        if driver:
            driver.close()
            logger.info("Neo4j connection for validation closed.")
