import os
from neo4j import GraphDatabase, Driver
from .config import settings, logger

# --- Cypher Query Templates ---

CLEAR_DB_QUERY = "MATCH (n) DETACH DELETE n"

CONSTRAINTS_AND_INDEXES = [
    "CREATE CONSTRAINT constraint_concept_id IF NOT EXISTS FOR (c:Concept) REQUIRE c.concept_id IS UNIQUE;",
    "CREATE CONSTRAINT constraint_domain_id IF NOT EXISTS FOR (d:Domain) REQUIRE d.domain_id IS UNIQUE;",
    "CREATE CONSTRAINT constraint_vocabulary_id IF NOT EXISTS FOR (v:Vocabulary) REQUIRE v.vocabulary_id IS UNIQUE;",
    "CREATE INDEX index_concept_code IF NOT EXISTS FOR (c:Concept) ON (c.concept_code);",
    "CREATE INDEX index_standard_label IF NOT EXISTS FOR (c:Standard) ON (c.concept_id);",
]

LOAD_DOMAINS_QUERY = """
LOAD CSV WITH HEADERS FROM $uri AS row
CREATE (d:Domain {
    domain_id: row.domain_id,
    domain_name: row.domain_name,
    domain_concept_id: toInteger(row.domain_concept_id)
});
"""

LOAD_VOCABULARIES_QUERY = """
LOAD CSV WITH HEADERS FROM $uri AS row
CREATE (v:Vocabulary {
    vocabulary_id: row.vocabulary_id,
    vocabulary_name: row.vocabulary_name,
    vocabulary_reference: row.vocabulary_reference,
    vocabulary_version: row.vocabulary_version,
    vocabulary_concept_id: toInteger(row.vocabulary_concept_id)
});
"""

LOAD_CONCEPTS_QUERY = """
CALL {
    LOAD CSV WITH HEADERS FROM $uri AS row
    // 1. Create the Concept Node
    CREATE (c:Concept {
        concept_id: toInteger(row.concept_id),
        name: row.concept_name,
        domain_id: row.domain_id,
        vocabulary_id: row.vocabulary_id,
        concept_class_id: row.concept_class_id,
        standard_concept: row.standard_concept,
        concept_code: row.concept_code,
        valid_start_date: date(row.valid_start_date),
        valid_end_date: date(row.valid_end_date),
        invalid_reason: row.invalid_reason,
        synonyms: CASE WHEN row.synonyms IS NOT NULL AND row.synonyms <> '' THEN split(row.synonyms, '|') ELSE [] END
    })

    // 2. Add dynamic/conditional labels
    WITH c, row
    CALL apoc.text.capitalizeAll(apoc.text.replace(row.domain_id, '[^A-Za-z0-9_]', '_')) YIELD value as standardizedLabel
    CALL apoc.create.addLabels(c, [standardizedLabel]) YIELD node

    WITH c, row
    // Add :Standard label conditionally. The inner query must return a value.
    CALL apoc.do.when(
        row.standard_concept = 'S',
        'SET c:Standard RETURN 1',
        '',
        {c:c}
    ) YIELD value

    // 3. Create Contextual Edges
    WITH c, row
    MATCH (d:Domain {domain_id: row.domain_id})
    CREATE (c)-[:IN_DOMAIN]->(d)
    WITH c, row
    MATCH (v:Vocabulary {vocabulary_id: row.vocabulary_id})
    CREATE (c)-[:FROM_VOCABULARY]->(v)
} IN TRANSACTIONS OF $batch_size ROWS
"""

LOAD_RELATIONSHIPS_QUERY = """
CALL {
    LOAD CSV WITH HEADERS FROM $uri AS row
    MATCH (c1:Concept {concept_id: toInteger(row.concept_id_1)})
    MATCH (c2:Concept {concept_id: toInteger(row.concept_id_2)})
    WITH c1, c2, row, toupper(apoc.text.replace(row.relationship_id, '[^A-Za-z0-9_]', '_')) AS relType
    CALL apoc.create.relationship(c1, relType, {
        valid_start_date: date(row.valid_start_date),
        valid_end_date: date(row.valid_end_date),
        invalid_reason: row.invalid_reason
    }, c2) YIELD rel
    RETURN count(rel)
} IN TRANSACTIONS OF $batch_size ROWS
"""

LOAD_ANCESTORS_QUERY = """
CALL {
    LOAD CSV WITH HEADERS FROM $uri AS row
    MATCH (d:Concept {concept_id: toInteger(row.descendant_concept_id)})
    MATCH (a:Concept {concept_id: toInteger(row.ancestor_concept_id)})
    CREATE (d)-[r:HAS_ANCESTOR]->(a)
    SET r.min_levels = toInteger(row.min_levels_of_separation),
        r.max_levels = toInteger(row.max_levels_of_separation)
} IN TRANSACTIONS OF $batch_size ROWS
"""

# --- Python Functions ---

def get_neo4j_driver() -> Driver:
    """Establishes and returns a connection to the Neo4j database."""
    try:
        driver = GraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )
        driver.verify_connectivity()
        logger.info("Successfully connected to Neo4j.")
        return driver
    except Exception as e:
        logger.error(f"Could not connect to Neo4j: {e}")
        raise


def run_cypher_query(driver: Driver, query: str, database: str = settings.neo4j_database, **params):
    """Helper function to run a Cypher query."""
    try:
        driver.execute_query(query, params, database=database)
        logger.info(f"Successfully executed query: {query.strip().splitlines()[0][:80]}...")
    except Exception as e:
        logger.error(f"Error executing query: {query.strip().splitlines()[0][:80]}... : {e}")
        raise


def clear_database(driver: Driver):
    """Clears the entire Neo4j database by dropping constraints, indexes, and deleting all nodes."""
    logger.info("Clearing the database...")

    # Drop all constraints
    constraints_result = driver.execute_query("SHOW CONSTRAINTS", database=settings.neo4j_database)
    for record in constraints_result.records:
        constraint_name = record["name"]
        logger.info(f"Dropping constraint: {constraint_name}")
        run_cypher_query(driver, f"DROP CONSTRAINT {constraint_name}")

    # Drop all indexes (that are not backing constraints)
    indexes_result = driver.execute_query("SHOW INDEXES", database=settings.neo4j_database)
    for record in indexes_result.records:
        # Only drop indexes that are not backing constraints
        if record["type"] != 'CONSTRAINT_BACKED':
            index_name = record["name"]
            logger.info(f"Dropping index: {index_name}")
            run_cypher_query(driver, f"DROP INDEX {index_name}")

    run_cypher_query(driver, CLEAR_DB_QUERY)
    logger.info("Database cleared.")


def create_constraints_and_indexes(driver: Driver):
    """Creates all necessary constraints and indexes for the OMOP graph."""
    logger.info("Creating constraints and indexes...")
    for query in CONSTRAINTS_AND_INDEXES:
        run_cypher_query(driver, query)
    logger.info("Constraints and indexes created successfully.")


def get_file_uri(filename: str) -> str:
    """Constructs the file URI for Neo4j's LOAD CSV, which reads from its 'import' directory."""
    return f"file:///{filename}"


def load_metadata(driver: Driver):
    """Loads Domain and Vocabulary CSV files into Neo4j."""
    logger.info("Loading metadata: Domain and Vocabulary...")
    run_cypher_query(driver, LOAD_DOMAINS_QUERY, uri=get_file_uri("domain.csv"))
    run_cypher_query(driver, LOAD_VOCABULARIES_QUERY, uri=get_file_uri("vocabulary.csv"))
    logger.info("Metadata loading complete.")


def load_concepts(driver: Driver, batch_size: int):
    """Loads the concepts_optimized.csv file into Neo4j."""
    logger.info(f"Loading concepts with batch size {batch_size}...")
    uri = get_file_uri("concepts_optimized.csv")
    run_cypher_query(driver, LOAD_CONCEPTS_QUERY, uri=uri, batch_size=batch_size)
    logger.info("Concept loading complete.")


def load_relationships(driver: Driver, batch_size: int):
    """Loads the concept_relationship.csv file into Neo4j."""
    logger.info(f"Loading relationships with batch size {batch_size}...")
    uri = get_file_uri("concept_relationship.csv")
    run_cypher_query(driver, LOAD_RELATIONSHIPS_QUERY, uri=uri, batch_size=batch_size)
    logger.info("Relationship loading complete.")


def load_ancestors(driver: Driver, batch_size: int):
    """Loads the concept_ancestor.csv file into Neo4j."""
    logger.info(f"Loading ancestors with batch size {batch_size}...")
    uri = get_file_uri("concept_ancestor.csv")
    run_cypher_query(driver, LOAD_ANCESTORS_QUERY, uri=uri, batch_size=batch_size)
    logger.info("Ancestor loading complete.")
