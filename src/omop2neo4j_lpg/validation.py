from __future__ import annotations
from neo4j import Driver
from .config import get_logger
from .loading import get_driver
import json

logger = get_logger(__name__)

def get_node_counts(driver: Driver) -> dict[str, int]:
    """
    Counts nodes for each distinct label in the database.
    """
    logger.info("Performing node count validation by label...")
    # This query gets counts for primary labels. For all label combinations,
    # a more complex query would be needed, but this is a good summary.
    query = """
    CALL db.labels() YIELD label
    CALL apoc.cypher.run('MATCH (:`' + label + '`) RETURN count(*) as count', {}) YIELD value
    RETURN label, value.count AS count
    ORDER BY label
    """
    with driver.session() as session:
        result = session.run(query)
        counts = {record["label"]: record["count"] for record in result}
        logger.info(f"Node counts: {counts}")
        return counts

def get_relationship_counts(driver: Driver) -> dict[str, int]:
    """
    Counts relationships for each distinct type in the database.
    """
    logger.info("Performing relationship count validation by type...")
    query = """
    CALL db.relationshipTypes() YIELD relationshipType
    CALL apoc.cypher.run('MATCH ()-[:`' + relationshipType + '`]->() RETURN count(*) as count', {}) YIELD value
    RETURN relationshipType, value.count AS count
    ORDER BY relationshipType
    """
    with driver.session() as session:
        result = session.run(query)
        counts = {record["relationshipType"]: record["count"] for record in result}
        logger.info(f"Relationship counts: {counts}")
        return counts

def verify_sample_concept(driver: Driver, concept_id: int = 1177480) -> dict | None:
    """
    Fetches a sample concept and its direct neighborhood to verify its structure.
    The default concept_id is 1177480 ('Enalapril').
    """
    logger.info(f"Performing structural validation for Concept ID: {concept_id}...")
    query = """
    MATCH (c:Concept {concept_id: $concept_id})
    // Collect outgoing relationships and connected neighbors
    CALL {
        WITH c
        MATCH (c)-[r]->(neighbor)
        RETURN type(r) AS rel_type,
               collect({name: neighbor.name, id: COALESCE(neighbor.concept_id, neighbor.domain_id, neighbor.vocabulary_id)}) AS neighbors
    }
    RETURN
        c.concept_id AS concept_id,
        c.name AS name,
        labels(c) AS labels,
        size(c.synonyms) AS synonym_count,
        collect({rel_type: rel_type, neighbors: neighbors}) AS relationships
    """
    with driver.session() as session:
        result = session.run(query, concept_id=concept_id).single()
        if not result or not result.get("concept_id"):
            logger.warning(f"Sample Concept ID {concept_id} not found in the database.")
            return None

        record_dict = dict(result)
        # Clean up the relationships aggregation
        rels = {}
        for item in record_dict.get("relationships", []):
            if item['rel_type']:
                rels[item['rel_type']] = {
                    "count": len(item['neighbors']),
                    "sample_neighbors": [n['name'] for n in item['neighbors'][:3]] # Show first 3
                }
        record_dict['relationships'] = rels

        logger.info(f"Structural validation for '{record_dict.get('name')}': \n{json.dumps(record_dict, indent=2)}")
        return record_dict

def run_validation():
    """
    Main orchestrator for the validation process.
    Connects to Neo4j and runs all validation checks.
    """
    logger.info("Starting validation process...")
    driver = None
    try:
        driver = get_driver()
        node_counts = get_node_counts(driver)
        rel_counts = get_relationship_counts(driver)
        sample_verification = verify_sample_concept(driver) # Using default ID

        return {
            "node_counts": node_counts,
            "relationship_counts": rel_counts,
            "sample_concept_verification": sample_verification
        }

    except Exception as e:
        logger.error(f"An error occurred during the validation process: {e}")
        # We return a dict so the CLI can handle the error gracefully
        return {"error": str(e)}
    finally:
        if driver:
            driver.close()
            logger.info("Validation process finished. Neo4j connection closed.")
