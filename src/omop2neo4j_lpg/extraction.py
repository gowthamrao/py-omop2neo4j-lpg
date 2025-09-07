import psycopg2
from .config import settings, get_logger

logger = get_logger(__name__)

# SQL queries for extraction. Note the absence of a `TO` clause in the subquery.
# We will use `COPY (query) TO STDOUT` and pipe the output to a file in Python.
# This is more portable than writing directly to a file from the DB server.
SQL_QUERIES = {
    "concepts_optimized.csv": """
        SELECT
            c.concept_id, c.concept_name, c.domain_id, c.vocabulary_id,
            c.concept_class_id, c.standard_concept, c.concept_code,
            to_char(c.valid_start_date, 'YYYY-MM-DD') as valid_start_date,
            to_char(c.valid_end_date, 'YYYY-MM-DD') as valid_end_date,
            c.invalid_reason,
            string_agg(cs.concept_synonym_name, '|') AS synonyms
        FROM
            {schema}.concept c
        LEFT JOIN
            {schema}.concept_synonym cs ON c.concept_id = cs.concept_id
        GROUP BY
            c.concept_id
    """,
    "domain.csv": "SELECT * FROM {schema}.domain",
    "vocabulary.csv": "SELECT * FROM {schema}.vocabulary",
    "concept_relationship.csv": """
        SELECT concept_id_1, concept_id_2, relationship_id,
            to_char(valid_start_date, 'YYYY-MM-DD') as valid_start_date,
            to_char(valid_end_date, 'YYYY-MM-DD') as valid_end_date,
            invalid_reason
        FROM {schema}.concept_relationship
    """,
    "concept_ancestor.csv": """
        SELECT descendant_concept_id, ancestor_concept_id, min_levels_of_separation, max_levels_of_separation
        FROM {schema}.concept_ancestor
    """
}

def export_tables_to_csv():
    """
    Connects to PostgreSQL and exports OMOP vocabulary tables to CSV files
    using the efficient `COPY ... TO STDOUT` command.
    """
    logger.info("Starting data extraction from PostgreSQL.")
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=settings.POSTGRES_DB,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
            host=settings.POSTGRES_HOST,
            port=settings.POSTGRES_PORT,
        )
        logger.info("Successfully connected to PostgreSQL.")

        for filename, query_template in SQL_QUERIES.items():
            filepath = settings.EXPORT_DIR / filename
            logger.info(f"Exporting data to '{filepath}'...")

            # Format the query with the correct schema
            query = query_template.format(schema=settings.OMOP_SCHEMA)

            # Construct the full COPY command
            sql_command = f"COPY ({query}) TO STDOUT WITH CSV HEADER FORCE QUOTE *"

            with conn.cursor() as cursor, open(filepath, "w", encoding="utf-8") as f:
                cursor.copy_expert(sql_command, f)

            logger.info(f"Successfully exported to '{filepath}'.")

    except psycopg2.Error as e:
        logger.error(f"Database error during extraction: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("PostgreSQL connection closed.")

    logger.info("All tables have been extracted successfully.")
