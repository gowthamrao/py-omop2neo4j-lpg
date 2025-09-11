import os
import psycopg2
from .config import settings, logger


def get_sql_queries(schema: str) -> dict[str, str]:
    """
    Returns a dictionary of table names and their corresponding COPY SQL queries.
    The queries are formatted with the provided schema.
    """
    # Note: These queries now use STDOUT for portability.
    return {
        "concepts_optimized.csv": f"""
            COPY (
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
                    c.concept_id, c.concept_name, c.domain_id, c.vocabulary_id,
                    c.concept_class_id, c.standard_concept, c.concept_code,
                    c.valid_start_date, c.valid_end_date, c.invalid_reason
            ) TO STDOUT WITH CSV HEADER FORCE QUOTE *;
        """,
        "domain.csv": f"COPY (SELECT * FROM {schema}.domain) TO STDOUT WITH CSV HEADER FORCE QUOTE *;",
        "vocabulary.csv": f"COPY (SELECT * FROM {schema}.vocabulary) TO STDOUT WITH CSV HEADER FORCE QUOTE *;",
        "concept_relationship.csv": f"""
            COPY (
                SELECT concept_id_1, concept_id_2, relationship_id,
                    to_char(valid_start_date, 'YYYY-MM-DD') as valid_start_date,
                    to_char(valid_end_date, 'YYYY-MM-DD') as valid_end_date,
                    invalid_reason
                FROM {schema}.concept_relationship
            ) TO STDOUT WITH CSV HEADER FORCE QUOTE *;
        """,
        "concept_ancestor.csv": f"""
            COPY (
                SELECT descendant_concept_id, ancestor_concept_id, min_levels_of_separation, max_levels_of_separation
                FROM {schema}.concept_ancestor
            ) TO STDOUT WITH CSV HEADER FORCE QUOTE *;
        """,
    }


def export_tables_to_csv():
    """
    Connects to PostgreSQL and exports tables to CSV files using COPY TO STDOUT.
    This method streams data from the server to the client, avoiding server-side
    file permission issues.
    """
    logger.info("Starting data extraction from PostgreSQL using STDOUT streaming.")

    export_dir = settings.EXPORT_DIR
    schema = settings.OMOP_SCHEMA

    # Ensure the export directory exists
    os.makedirs(export_dir, exist_ok=True)
    logger.info(f"Export directory: {os.path.abspath(export_dir)}")

    queries = get_sql_queries(schema)

    conn = None
    try:
        logger.info(f"Connecting to PostgreSQL database '{settings.POSTGRES_DB}'...")
        conn = psycopg2.connect(
            dbname=settings.POSTGRES_DB,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
            host=settings.POSTGRES_HOST,
            port=settings.POSTGRES_PORT,
        )
        logger.info("PostgreSQL connection successful.")

        with conn.cursor() as cursor:
            for filename, query in queries.items():
                output_path = os.path.join(export_dir, filename)
                logger.info(f"Exporting query to '{output_path}'...")
                try:
                    with open(output_path, "w", encoding="utf-8") as f_out:
                        cursor.copy_expert(query, f_out)
                    logger.info(f"Successfully exported to '{filename}'.")
                except Exception as e:
                    logger.error(f"Error exporting to '{filename}': {e}")
                    raise

    except psycopg2.Error as e:
        logger.error(f"Database connection failed: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("PostgreSQL connection closed.")
