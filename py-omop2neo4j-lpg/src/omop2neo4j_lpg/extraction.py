import os
import psycopg2
from .config import settings, logger

# SQL templates for extraction using COPY command.
# We use `COPY ... TO STDOUT` and pipe the output to a local file using psycopg2's
# `copy_expert`. This avoids file permission issues on the PostgreSQL server, which
# is a common problem with `COPY ... TO 'filename'`.
SQL_QUERIES = {
    "concepts_optimized.csv": """
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
                c.concept_id
        ) TO STDOUT WITH CSV HEADER FORCE QUOTE *
    """,
    "domain.csv": """
        COPY {schema}.domain TO STDOUT WITH CSV HEADER FORCE QUOTE *
    """,
    "vocabulary.csv": """
        COPY {schema}.vocabulary TO STDOUT WITH CSV HEADER FORCE QUOTE *
    """,
    "concept_relationship.csv": """
        COPY (
            SELECT concept_id_1, concept_id_2, relationship_id,
                to_char(valid_start_date, 'YYYY-MM-DD') as valid_start_date,
                to_char(valid_end_date, 'YYYY-MM-DD') as valid_end_date,
                invalid_reason
            FROM {schema}.concept_relationship
        ) TO STDOUT WITH CSV HEADER FORCE QUOTE *
    """,
    "concept_ancestor.csv": """
        COPY (
            SELECT descendant_concept_id, ancestor_concept_id, min_levels_of_separation, max_levels_of_separation
            FROM {schema}.concept_ancestor
        ) TO STDOUT WITH CSV HEADER FORCE QUOTE *
    """
}


def get_pg_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=settings.pg_database,
            user=settings.pg_user,
            password=settings.pg_password,
            host=settings.pg_host,
            port=settings.pg_port,
        )
        logger.info("Successfully connected to PostgreSQL.")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Could not connect to PostgreSQL: {e}")
        raise


def extract_data(schema: str = settings.pg_schema, export_dir: str = settings.export_dir):
    """
    Extracts OMOP vocabulary tables from PostgreSQL to local CSV files.

    Args:
        schema (str): The database schema where OMOP tables reside.
        export_dir (str): The local directory to save the CSV files into.
    """
    logger.info(f"Starting data extraction from schema '{schema}' to directory '{export_dir}'.")
    conn = get_pg_connection()
    if not conn:
        return

    try:
        with conn.cursor() as cursor:
            for filename, sql_template in SQL_QUERIES.items():
                output_path = os.path.join(export_dir, filename)
                logger.info(f"Exporting data to '{output_path}'...")

                # Format the SQL with the correct schema
                sql = sql_template.format(schema=schema)

                try:
                    with open(output_path, "w", encoding="utf-8") as f:
                        # Use copy_expert to efficiently stream data to the local file
                        cursor.copy_expert(sql, f)
                    logger.info(f"Successfully exported '{filename}'.")
                except (psycopg2.Error, IOError) as e:
                    logger.error(f"Error exporting '{filename}': {e}")
                    # Clean up the partially written file on error
                    if os.path.exists(output_path):
                        os.remove(output_path)
                    raise
        logger.info("All tables have been extracted successfully.")
    finally:
        if conn:
            conn.close()
            logger.info("PostgreSQL connection closed.")
