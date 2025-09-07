"""
Command-Line Interface for the OMOP to Neo4j ETL tool.

This module provides a CLI using 'click' to orchestrate the different
stages of the ETL process, including extraction, loading, and database
management tasks.
"""

import click
from . import extraction
from . import loading
from .config import get_logger

logger = get_logger(__name__)

@click.group()
def cli():
    """A CLI tool to migrate OMOP vocabulary from PostgreSQL to Neo4j."""
    pass

@cli.command()
def extract():
    """
    Extracts OMOP vocabulary data from PostgreSQL to CSV files.
    """
    logger.info("CLI: Starting extraction process...")
    try:
        extraction.export_tables_to_csv()
        logger.info("CLI: Extraction process completed successfully.")
    except Exception as e:
        logger.error(f"CLI: An error occurred during extraction: {e}")
        # click.echo(f"Error during extraction: {e}") # This would print to console
        # To keep logs clean, we just log it. The logger also prints to stdout.

@cli.command()
def clear_db():
    """
    Clears the Neo4j database by deleting all nodes and relationships.
    Also drops all constraints and indexes.
    """
    logger.info("CLI: Starting database clearing process...")
    try:
        driver = loading.get_driver()
        loading.clear_database(driver)
        driver.close()
        logger.info("CLI: Database clearing process completed successfully.")
    except Exception as e:
        logger.error(f"CLI: An error occurred while clearing the database: {e}")

@cli.command()
@click.option('--batch-size', default=None, type=int, help='Override LOAD_CSV_BATCH_SIZE from settings.')
def load_csv(batch_size):
    """
    Loads data from CSV files into Neo4j using the online LOAD CSV method.
    This is a full reload: it clears the DB, creates schema, and loads data.
    """
    logger.info("CLI: Starting LOAD CSV process...")
    # Note: The current `run_load_csv` doesn't support overriding batch_size.
    # This is a placeholder for future enhancement. If batch_size is passed,
    # we could update settings, but that's complex. For now, we ignore it.
    if batch_size:
        logger.warning(f"CLI: --batch-size option is not yet implemented. Using value from settings.")
    try:
        loading.run_load_csv()
        logger.info("CLI: LOAD CSV process completed successfully.")
    except Exception as e:
        logger.error(f"CLI: An error occurred during the LOAD CSV process: {e}")

@cli.command()
def create_indexes():
    """
    Creates all predefined constraints and indexes in the Neo4j database.
    Useful after a manual import or if schema setup failed.
    """
    logger.info("CLI: Starting index and constraint creation process...")
    try:
        driver = loading.get_driver()
        loading.create_constraints_and_indexes(driver)
        driver.close()
        logger.info("CLI: Index and constraint creation completed successfully.")
    except Exception as e:
        logger.error(f"CLI: An error occurred during index/constraint creation: {e}")

if __name__ == '__main__':
    cli()
