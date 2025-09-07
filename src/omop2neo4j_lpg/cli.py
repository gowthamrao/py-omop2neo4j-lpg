import click
from .config import logger, settings
from .extraction import export_tables_to_csv
from .loading import (
    get_driver,
    clear_database,
    create_constraints_and_indexes,
    run_load_csv,
)

@click.group()
@click.version_option()
def main():
    """
    A command-line interface for migrating OMOP vocabulary from PostgreSQL to Neo4j.
    """
    pass

@main.command()
def extract():
    """
    Extracts OMOP vocabulary tables from PostgreSQL to CSV files.
    """
    logger.info("CLI 'extract' command initiated.")
    try:
        logger.info(f"Using PostgreSQL schema: {settings.OMOP_SCHEMA}")
        logger.info(f"Exporting to directory: {settings.EXPORT_DIR}")
        export_tables_to_csv()
        logger.info("CLI 'extract' command completed successfully.")
        click.echo("Extraction completed successfully. Check logs for details.")
    except Exception as e:
        logger.error(f"An error occurred during the extraction process: {e}", exc_info=True)
        raise click.ClickException("Extraction failed. See logs for details.")

@main.command()
def clear_db():
    """
    Clears the Neo4j database (deletes all nodes, relationships, indexes, constraints).
    """
    logger.info("CLI 'clear-db' command initiated.")
    try:
        driver = get_driver()
        clear_database(driver)
        driver.close()
        logger.info("CLI 'clear-db' command completed successfully.")
        click.echo("Neo4j database cleared successfully.")
    except Exception as e:
        logger.error(f"An error occurred during the database clearing process: {e}", exc_info=True)
        raise click.ClickException("Database clearing failed. See logs for details.")

@main.command()
def load_csv():
    """
    Loads data into Neo4j using the online LOAD CSV method.
    This performs a full reload: clears the DB, creates schema, and loads all CSVs.
    """
    logger.info("CLI 'load-csv' command initiated.")
    try:
        run_load_csv()
        logger.info("CLI 'load-csv' command completed successfully.")
        click.echo("LOAD CSV process completed successfully. See logs for details.")
    except Exception as e:
        logger.error(f"An error occurred during the LOAD CSV process: {e}", exc_info=True)
        raise click.ClickException("LOAD CSV process failed. See logs for details.")

@main.command()
@click.option('--chunk-size', default=None, help=f'Chunk size for processing large files. Default: {settings.TRANSFORMATION_CHUNK_SIZE}')
def prepare_bulk(chunk_size):
    """
    (Not Implemented) Prepares CSVs for neo4j-admin bulk import.
    """
    final_chunk_size = chunk_size if chunk_size is not None else settings.TRANSFORMATION_CHUNK_SIZE
    logger.warning("Command 'prepare-bulk' is not yet implemented.")
    click.echo(f"Command 'prepare-bulk' is not yet implemented. Chunk size would be {final_chunk_size}")

@main.command()
def create_indexes():
    """
    Creates constraints and indexes in Neo4j. Useful after a bulk import.
    """
    logger.info("CLI 'create-indexes' command initiated.")
    try:
        driver = get_driver()
        create_constraints_and_indexes(driver)
        driver.close()
        logger.info("CLI 'create-indexes' command completed successfully.")
        click.echo("Constraints and indexes created successfully.")
    except Exception as e:
        logger.error(f"An error occurred during index creation: {e}", exc_info=True)
        raise click.ClickException("Index creation failed. See logs for details.")

@main.command()
def validate():
    """
    (Not Implemented) Validates the loaded graph data.
    """
    logger.warning("Command 'validate' is not yet implemented.")
    click.echo("Command 'validate' is not yet implemented.")
