import click
from .config import logger, settings
from .extraction import export_tables_to_csv

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
    (Not Implemented) Clears the Neo4j database.
    """
    logger.warning("Command 'clear-db' is not yet implemented.")
    click.echo("Command 'clear-db' is not yet implemented.")

@main.command()
@click.option('--batch-size', default=None, help=f'Batch size for LOAD CSV transactions. Default: {settings.LOAD_CSV_BATCH_SIZE}')
def load_csv(batch_size):
    """
    (Not Implemented) Loads data into Neo4j using LOAD CSV.
    """
    final_batch_size = batch_size if batch_size is not None else settings.LOAD_CSV_BATCH_SIZE
    logger.warning("Command 'load-csv' is not yet implemented.")
    click.echo(f"Command 'load-csv' is not yet implemented. Batch size would be {final_batch_size}")

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
    (Not Implemented) Creates constraints and indexes in Neo4j.
    """
    logger.warning("Command 'create-indexes' is not yet implemented.")
    click.echo("Command 'create-indexes' is not yet implemented.")

@main.command()
def validate():
    """
    (Not Implemented) Validates the loaded graph data.
    """
    logger.warning("Command 'validate' is not yet implemented.")
    click.echo("Command 'validate' is not yet implemented.")
