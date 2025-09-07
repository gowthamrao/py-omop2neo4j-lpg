import click
from .extraction import export_tables_to_csv
from .loading import run_load_csv, clear_database, create_constraints_and_indexes, get_driver
from .config import get_logger

logger = get_logger(__name__)

@click.group(context_settings=dict(help_option_names=['-h', '--help']))
def main():
    """
    A command-line interface for migrating OMOP vocabulary data from
    PostgreSQL to a Neo4j Labeled Property Graph.

    Configuration is managed via a .env file in the project root.
    """
    pass

@main.command()
def extract():
    """
    Extracts OMOP vocabulary tables from PostgreSQL to CSV files.
    The CSV files will be saved in the directory specified by EXPORT_DIR
    in your .env file (defaults to './export').
    """
    logger.info("CLI: Initiating 'extract' command.")
    try:
        export_tables_to_csv()
        logger.info("CLI: 'extract' command completed successfully.")
    except Exception as e:
        logger.error(f"CLI: An error occurred during the extraction process: {e}", exc_info=True)
        click.echo(f"Extraction failed. Check the log file for details.")


@main.command()
def load_csv():
    """
    Runs the full online loading process into Neo4j using LOAD CSV.

    This is a destructive operation that will first WIPE the entire Neo4j
    database. It then creates the schema (constraints/indexes) and loads
    all data from the CSV files in the export directory.
    """
    logger.info("CLI: Initiating 'load-csv' command.")
    if not click.confirm(
        "⚠️ This is a destructive operation that will WIPE the Neo4j database. "
        "Are you sure you want to continue?"
    ):
        click.echo("Command aborted by user.")
        return

    try:
        run_load_csv()
        logger.info("CLI: 'load-csv' command completed successfully.")
        click.echo("Successfully loaded all data into Neo4j.")
    except Exception as e:
        logger.error(f"CLI: An error occurred during the loading process: {e}", exc_info=True)
        click.echo(f"Loading failed. Check the log file for details.")


@main.command()
def clear_db():
    """
    Clears the Neo4j database.

    This command drops all constraints and indexes, then deletes all nodes
    and relationships.
    """
    logger.info("CLI: Initiating 'clear-db' command.")
    if not click.confirm(
        "⚠️ This will WIPE the Neo4j database completely. "
        "Are you sure you want to continue?"
    ):
        click.echo("Command aborted by user.")
        return

    driver = None
    try:
        driver = get_driver()
        clear_database(driver)
        logger.info("CLI: 'clear-db' command completed successfully.")
        click.echo("Neo4j database has been cleared.")
    except Exception as e:
        logger.error(f"CLI: An error occurred while clearing the database: {e}", exc_info=True)
        click.echo(f"Clearing the database failed. Check the log file for details.")
    finally:
        if driver:
            driver.close()


@main.command()
def create_indexes():
    """
    Creates all constraints and indexes for the OMOP graph model.
    This operation is idempotent and can be run safely multiple times.
    It is also run automatically as part of the `load-csv` command.
    """
    logger.info("CLI: Initiating 'create-indexes' command.")
    driver = None
    try:
        driver = get_driver()
        create_constraints_and_indexes(driver)
        logger.info("CLI: 'create-indexes' command completed successfully.")
        click.echo("Successfully created constraints and indexes.")
    except Exception as e:
        logger.error(f"CLI: An error occurred while creating indexes: {e}", exc_info=True)
        click.echo(f"Index creation failed. Check the log file for details.")
    finally:
        if driver:
            driver.close()

if __name__ == '__main__':
    main()
