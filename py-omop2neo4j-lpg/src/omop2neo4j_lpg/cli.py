import click
from .config import settings, logger
from . import extraction, loading, transformation, validation

@click.group(context_settings=dict(help_option_names=['-h', '--help']))
def main():
    """
    A CLI tool to orchestrate the migration of OMOP vocabulary tables
    from PostgreSQL to a Neo4j Labeled Property Graph.

    Configuration can be managed via a .env file in the current directory.
    """
    logger.info("py-omop2neo4j-lpg CLI started.")
    pass

@main.command()
@click.option('--schema', default=settings.pg_schema, show_default=True, help='PostgreSQL schema containing OMOP tables.')
@click.option('--export-dir', default=settings.export_dir, show_default=True, type=click.Path(), help='Local directory to export CSV files to.')
def extract(schema, export_dir):
    """Extracts OMOP tables from PostgreSQL to local CSV files."""
    logger.info(f"Starting extraction from schema '{schema}' to '{export_dir}'...")
    try:
        extraction.extract_data(schema=schema, export_dir=export_dir)
        logger.info("Extraction completed successfully.")
    except Exception as e:
        logger.error(f"Extraction failed: {e}", exc_info=True)
        raise

@main.command()
@click.confirmation_option(prompt='This will WIPE the entire Neo4j database. Are you sure you want to continue?')
def clear_db():
    """Wipes the Neo4j database, deleting all nodes, relationships, and schema."""
    logger.info("Starting database clearing process...")
    driver = None
    try:
        driver = loading.get_neo4j_driver()
        loading.clear_database(driver)
        logger.info("Successfully cleared the database.")
    except Exception as e:
        logger.error(f"Failed to clear the database: {e}", exc_info=True)
        raise
    finally:
        if driver:
            driver.close()

@main.command()
@click.option('--batch-size', default=settings.load_csv_batch_size, show_default=True, help='Batch size for LOAD CSV transactions.')
@click.option('--no-clean', is_flag=True, default=False, help="Do not clean the database before loading.")
def load_csv(batch_size, no_clean):
    """
    Loads data into Neo4j using the online LOAD CSV method.
    """
    click.secho("--- IMPORTANT ---", fg="yellow")
    click.echo(f"This command assumes the CSV files from the 'extract' command are present in your Neo4j server's configured import directory.")
    click.secho("-----------------", fg="yellow")

    logger.info("Starting online load process using LOAD CSV...")
    driver = None
    try:
        driver = loading.get_neo4j_driver()

        if not no_clean:
            if click.confirm("This will WIPE the entire Neo4j database before loading. Continue?"):
                loading.clear_database(driver)
            else:
                logger.warning("Load operation cancelled by user.")
                return

        loading.create_constraints_and_indexes(driver)
        loading.load_metadata(driver)
        loading.load_concepts(driver, batch_size=batch_size)
        loading.load_relationships(driver, batch_size=batch_size)
        loading.load_ancestors(driver, batch_size=batch_size)

        logger.info("Online load completed successfully.")
        click.secho("Running validation checks post-load...", fg="green")
        validation.validate_graph()

    except Exception as e:
        logger.error(f"Online load failed: {e}", exc_info=True)
        raise
    finally:
        if driver:
            driver.close()

@main.command()
@click.option('--import-dir', default=settings.export_dir, show_default=True, type=click.Path(exists=True), help='Directory containing the source CSVs.')
@click.option('--output-dir', default=settings.bulk_import_dir, show_default=True, type=click.Path(), help='Directory to save bulk-import-ready files.')
@click.option('--chunk-size', default=settings.transformation_chunk_size, show_default=True, help='Chunk size for processing large CSVs.')
def prepare_bulk(import_dir, output_dir, chunk_size):
    """Transforms extracted CSVs into files for neo4j-admin bulk import."""
    logger.info("Starting preparation for bulk import...")
    try:
        transformation.prepare_for_bulk_import(
            import_dir=import_dir, output_dir=output_dir, chunk_size=chunk_size
        )
    except Exception as e:
        logger.error(f"Bulk import preparation failed: {e}", exc_info=True)
        raise

@main.command()
def create_indexes():
    """Creates constraints and indexes. Essential after a bulk import."""
    logger.info("Creating constraints and indexes in Neo4j...")
    driver = None
    try:
        driver = loading.get_neo4j_driver()
        loading.create_constraints_and_indexes(driver)
    except Exception as e:
        logger.error(f"Failed to create constraints/indexes: {e}", exc_info=True)
        raise
    finally:
        if driver:
            driver.close()

@main.command()
def validate():
    """Runs validation checks against the loaded Neo4j graph."""
    try:
        validation.validate_graph()
    except Exception as e:
        logger.error(f"Validation failed: {e}", exc_info=True)
        raise

if __name__ == '__main__':
    main()
