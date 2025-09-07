"""
Command-Line Interface for the OMOP to Neo4j ETL tool.

This module provides a CLI using 'click' to orchestrate the different
stages of the ETL process, including extraction, loading, and database
management tasks.
"""

import click
import json
from . import extraction
from . import loading
from . import validation
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

@cli.command()
@click.option('--concept-id', default=1177480, show_default=True, type=int, help='Concept ID for the structural validation check.')
def validate(concept_id):
    """
    Runs validation checks against the Neo4j database.
    """
    logger.info(f"CLI: Starting validation process for concept {concept_id}...")
    click.secho("--- Running Database Validation ---", fg="cyan")

    driver = None
    try:
        driver = loading.get_driver()

        # 1. Node Counts
        click.secho("\n[1/3] Node Counts by Label:", bold=True)
        node_counts = validation.get_node_counts(driver)
        if node_counts:
            for label, count in node_counts.items():
                click.echo(f"  - {label}: {count:,}")
        else:
            click.secho("  No nodes found.", fg="yellow")

        # 2. Relationship Counts
        click.secho("\n[2/3] Relationship Counts by Type:", bold=True)
        rel_counts = validation.get_relationship_counts(driver)
        if rel_counts:
            for rel_type, count in rel_counts.items():
                click.echo(f"  - {rel_type}: {count:,}")
        else:
            click.secho("  No relationships found.", fg="yellow")

        # 3. Sample Concept Verification
        click.secho(f"\n[3/3] Structural Validation for Concept ID: {concept_id}", bold=True)
        sample_data = validation.verify_sample_concept(driver, concept_id=concept_id)
        if sample_data:
            click.echo(f"  - Name: {sample_data.get('name')}")
            click.echo(f"  - Labels: {sample_data.get('labels')}")
            click.echo(f"  - Synonym Count: {sample_data.get('synonym_count')}")
            click.echo("  - Relationships:")
            if sample_data.get('relationships'):
                for rel_type, data in sample_data['relationships'].items():
                    click.echo(f"    - {rel_type} ({data['count']}): {data['sample_neighbors']}")
            else:
                 click.echo("    - None found.")
        else:
            click.secho(f"  Concept ID {concept_id} not found.", fg="red")

        click.secho("\n--- Validation Complete ---", fg="green")

    except Exception as e:
        logger.error(f"CLI: An error occurred during validation: {e}")
        click.secho(f"Error during validation: {e}", fg="red")
    finally:
        if driver:
            driver.close()


if __name__ == '__main__':
    cli()
