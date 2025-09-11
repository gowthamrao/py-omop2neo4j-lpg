import os
import pytest
from click.testing import CliRunner
from dotenv import load_dotenv

# Load test environment variables before other imports
load_dotenv(dotenv_path=".env.test")

from omop2neo4j.cli import cli
from omop2neo4j.config import settings


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(
        str(pytestconfig.rootdir), "docker-compose.test.yml"
    )


import psycopg2
import time

@pytest.fixture(scope="session")
def postgres_service(docker_services):
    # Healthcheck for postgres
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            conn = psycopg2.connect(
                host="localhost",
                port=5433,
                user="testuser",
                password="testpass",
                dbname="testdb"
            )
            conn.close()
            break
        except psycopg2.OperationalError:
            time.sleep(1)
    else:
        pytest.fail("PostgreSQL did not become available in 60 seconds.")


from neo4j import GraphDatabase

@pytest.fixture(scope="session")
def neo4j_service(docker_services):
    # Healthcheck for neo4j
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            driver = GraphDatabase.driver("bolt://localhost:7688", auth=("neo4j", "StrongPass123"))
            driver.verify_connectivity()
            driver.close()
            break
        except Exception:
            time.sleep(1)
    else:
        pytest.fail("Neo4j did not become available in 60 seconds.")


def test_full_etl_pipeline(postgres_service, neo4j_service, docker_services):
    try:
        runner = CliRunner()

        # 1. Extract
        result_extract = runner.invoke(cli, ["extract"])
        assert result_extract.exit_code == 0
        assert os.path.exists(os.path.join(settings.EXPORT_DIR, "concepts_optimized.csv"))

        # 2. Load CSV
        result_load = runner.invoke(cli, ["load-csv"])
        assert result_load.exit_code == 0

        # 3. Validate
        result_validate = runner.invoke(cli, ["validate"])
        assert result_validate.exit_code == 0
        assert '"Concept:Drug:Standard": 1' in result_validate.output
        assert '"Concept:Condition:Standard": 1' in result_validate.output
        assert '"Domain": 2' in result_validate.output
        assert '"Vocabulary": 2' in result_validate.output
        assert '"TREATS": 1' in result_validate.output
        assert '"MAPS_TO": 1' in result_validate.output
        assert '"HAS_ANCESTOR": 1' in result_validate.output

    finally:
        # Print logs if the test fails
        logs = docker_services._docker_compose.execute("logs postgres-test")
        print(logs)

        # Clean up
        for f in os.listdir(settings.EXPORT_DIR):
            os.remove(os.path.join(settings.EXPORT_DIR, f))
        os.rmdir(settings.EXPORT_DIR)


import tempfile
import shutil

def test_prepare_bulk_workflow(postgres_service, neo4j_service, docker_services):
    runner = CliRunner()
    # Use a temporary directory for the bulk import files
    bulk_import_dir = tempfile.mkdtemp()

    try:
        # 1. Extract
        result_extract = runner.invoke(cli, ["extract"])
        assert result_extract.exit_code == 0
        assert os.path.exists(os.path.join(settings.EXPORT_DIR, "concepts_optimized.csv"))

        # 2. Prepare for bulk import
        result_prepare = runner.invoke(
            cli, ["prepare-bulk", "--import-dir", bulk_import_dir]
        )
        assert result_prepare.exit_code == 0

        # 3. Verify file creation
        # The `prepare-bulk` command creates several files. We'll check for a few key ones.
        assert os.path.exists(os.path.join(bulk_import_dir, "nodes_concept.csv"))
        assert os.path.exists(os.path.join(bulk_import_dir, "rels_semantic.csv"))


        # 4. Verify command output
        assert "neo4j-admin database import full" in result_prepare.output
        assert "--nodes='nodes_concept.csv'" in result_prepare.output
        assert "--relationships='rels_semantic.csv'" in result_prepare.output

    finally:
        # Clean up created files
        shutil.rmtree(bulk_import_dir)
        if os.path.exists(settings.EXPORT_DIR):
            for f in os.listdir(settings.EXPORT_DIR):
                os.remove(os.path.join(settings.EXPORT_DIR, f))
            os.rmdir(settings.EXPORT_DIR)
