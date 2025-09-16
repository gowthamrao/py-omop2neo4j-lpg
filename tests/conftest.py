import os
import shutil
import pytest
from py_omop2neo4j_lpg.config import settings

@pytest.fixture(scope="session")
def test_export_dir():
    """Creates a directory for test exports."""
    export_dir = "./export_test"
    os.makedirs(export_dir, exist_ok=True)
    yield export_dir

@pytest.fixture(autouse=True)
def clean_export_dir(test_export_dir):
    """Cleans the export_test directory before each test."""
    export_dir = test_export_dir
    if os.path.exists(export_dir):
        for item in os.listdir(export_dir):
            item_path = os.path.join(export_dir, item)
            if os.path.isdir(item_path):
                shutil.rmtree(item_path)
            else:
                os.remove(item_path)

@pytest.fixture(autouse=True)
def monkeypatch_settings(monkeypatch, test_export_dir):
    """Monkeypatches the settings for the test environment."""
    monkeypatch.setattr(settings, "EXPORT_DIR", test_export_dir)


import psycopg2
import time
from neo4j import GraphDatabase

@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(
        str(pytestconfig.rootdir), "docker-compose.test.yml"
    )

@pytest.fixture(scope="session")
def postgres_service():
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


@pytest.fixture(scope="session")
def neo4j_service():
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
