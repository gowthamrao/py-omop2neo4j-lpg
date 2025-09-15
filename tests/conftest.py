import os
import shutil
import pytest
from omop2neo4j_lpg.config import settings

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
