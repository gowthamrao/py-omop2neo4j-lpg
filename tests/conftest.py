import os
import shutil
import pytest

@pytest.fixture(autouse=True)
def clean_export_dir():
    """Cleans the export_test directory before each test."""
    export_dir = "./export_test"
    if os.path.exists(export_dir):
        for item in os.listdir(export_dir):
            item_path = os.path.join(export_dir, item)
            if os.path.isdir(item_path):
                shutil.rmtree(item_path)
            else:
                os.remove(item_path)
