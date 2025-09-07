import logging
import os
from pydantic_settings import BaseSettings, SettingsConfigDict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """
    Manages configuration for the application using Pydantic.
    Loads settings from environment variables or a .env file.
    """
    # PostgreSQL Connection Settings
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_user: str = "postgres"
    pg_password: str = "password"
    pg_database: str = "ohdsi"
    pg_schema: str = "cdm_v5"

    # Neo4j Connection Settings
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "password"
    neo4j_database: str = "neo4j"

    # File Paths
    export_dir: str = "export"
    # The directory for neo4j-admin import files. Must be a path inside the Neo4j container's import directory.
    bulk_import_dir: str = "import"

    # Tuning Parameters
    load_csv_batch_size: int = 10000
    transformation_chunk_size: int = 100000

    # Load settings from a .env file
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding='utf-8', extra='ignore')


# Instantiate settings once and export
settings = Settings()

# Ensure the local directory for CSV exports exists
try:
    os.makedirs(settings.export_dir, exist_ok=True)
    logger.info(f"Export directory '{settings.export_dir}' is ready.")
except OSError as e:
    logger.error(f"Failed to create export directory '{settings.export_dir}': {e}")
