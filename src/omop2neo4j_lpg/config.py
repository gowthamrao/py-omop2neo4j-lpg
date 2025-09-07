import logging
import sys
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

# --- Project Root Directory ---
# This helps in creating absolute paths for file operations, like the export dir or .env file.
ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_EXPORT_DIR = ROOT_DIR / "export"


# --- Pydantic Settings ---
# All configuration is managed here, loaded from environment variables or a .env file.
class Settings(BaseSettings):
    """
    Manages application configuration using Pydantic.
    Reads from environment variables or a .env file.
    """
    # PostgreSQL Connection
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "password"
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "ohdsi"
    OMOP_SCHEMA: str = "cdm_synthea_v1"

    # Neo4j Connection
    NEO4J_URI: str = "neo4j://localhost:7687"
    NEO4J_USER: str = "neo4j"
    NEO4J_PASSWORD: str = "password"

    # Data Directories
    EXPORT_DIR: Path = DEFAULT_EXPORT_DIR

    # Tuning Parameters
    LOAD_CSV_BATCH_SIZE: int = 10000
    TRANSFORMATION_CHUNK_SIZE: int = 100000

    model_config = SettingsConfigDict(
        env_file=str(ROOT_DIR / ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
    )


# --- Instantiate Settings ---
# A single, global instance of the settings for the application.
settings = Settings()

# --- Create Export Directory ---
# The directory for CSV exports is created on startup if it doesn't exist.
settings.EXPORT_DIR.mkdir(parents=True, exist_ok=True)


# --- Logging Configuration ---
# A structured and consistent logging setup for the entire application.
def get_logger(name: str) -> logging.Logger:
    """
    Configures and returns a logger instance.
    """
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(ROOT_DIR / "omop2neo4j.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return logging.getLogger(name)
