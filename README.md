# py-omop2neo4j-lpg

A robust, high-performance Python package to orchestrate the migration of OMOP vocabulary tables from PostgreSQL to a mature Labeled Property Graph (LPG) in Neo4j.

## Overview

This package provides a suite of command-line tools to manage the ETL process for moving OMOP vocabulary data into a Neo4j graph database. It is designed for performance and scalability, prioritizing the use of native database tools (`COPY` and `LOAD CSV`) wherever possible.

## Prerequisites

- Python 3.8+
- Access to a PostgreSQL database with the OMOP CDM vocabulary tables.
- A running Neo4j instance.

## Setup and Configuration

1.  **Install Dependencies:**
    It is recommended to install the package in a virtual environment.
    ```bash
    pip install .
    ```

2.  **Configure Environment:**
    The application is configured using environment variables. Create a `.env` file in the root of the project directory by copying the example file:
    ```bash
    cp .env.example .env
    ```
    Now, edit the `.env` file with your specific database credentials and settings.

    ```dotenv
    # .env file
    # PostgreSQL Connection Settings
    POSTGRES_HOST=localhost
    POSTGRES_PORT=5432
    POSTGRES_USER=your_postgres_user
    POSTGRES_PASSWORD=your_postgres_password
    POSTGRES_DB=your_database
    OMOP_SCHEMA=public # Your OMOP CDM schema

    # Neo4j Connection Settings
    NEO4J_URI=bolt://localhost:7687
    NEO4J_USER=neo4j
    NEO4J_PASSWORD=your_neo4j_password

    # ETL Configuration
    EXPORT_DIR=export
    ```

## Usage

The package provides a single command-line interface, `py-omop2neo4j-lpg`.

### Extract Data from PostgreSQL

To extract the necessary vocabulary tables from PostgreSQL into CSV files, run the `extract` command. The files will be saved in the directory specified by `EXPORT_DIR` (default: `export`).

```bash
py-omop2neo4j-lpg extract
```

This will create the following files in your export directory:
- `concepts_optimized.csv`
- `domain.csv`
- `vocabulary.csv`
- `concept_relationship.csv`
- `concept_ancestor.csv`
