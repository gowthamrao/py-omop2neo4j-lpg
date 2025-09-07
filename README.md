# py-omop2neo4j-lpg

A robust, high-performance Python package to orchestrate the migration of OMOP vocabulary tables from PostgreSQL to a mature Labeled Property Graph (LPG) in Neo4j.

## Overview

This package provides a suite of command-line tools to manage the ETL process for moving OMOP vocabulary data into a Neo4j graph database. It is designed for performance and scalability, prioritizing the use of native database tools (`COPY` and `LOAD CSV`) and providing two main loading strategies:

1.  **Online Loading (`load-csv`):** An easy-to-use method that streams data directly into a running Neo4j instance. Ideal for most standard vocabulary sizes.
2.  **Offline Loading (`prepare-bulk`):** A high-performance method for extremely large datasets that prepares data for Neo4j's offline `neo4j-admin` import tool. (This command is a placeholder and not yet fully implemented).

This document focuses on the **Online Loading** method.

## Prerequisites

*   Python 3.8+
*   Docker and Docker Compose (recommended for running Neo4j)
*   Access to a PostgreSQL database with the OMOP CDM vocabulary tables.

## 1. Neo4j Setup (Recommended)

The `load-csv` command requires a running Neo4j instance with the **APOC** plugin installed. The easiest way to set this up is with Docker. The command also needs access to the generated CSV files, which requires mounting a local directory to the Neo4j container's `/import` directory.

Here is a reference `docker-compose.yml` file to configure the service correctly:

```yaml
# docker-compose.yml
version: '3.8'
services:
  neo4j:
    image: neo4j:latest
    container_name: neo4j-omop-vocab
    environment:
      - NEO4J_AUTH=neo4j/your_strong_password
      - NEO4J_apoc_import_file_enabled=true
      - NEO4J_apoc_import_file_use__neo4j__config=true
      - NEO4JLABS_PLUGINS=["apoc"]
      # Example Memory Tuning (Adjust based on your hardware)
      - NEO4J_server_memory_heap_initial_size=4G
      - NEO4J_server_memory_heap_max_size=4G
      - NEO4J_server_memory_pagecache_size=8G
    ports:
      - "7474:7474"
      - "7687:7687"
    volumes:
      - ./neo4j/data:/data
      # Mounts the local 'export' directory to the container's '/import' directory
      - ./export:/import
```

To start the service, save the file as `docker-compose.yml` and run:
```bash
docker-compose up -d
```

## 2. Package Installation and Configuration

1.  **Install Package:**
    It is recommended to install the package in a Python virtual environment.
    ```bash
    # Clone the repository (if you haven't already)
    # git clone <repo_url>
    # cd py-omop2neo4j-lpg

    # Install
    pip install .
    ```

2.  **Configure Environment:**
    The application is configured using environment variables. Create a `.env` file in the root of the project directory by copying the example file:
    ```bash
    cp .env.example .env
    ```
    Now, edit the `.env` file with your specific database credentials. **Make sure the `NEO4J_PASSWORD` matches the one you set in `docker-compose.yml`**.

## 3. Usage (Online `load-csv` Workflow)

The package provides a single command-line interface, `py-omop2neo4j-lpg`.

### Step 1: Extract Data from PostgreSQL

Run the `extract` command to export the necessary vocabulary tables from PostgreSQL into CSV files. The files will be saved in the directory specified by `EXPORT_DIR` (default: `./export`), which is the same directory mounted into the Neo4j container.

```bash
py-omop2neo4j-lpg extract
```

### Step 2: Load Data into Neo4j

Run the `load-csv` command to perform a full reload of the Neo4j database. This single command will automatically:
1.  Clear the entire Neo4j database.
2.  Create the necessary constraints and indexes.
3.  Load all the data from the CSV files.

```bash
py-omop2neo4j-lpg load-csv
```

The process can take several minutes depending on the size of the vocabulary and your hardware. Check the logs for detailed progress.

### Utility Commands

The following commands are also available:

*   **`clear-db`**: Use this command to only wipe the Neo4j database without loading new data.
    ```bash
    py-omop2neo4j-lpg clear-db
    ```
*   **`create-indexes`**: Use this to apply the schema (constraints and indexes) to an existing database. This is mainly useful after a manual or bulk import.
    ```bash
    py-omop2neo4j-lpg create-indexes
    ```
