# py-omop2neo4j-lpg

A robust, high-performance Python package to orchestrate the migration of OMOP vocabulary tables from PostgreSQL to a mature Labeled Property Graph (LPG) in Neo4j.

This tool is designed for efficiency and scalability, using native database bulk loading tools wherever possible to handle large vocabulary datasets.

## Key Features

- **Two Loading Methods**:
  - **Online `LOAD CSV`**: For smaller datasets or when the Neo4j instance must remain online.
  - **Offline `neo4j-admin import`**: A high-performance method for very large datasets that can afford downtime.
- **Mature Graph Model**: Creates a rich, query-optimized graph with dynamic labels for domains (e.g., `:Drug`), dynamic relationship types (e.g., `:MAPS_TO`), and special labels for query optimization (e.g., `:Standard`).
- **Performance Focused**: Uses PostgreSQL `COPY` and Neo4j's native import tools to avoid slow, client-side data streaming.
- **Memory Efficient**: Implements chunking for the offline transformation step to handle datasets that exceed available RAM.
- **Simple CLI**: A user-friendly command-line interface to orchestrate the entire ETL process.

## Prerequisites

1.  **Python**: Python 3.9+
2.  **PostgreSQL**: A running PostgreSQL instance with the OMOP CDM vocabulary tables.
3.  **Neo4j**: A running Neo4j 5.x instance.
4.  **APOC Plugin**: The [APOC plugin](https://neo4j.com/labs/apoc/) must be installed in Neo4j. This is required for both loading methods.
5.  **Docker (Recommended)**: A `docker-compose.yml` is provided for easy setup of a pre-configured Neo4j instance.

## Installation

1.  Clone the repository:
    ```bash
    git clone <repository_url>
    cd py-omop2neo4j-lpg
    ```
2.  Create a virtual environment and install the package in editable mode:
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -e .
    ```

## Configuration

The tool is configured using environment variables. You can create a `.env` file in the project root directory to manage them.

1.  Copy the example file:
    ```bash
    cp .env.example .env
    ```
2.  Edit the `.env` file with your database credentials and preferred settings. The tool will automatically load these settings.

## Usage

The tool provides two main workflows for migrating the data.

### Method 1: Online Loading via `LOAD CSV`

This method is suitable for smaller vocabularies or situations where the Neo4j database must remain online.

**Step 1: Extract Data from PostgreSQL**

This command connects to PostgreSQL and exports the necessary vocabulary tables into CSV files in the directory specified by `EXPORT_DIR` (default: `export/`).

```bash
py-omop2neo4j-lpg extract
```

**Step 2: Place CSVs in Neo4j Import Directory**

The `LOAD CSV` command requires the CSV files to be inside the Neo4j server's configured `import` directory. If you are using the provided `docker-compose.yml`, the local `./export` directory is automatically mapped to the container's `/import` directory, so this step is already handled.

**Step 3: Run the Online Loader**

This command will connect to Neo4j and run a series of `LOAD CSV` Cypher queries to import the data. It will wipe the database first (after confirmation) and run validation checks upon completion.

```bash
py-omop2neo4j-lpg load-csv
```

### Method 2: Offline Loading via `neo4j-admin import`

This is the fastest method and is recommended for very large vocabularies. It requires the Neo4j service to be stopped during the import.

**Step 1: Extract Data from PostgreSQL**

This is the same as in Method 1.
```bash
py-omop2neo4j-lpg extract
```

**Step 2: Prepare Files for Bulk Import**

This command reads the extracted CSVs and transforms them into a set of new files specially formatted for the `neo4j-admin` tool. These new files will be saved in a separate directory (default: `import_transformed/`).

```bash
py-omop2neo4j-lpg prepare-bulk
```
This will output a `neo4j-admin database import full ...` command to your console.

**Step 3: Run the Bulk Import**

1.  **Stop Neo4j**:
    ```bash
    docker-compose stop neo4j
    ```
2.  **Run the command**: Copy the `neo4j-admin` command printed in the previous step and run it. **Note:** You must execute this command *inside* the stopped container.
    ```bash
    # Example for running the command with Docker
    docker-compose exec neo4j <paste_the_entire_neo4j-admin_command_here>
    ```
3.  **Set Permissions**: The import tool may create files owned by `root`. Fix ownership so the `neo4j` user can start the database.
    ```bash
    docker-compose exec -u root neo4j chown -R neo4j:neo4j /data
    ```
4.  **Restart Neo4j**:
    ```bash
    docker-compose start neo4j
    ```

**Step 4: Create Indexes**

After the bulk import is complete and Neo4j is running again, you **must** create the schema constraints and indexes.

```bash
py-omop2neo4j-lpg create-indexes
```

### Validation

You can run the validation checks at any time to verify the integrity of the graph:

```bash
py-omop2neo4j-lpg validate
```

## Performance Tuning Guide

For large vocabularies, you must configure Neo4j's memory settings appropriately. These can be set in your `neo4j.conf` file or as environment variables in the `docker-compose.yml` file.

-   `NEO4J_server_memory_heap_initial_size` & `NEO4J_server_memory_heap_max_size`: Controls the Java heap space. The import process can be memory-intensive. A good starting point is `4G` to `8G`. Set both to the same value for best performance.
-   `NEO4J_server_memory_pagecache_size`: This is critical for query performance *after* the import. It should be large enough to hold the graph data. For the import process itself, it's less critical but still important. A setting of `8G` or more is recommended for large graphs.

Monitor your system's memory usage and adjust these settings based on your available hardware.

## CLI Command Reference

-   `extract`: Extracts data from PostgreSQL to CSVs.
-   `clear-db`: Wipes the Neo4j database.
-   `load-csv`: Imports data using the online `LOAD CSV` method.
-   `prepare-bulk`: Prepares CSVs for the offline bulk importer.
-   `create-indexes`: Creates schema constraints and indexes.
-   `validate`: Runs post-load validation checks.

Use `py-omop2neo4j-lpg <command> --help` for more details on each command.
