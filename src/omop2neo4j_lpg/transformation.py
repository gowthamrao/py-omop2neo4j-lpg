import os
import pandas as pd
import numpy as np
from .config import settings, get_logger
from .utils import standardize_label, standardize_reltype

logger = get_logger(__name__)

def prepare_for_bulk_import(chunk_size: int, import_dir: str):
    """
    Transforms extracted CSVs into a format suitable for neo4j-admin import.
    - Creates header files and data files for nodes and relationships.
    - Processes large files in chunks to manage memory usage.
    - Returns the neo4j-admin command to be executed.
    """
    source_dir = settings.EXPORT_DIR
    os.makedirs(import_dir, exist_ok=True)
    logger.info(f"Preparing bulk import files in directory: {import_dir}")

    # --- Define File Paths ---
    paths = {
        # Input
        'domain_in': os.path.join(source_dir, 'domain.csv'),
        'vocabulary_in': os.path.join(source_dir, 'vocabulary.csv'),
        'concept_in': os.path.join(source_dir, 'concepts_optimized.csv'),
        'relationship_in': os.path.join(source_dir, 'concept_relationship.csv'),
        'ancestor_in': os.path.join(source_dir, 'concept_ancestor.csv'),
        # Output Nodes
        'domain_nodes': os.path.join(import_dir, 'nodes_domain.csv'),
        'vocabulary_nodes': os.path.join(import_dir, 'nodes_vocabulary.csv'),
        'concept_nodes': os.path.join(import_dir, 'nodes_concept.csv'),
        # Output Rels
        'in_domain_rels': os.path.join(import_dir, 'rels_in_domain.csv'),
        'from_vocab_rels': os.path.join(import_dir, 'rels_from_vocabulary.csv'),
        'semantic_rels': os.path.join(import_dir, 'rels_semantic.csv'),
        'ancestor_rels': os.path.join(import_dir, 'rels_ancestor.csv'),
    }

    # --- Clear previous import files ---
    for key, path in paths.items():
        if 'in' not in key and os.path.exists(path):
            os.remove(path)
            logger.debug(f"Removed existing file: {path}")

    # --- Process Metadata (Small Files) ---
    logger.info("Processing Domain and Vocabulary nodes...")
    # Domains
    df_domain = pd.read_csv(paths['domain_in'], dtype=str)
    df_domain[':LABEL'] = 'Domain'
    df_domain.rename(columns={'domain_id': ':ID(Domain)'}, inplace=True)
    df_domain.to_csv(paths['domain_nodes'], index=False)

    # Vocabularies
    df_vocab = pd.read_csv(paths['vocabulary_in'], dtype=str)
    df_vocab[':LABEL'] = 'Vocabulary'
    df_vocab.rename(columns={'vocabulary_id': ':ID(Vocabulary)'}, inplace=True)
    df_vocab.to_csv(paths['vocabulary_nodes'], index=False)
    logger.info("Metadata processing complete.")

    # --- Process Concepts (Chunked) ---
    logger.info(f"Processing concepts in chunks of {chunk_size}...")
    concept_cols = {
        "concept_id": ":ID(Concept)", "concept_name": "name:string",
        "concept_code": "concept_code:string",
        "standard_concept": "standard_concept:string", "invalid_reason": "invalid_reason:string",
        "valid_start_date": "valid_start_date:date", "valid_end_date": "valid_end_date:date",
        "synonyms": "synonyms:string[]"
    }
    is_first_chunk = True
    for chunk in pd.read_csv(paths['concept_in'], chunksize=chunk_size, dtype=str, keep_default_na=False):
        # 1. Prepare Concept Nodes
        chunk[':LABEL'] = 'Concept;' + chunk['domain_id'].apply(standardize_label)
        chunk.loc[chunk['standard_concept'] == 'S', ':LABEL'] += ';Standard'

        # Rename columns for neo4j-admin
        chunk.rename(columns=concept_cols, inplace=True)

        # Select and write node data
        node_data = chunk[list(concept_cols.values()) + [':LABEL']]
        node_data.to_csv(paths['concept_nodes'], mode='a', index=False, header=is_first_chunk)

        # 2. Prepare Contextual Relationships
        # IN_DOMAIN
        rels_domain = chunk[[':ID(Concept)', 'domain_id']].copy()
        rels_domain.rename(columns={'domain_id': ':END_ID(Domain)'}, inplace=True)
        rels_domain[':TYPE'] = 'IN_DOMAIN'
        rels_domain.to_csv(paths['in_domain_rels'], mode='a', index=False, header=is_first_chunk)

        # FROM_VOCABULARY
        rels_vocab = chunk[[':ID(Concept)', 'vocabulary_id']].copy()
        rels_vocab.rename(columns={'vocabulary_id': ':END_ID(Vocabulary)'}, inplace=True)
        rels_vocab[':TYPE'] = 'FROM_VOCABULARY'
        rels_vocab.to_csv(paths['from_vocab_rels'], mode='a', index=False, header=is_first_chunk)

        is_first_chunk = False
    logger.info("Concept processing complete.")

    # --- Process Semantic Relationships (Chunked) ---
    logger.info(f"Processing concept relationships in chunks of {chunk_size}...")
    is_first_chunk = True
    for chunk in pd.read_csv(paths['relationship_in'], chunksize=chunk_size, dtype=str, keep_default_na=False):
        chunk.rename(columns={
            'concept_id_1': ':START_ID(Concept)',
            'concept_id_2': ':END_ID(Concept)',
            'valid_start_date': 'valid_start_date:date',
            'valid_end_date': 'valid_end_date:date',
            'invalid_reason': 'invalid_reason:string'
        }, inplace=True)
        chunk[':TYPE'] = chunk['relationship_id'].apply(standardize_reltype)
        chunk.drop(columns=['relationship_id'], inplace=True)
        chunk.to_csv(paths['semantic_rels'], mode='a', index=False, header=is_first_chunk)
        is_first_chunk = False
    logger.info("Semantic relationship processing complete.")

    # --- Process Ancestor Relationships (Chunked) ---
    logger.info(f"Processing concept ancestors in chunks of {chunk_size}...")
    is_first_chunk = True
    for chunk in pd.read_csv(paths['ancestor_in'], chunksize=chunk_size, dtype=str, keep_default_na=False):
        chunk.rename(columns={
            'descendant_concept_id': ':START_ID(Concept)',
            'ancestor_concept_id': ':END_ID(Concept)',
            'min_levels_of_separation': 'min_levels:int',
            'max_levels_of_separation': 'max_levels:int'
        }, inplace=True)
        chunk[':TYPE'] = 'HAS_ANCESTOR'
        chunk.to_csv(paths['ancestor_rels'], mode='a', index=False, header=is_first_chunk)
        is_first_chunk = False
    logger.info("Ancestor relationship processing complete.")

    # --- Generate Header Files and Final Command ---
    logger.info("Generating header files and neo4j-admin command...")
    node_files = {
        'domain_nodes': 'nodes_domain_header.csv',
        'vocabulary_nodes': 'nodes_vocabulary_header.csv',
        'concept_nodes': 'nodes_concept_header.csv'
    }
    rel_files = {
        'in_domain_rels': 'rels_in_domain_header.csv',
        'from_vocab_rels': 'rels_from_vocabulary_header.csv',
        'semantic_rels': 'rels_semantic_header.csv',
        'ancestor_rels': 'rels_ancestor_header.csv'
    }

    command_parts = ["neo4j-admin database import full \\"]

    for key, header_name in node_files.items():
        df_header = pd.read_csv(paths[key], nrows=0)
        header_path = os.path.join(import_dir, header_name)
        data_path = os.path.join(import_dir, os.path.basename(paths[key]))
        df_header.to_csv(header_path, index=False)
        command_parts.append(f"  --nodes='{header_path}' \\")
        command_parts.append(f"  --nodes='{data_path}' \\")

    for key, header_name in rel_files.items():
        df_header = pd.read_csv(paths[key], nrows=0)
        header_path = os.path.join(import_dir, header_name)
        data_path = os.path.join(import_dir, os.path.basename(paths[key]))
        df_header.to_csv(header_path, index=False)
        command_parts.append(f"  --relationships='{header_path}' \\")
        command_parts.append(f"  --relationships='{data_path}' \\")

    command_parts.append("  --delimiter=',' --array-delimiter='|' --multiline-fields=true \\")
    command_parts.append("  neo4j") # Target database name

    final_command = "\n".join(command_parts)
    logger.info(f"Generated neo4j-admin command:\n{final_command}")

    return final_command
