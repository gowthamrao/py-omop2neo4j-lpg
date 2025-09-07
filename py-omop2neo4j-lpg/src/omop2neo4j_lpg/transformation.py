import pandas as pd
import os
from .config import settings, logger
from .utils import standardize_label, standardize_reltype

def _write_chunk(df, filepath, is_first_chunk):
    """Helper to write DataFrame chunk to a CSV file."""
    df.to_csv(filepath, mode='a', header=is_first_chunk, index=False)

def transform_metadata_nodes(import_dir, output_dir):
    """Transforms domain and vocabulary CSVs into node files for bulk import."""
    logger.info("Transforming metadata nodes (Domain, Vocabulary)...")

    # Domain nodes
    domain_file = os.path.join(import_dir, 'domain.csv')
    df_domain = pd.read_csv(domain_file, dtype=str)
    df_domain[':LABEL'] = 'Domain'
    df_domain.rename(columns={'domain_id': ':ID(Domain-ID)'}, inplace=True)
    domain_nodes = df_domain[[':ID(Domain-ID)', ':LABEL', 'domain_name', 'domain_concept_id']]
    domain_nodes.to_csv(os.path.join(output_dir, 'nodes_domain.csv'), index=False)

    # Vocabulary nodes
    vocab_file = os.path.join(import_dir, 'vocabulary.csv')
    df_vocab = pd.read_csv(vocab_file, dtype=str)
    df_vocab[':LABEL'] = 'Vocabulary'
    df_vocab.rename(columns={'vocabulary_id': ':ID(Vocabulary-ID)'}, inplace=True)
    vocab_nodes = df_vocab[[':ID(Vocabulary-ID)', ':LABEL', 'vocabulary_name', 'vocabulary_reference', 'vocabulary_version', 'vocabulary_concept_id']]
    vocab_nodes.to_csv(os.path.join(output_dir, 'nodes_vocabulary.csv'), index=False)
    logger.info("Metadata node transformation complete.")

def transform_concepts_and_contextual_rels(import_dir, output_dir, chunk_size):
    """Transforms concept CSV into node and contextual relationship files, using chunking."""
    logger.info("Transforming concept nodes and contextual relationships...")
    source_file = os.path.join(import_dir, 'concepts_optimized.csv')
    node_file = os.path.join(output_dir, 'nodes_concepts.csv')
    rel_domain_file = os.path.join(output_dir, 'rels_in_domain.csv')
    rel_vocab_file = os.path.join(output_dir, 'rels_from_vocabulary.csv')

    # Ensure files are empty before starting
    for f in [node_file, rel_domain_file, rel_vocab_file]:
        if os.path.exists(f): os.remove(f)

    iter_csv = pd.read_csv(source_file, chunksize=chunk_size, dtype=str, keep_default_na=False)

    for i, chunk in enumerate(iter_csv):
        is_first = (i == 0)
        # --- Process Nodes ---
        chunk.rename(columns={
            'concept_id': ':ID(Concept-ID)',
            'concept_name': 'name:string',
            'synonyms': 'synonyms:string[]',
            'valid_start_date': 'valid_start_date:date',
            'valid_end_date': 'valid_end_date:date'
        }, inplace=True)

        chunk['domain_label'] = chunk['domain_id'].apply(standardize_label)
        chunk[':LABEL'] = 'Concept;' + chunk['domain_label']
        chunk.loc[chunk['standard_concept'] == 'S', ':LABEL'] += ';Standard'

        node_cols = [':ID(Concept-ID)', ':LABEL', 'name:string', 'domain_id', 'vocabulary_id',
                     'concept_class_id', 'standard_concept', 'concept_code',
                     'valid_start_date:date', 'valid_end_date:date', 'invalid_reason', 'synonyms:string[]']
        _write_chunk(chunk[node_cols], node_file, is_first)

        # --- Process Contextual Relationships ---
        rels_domain_chunk = pd.DataFrame({
            ':START_ID(Concept-ID)': chunk[':ID(Concept-ID)'],
            ':END_ID(Domain-ID)': chunk['domain_id'],
            ':TYPE': 'IN_DOMAIN'
        })
        _write_chunk(rels_domain_chunk, rel_domain_file, is_first)

        rels_vocab_chunk = pd.DataFrame({
            ':START_ID(Concept-ID)': chunk[':ID(Concept-ID)'],
            ':END_ID(Vocabulary-ID)': chunk['vocabulary_id'],
            ':TYPE': 'FROM_VOCABULARY'
        })
        _write_chunk(rels_vocab_chunk, rel_vocab_file, is_first)
        logger.info(f"Processed concept chunk {i+1}...")

def transform_semantic_rels(import_dir, output_dir, chunk_size):
    """Transforms relationship CSV into semantic relationship files, using chunking."""
    logger.info("Transforming semantic relationships...")
    source_file = os.path.join(import_dir, 'concept_relationship.csv')
    output_file = os.path.join(output_dir, 'rels_semantic.csv')
    if os.path.exists(output_file): os.remove(output_file)

    iter_csv = pd.read_csv(source_file, chunksize=chunk_size, dtype=str, keep_default_na=False)

    for i, chunk in enumerate(iter_csv):
        chunk.rename(columns={
            'concept_id_1': ':START_ID(Concept-ID)',
            'concept_id_2': ':END_ID(Concept-ID)',
            'valid_start_date': 'valid_start_date:date',
            'valid_end_date': 'valid_end_date:date'
        }, inplace=True)
        chunk[':TYPE'] = chunk['relationship_id'].apply(standardize_reltype)

        rel_cols = [':START_ID(Concept-ID)', ':END_ID(Concept-ID)', ':TYPE',
                    'valid_start_date:date', 'valid_end_date:date', 'invalid_reason']
        _write_chunk(chunk[rel_cols], output_file, (i == 0))
        logger.info(f"Processed semantic relationship chunk {i+1}...")

def transform_ancestor_rels(import_dir, output_dir, chunk_size):
    """Transforms ancestor CSV into ancestor relationship files, using chunking."""
    logger.info("Transforming ancestor relationships...")
    source_file = os.path.join(import_dir, 'concept_ancestor.csv')
    output_file = os.path.join(output_dir, 'rels_ancestor.csv')
    if os.path.exists(output_file): os.remove(output_file)

    iter_csv = pd.read_csv(source_file, chunksize=chunk_size, dtype=str, keep_default_na=False)

    for i, chunk in enumerate(iter_csv):
        chunk.rename(columns={
            'descendant_concept_id': ':START_ID(Concept-ID)',
            'ancestor_concept_id': ':END_ID(Concept-ID)',
            'min_levels_of_separation': 'min_levels:int',
            'max_levels_of_separation': 'max_levels:int'
        }, inplace=True)
        chunk[':TYPE'] = 'HAS_ANCESTOR'

        rel_cols = [':START_ID(Concept-ID)', ':END_ID(Concept-ID)', ':TYPE', 'min_levels:int', 'max_levels:int']
        _write_chunk(chunk[rel_cols], output_file, (i == 0))
        logger.info(f"Processed ancestor relationship chunk {i+1}...")

def generate_import_command(output_dir, db_name):
    """Generates and prints the neo4j-admin import command."""
    command = f"""
# --- neo4j-admin database import full command ---
# 1. Stop your Neo4j server.
# 2. Ensure the files listed below are in your Neo4j 'import' directory.
#    The path '{output_dir}' should be relative to the Neo4j import directory.
# 3. Run the command below from your shell. Adjust memory settings if needed.
# 4. After a successful import, start your Neo4j server.
# 5. Run 'py-omop2neo4j-lpg create-indexes' to build schema indexes.

neo4j-admin database import full \\
  --nodes="{os.path.join(output_dir, 'nodes_domain.csv')}" \\
  --nodes="{os.path.join(output_dir, 'nodes_vocabulary.csv')}" \\
  --nodes="{os.path.join(output_dir, 'nodes_concepts.csv')}" \\
  --relationships="{os.path.join(output_dir, 'rels_in_domain.csv')}" \\
  --relationships="{os.path.join(output_dir, 'rels_from_vocabulary.csv')}" \\
  --relationships="{os.path.join(output_dir, 'rels_semantic.csv')}" \\
  --relationships="{os.path.join(output_dir, 'rels_ancestor.csv')}" \\
  --delimiter=',' --array-delimiter='|' --multiline-fields=true \\
  {db_name}
"""
    logger.info("--- Generated neo4j-admin import command ---")
    print(command)

def prepare_for_bulk_import(
    import_dir: str = settings.export_dir,
    output_dir: str = settings.bulk_import_dir,
    chunk_size: int = settings.transformation_chunk_size
):
    """
    Orchestrates the transformation of extracted CSVs into files
    formatted for Neo4j's bulk import tool.
    """
    logger.info(f"Starting transformation for bulk import. Chunk size: {chunk_size}")
    os.makedirs(output_dir, exist_ok=True)

    transform_metadata_nodes(import_dir, output_dir)
    transform_concepts_and_contextual_rels(import_dir, output_dir, chunk_size)
    transform_semantic_rels(import_dir, output_dir, chunk_size)
    transform_ancestor_rels(import_dir, output_dir, chunk_size)

    generate_import_command(output_dir, settings.neo4j_database)

    logger.info(f"Transformation complete. Files for bulk import are ready in '{output_dir}'.")
