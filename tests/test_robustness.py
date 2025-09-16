import os
import pytest
from click.testing import CliRunner

from py_omop2neo4j_lpg.cli import cli
from py_omop2neo4j_lpg.config import settings


def test_etl_idempotency_and_clear(postgres_service, neo4j_service):
    runner = CliRunner()

    # 1. Extract
    result_extract = runner.invoke(cli, ["extract"])
    assert result_extract.exit_code == 0
    assert os.path.exists(os.path.join(settings.EXPORT_DIR, "concepts_optimized.csv"))

    #
    #
    # 2. Load CSV - First run
    result_load_1 = runner.invoke(cli, ["load-csv"])
    assert result_load_1.exit_code == 0

    # 3. Validate - First run
    result_validate_1 = runner.invoke(cli, ["validate"])
    assert result_validate_1.exit_code == 0
    assert '"Concept:Drug:Standard": 1' in result_validate_1.output

    # 4. Load CSV - Second run (idempotency check)
    result_load_2 = runner.invoke(cli, ["load-csv"])
    assert result_load_2.exit_code == 0

    # 5. Validate - Second run
    result_validate_2 = runner.invoke(cli, ["validate"])
    assert result_validate_2.exit_code == 0
    assert '"Concept:Drug:Standard": 1' in result_validate_2.output

    # 6. Clear the database
    result_clear = runner.invoke(cli, ["clear-db"])
    assert result_clear.exit_code == 0

    # 7. Validate - After clear
    result_validate_3 = runner.invoke(cli, ["validate"])
    assert result_validate_3.exit_code == 0
    assert '"node_counts_by_label_combination": {}' in result_validate_3.output
    assert '"relationship_counts_by_type": {}' in result_validate_3.output
