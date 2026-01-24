"""
Test Silver Layer Configurations
Supports both AUTO mode (config.yaml only) and MANUAL mode (4 files)
"""

import pytest
import yaml
from pathlib import Path
from utils.validators import ConfigValidator

SILVER_CONFIG_DIR = Path(__file__).parent.parent / 'config' / 'silver'


def get_table_folders():
    """
    Get all table folders recursively from silver config directory.
    Includes nested folders like silver/orion/transaction/
    """
    folders = []

    for source_dir in SILVER_CONFIG_DIR.iterdir():
        if source_dir.is_dir() and not source_dir.name.startswith('_'):
            # Check if this is a source folder (contains table subfolders)
            # or a direct table folder (contains config files)
            has_config_file = any(
                f.suffix in ['.yaml', '.yml', '.sql']
                for f in source_dir.iterdir() if f.is_file()
            )

            if has_config_file:
                # This is a table folder directly under silver/
                folders.append(source_dir)
            else:
                # This is a source folder (e.g., silver/orion/)
                for table_dir in source_dir.iterdir():
                    if table_dir.is_dir() and not table_dir.name.startswith('_'):
                        folders.append(table_dir)

    return folders


def detect_mode(table_folder: Path) -> str:
    """
    Detect if a table folder uses AUTO or MANUAL mode.
    AUTO mode: Has config.yaml only
    MANUAL mode: Has query.sql, schema.yaml, dag.yaml, tests.yaml
    """
    has_config_yaml = (table_folder / 'config.yaml').exists()
    has_query_sql = (table_folder / 'query.sql').exists()
    has_schema_yaml = (table_folder / 'schema.yaml').exists()

    if has_config_yaml and not has_query_sql:
        return 'auto'
    elif has_query_sql and has_schema_yaml:
        return 'manual'
    else:
        # Default to auto if unclear
        return 'auto'


def get_folder_id(folder: Path) -> str:
    """Generate a test ID from folder path."""
    # Get relative path from silver dir
    try:
        rel_path = folder.relative_to(SILVER_CONFIG_DIR)
        return str(rel_path)
    except ValueError:
        return folder.name


# Discover all table folders
table_folders = get_table_folders()


@pytest.mark.parametrize('table_folder', table_folders, ids=[get_folder_id(f) for f in table_folders])
def test_required_files_exist(table_folder):
    """Test that all required files exist based on mode"""
    mode = detect_mode(table_folder)

    if mode == 'auto':
        required_files = ['config.yaml']
    else:
        required_files = ['query.sql', 'schema.yaml', 'dag.yaml', 'tests.yaml']

    for file_name in required_files:
        file_path = table_folder / file_name
        assert file_path.exists(), f"Missing required file: {file_name} (mode={mode})"


@pytest.mark.parametrize('table_folder', table_folders, ids=[get_folder_id(f) for f in table_folders])
def test_yaml_syntax_valid(table_folder):
    """Test that YAML files are valid"""
    mode = detect_mode(table_folder)

    if mode == 'auto':
        yaml_files = ['config.yaml']
    else:
        yaml_files = ['schema.yaml', 'dag.yaml', 'tests.yaml']

    for yaml_file in yaml_files:
        file_path = table_folder / yaml_file
        if file_path.exists():
            with open(file_path) as f:
                config = yaml.safe_load(f)
            assert config is not None, f"{yaml_file} is empty"


@pytest.mark.parametrize('table_folder', table_folders, ids=[get_folder_id(f) for f in table_folders])
def test_sql_has_source_reference(table_folder):
    """Test that SQL has source references (manual mode only)"""
    mode = detect_mode(table_folder)

    if mode != 'manual':
        pytest.skip("Auto mode doesn't have query.sql")

    sql_path = table_folder / 'query.sql'
    with open(sql_path) as f:
        sql = f.read()

    assert '{{ source(' in sql or 'FROM ' in sql.upper(), \
        "SQL must reference source tables"


@pytest.mark.parametrize('table_folder', table_folders, ids=[get_folder_id(f) for f in table_folders])
def test_incremental_logic_present(table_folder):
    """Test that SQL has incremental loading logic (manual mode only)"""
    mode = detect_mode(table_folder)

    if mode != 'manual':
        pytest.skip("Auto mode doesn't have query.sql")

    sql_path = table_folder / 'query.sql'
    with open(sql_path) as f:
        sql = f.read()

    assert '{{ ds }}' in sql or 'processing_date' in sql.lower(), \
        "SQL should have incremental loading logic"


@pytest.mark.parametrize('table_folder', table_folders, ids=[get_folder_id(f) for f in table_folders])
def test_auto_mode_config_structure(table_folder):
    """Test that auto mode configs have required fields"""
    mode = detect_mode(table_folder)

    if mode != 'auto':
        pytest.skip("Manual mode uses different config structure")

    config_path = table_folder / 'config.yaml'
    with open(config_path) as f:
        config = yaml.safe_load(f)

    result = ConfigValidator.validate_silver_config(config, mode='auto')
    assert result['valid'], f"Validation errors: {result['errors']}"


@pytest.mark.parametrize('table_folder', table_folders, ids=[get_folder_id(f) for f in table_folders])
def test_manual_mode_schema_has_columns(table_folder):
    """Test that manual mode schema has columns defined"""
    mode = detect_mode(table_folder)

    if mode != 'manual':
        pytest.skip("Auto mode infers schema from bronze")

    schema_path = table_folder / 'schema.yaml'
    with open(schema_path) as f:
        schema = yaml.safe_load(f)

    assert 'columns' in schema, "Manual mode requires columns in schema.yaml"
    assert len(schema['columns']) > 0, "Schema columns cannot be empty"


@pytest.mark.parametrize('table_folder', table_folders, ids=[get_folder_id(f) for f in table_folders])
def test_target_database_naming(table_folder):
    """Test that target database follows naming convention ({source}_silver)"""
    mode = detect_mode(table_folder)

    if mode == 'auto':
        config_path = table_folder / 'config.yaml'
        with open(config_path) as f:
            config = yaml.safe_load(f)
        target_db = config.get('target', {}).get('database', '')
    else:
        schema_path = table_folder / 'schema.yaml'
        with open(schema_path) as f:
            schema = yaml.safe_load(f)
        target_db = schema.get('database', '')

    assert target_db.endswith('_silver'), \
        f"Target database '{target_db}' should end with '_silver'"


@pytest.mark.parametrize('table_folder', table_folders, ids=[get_folder_id(f) for f in table_folders])
def test_dedupe_configuration(table_folder):
    """Test that deduplication is configured (auto mode)"""
    mode = detect_mode(table_folder)

    if mode != 'auto':
        pytest.skip("Manual mode handles dedupe in SQL")

    config_path = table_folder / 'config.yaml'
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Dedupe is recommended but not required
    if 'dedupe' in config:
        dedupe = config['dedupe']
        assert 'partition_by' in dedupe, "dedupe.partition_by is required"
        assert 'order_by' in dedupe, "dedupe.order_by is required"


@pytest.mark.parametrize('table_folder', table_folders, ids=[get_folder_id(f) for f in table_folders])
def test_filters_are_valid(table_folder):
    """Test that filters are valid SQL expressions (auto mode)"""
    mode = detect_mode(table_folder)

    if mode != 'auto':
        pytest.skip("Manual mode handles filters in SQL")

    config_path = table_folder / 'config.yaml'
    with open(config_path) as f:
        config = yaml.safe_load(f)

    if 'filters' in config:
        filters = config['filters']
        assert isinstance(filters, list), "filters must be a list"
        for f in filters:
            assert isinstance(f, str), f"Filter must be a string: {f}"
            # Basic SQL syntax check
            assert any(keyword in f.upper() for keyword in ['IS', '=', '!=', '<', '>', 'IN', 'LIKE', 'NOT']), \
                f"Filter doesn't look like valid SQL: {f}"
