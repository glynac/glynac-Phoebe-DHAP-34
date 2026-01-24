"""
Test Bronze Layer Configurations
"""

import pytest
import yaml
from pathlib import Path
from utils.validators import ConfigValidator

# Get config directory
TESTS_DIR = Path(__file__).parent
CONFIG_DIR = TESTS_DIR.parent / 'config' / 'bronze'
yaml_files = [f for f in CONFIG_DIR.rglob('*.yaml') if not f.name.startswith('_')]


@pytest.mark.parametrize('yaml_file', yaml_files, ids=[str(f.name) for f in yaml_files])
def test_yaml_syntax_valid(yaml_file):
    """Test that YAML files are valid syntax"""
    with open(yaml_file) as f:
        config = yaml.safe_load(f)

    assert config is not None, "YAML file is empty"
    assert isinstance(config, dict), "YAML must be a dictionary"


@pytest.mark.parametrize('yaml_file', yaml_files, ids=[str(f.name) for f in yaml_files])
def test_config_schema_valid(yaml_file):
    """Test that config has required fields"""
    with open(yaml_file) as f:
        config = yaml.safe_load(f)

    result = ConfigValidator.validate_bronze_config(config)

    assert result['valid'], f"Validation errors: {result['errors']}"


def test_no_duplicate_tables():
    """Test that there are no duplicate table/view definitions"""
    tables = {}

    for yaml_file in yaml_files:
        with open(yaml_file) as f:
            config = yaml.safe_load(f)

        # Support both 'table' and 'view' fields
        table_name = config.get('table') or config.get('view')
        if not table_name:
            continue

        key = f"{config.get('database', 'unknown')}.{table_name}"

        if key in tables:
            pytest.fail(
                f"Duplicate table definition: {key}\n"
                f"  File 1: {tables[key]}\n"
                f"  File 2: {yaml_file}"
            )

        tables[key] = str(yaml_file)


def test_manual_schema_has_columns():
    """Test that manual mode schemas have columns defined"""
    for yaml_file in yaml_files:
        with open(yaml_file) as f:
            config = yaml.safe_load(f)

        if config.get('schema', {}).get('mode') == 'manual':
            assert 'columns' in config['schema'], \
                f"{yaml_file}: manual mode requires schema.columns"
            assert len(config['schema']['columns']) > 0, \
                f"{yaml_file}: schema.columns cannot be empty"


def test_required_fields_present():
    """Test that all configs have required fields"""
    for yaml_file in yaml_files:
        with open(yaml_file) as f:
            config = yaml.safe_load(f)

        assert 'version' in config, f"{yaml_file}: missing 'version'"
        assert 'database' in config, f"{yaml_file}: missing 'database'"
        assert 'table' in config or 'view' in config, \
            f"{yaml_file}: missing 'table' or 'view'"


def test_source_config_present():
    """Test that source configuration is defined"""
    for yaml_file in yaml_files:
        with open(yaml_file) as f:
            config = yaml.safe_load(f)

        # Source config should exist
        assert 'source' in config, f"{yaml_file}: missing 'source' configuration"
        source = config['source']

        # Should have bucket and path
        assert 'bucket' in source or 'path' in source, \
            f"{yaml_file}: source should have 'bucket' or 'path'"


@pytest.mark.parametrize('yaml_file', yaml_files, ids=[str(f.name) for f in yaml_files])
def test_database_naming_convention(yaml_file):
    """Test that database names follow naming convention"""
    with open(yaml_file) as f:
        config = yaml.safe_load(f)

    db_name = config.get('database', '')

    # Database name should be lowercase, alphanumeric with underscores
    assert db_name.islower() or db_name.replace('_', '').isalnum(), \
        f"Database name '{db_name}' should be lowercase with underscores"
