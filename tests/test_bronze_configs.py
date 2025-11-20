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
    with open(yaml_file) as f:
        config = yaml.safe_load(f)
    
    assert config is not None, "YAML file is empty"
    assert isinstance(config, dict), "YAML must be a dictionary"


@pytest.mark.parametrize('yaml_file', yaml_files, ids=[str(f.name) for f in yaml_files])
def test_config_schema_valid(yaml_file):
    with open(yaml_file) as f:
        config = yaml.safe_load(f)
    
    result = ConfigValidator.validate_bronze_config(config)
    
    assert result['valid'], f"Validation errors: {result['errors']}"


def test_no_duplicate_tables():
    tables = {}
    
    for yaml_file in yaml_files:
        with open(yaml_file) as f:
            config = yaml.safe_load(f)
        
        key = f"{config['database']}.{config['table']}"
        
        if key in tables:
            pytest.fail(
                f"Duplicate table definition: {key}\n"
                f"  File 1: {tables[key]}\n"
                f"  File 2: {yaml_file}"
            )
        
        tables[key] = str(yaml_file)


def test_manual_schema_has_columns():
    for yaml_file in yaml_files:
        with open(yaml_file) as f:
            config = yaml.safe_load(f)
        
        if config.get('schema', {}).get('mode') == 'manual':
            assert 'columns' in config['schema'], \
                f"{yaml_file}: manual mode requires schema.columns"
            assert len(config['schema']['columns']) > 0, \
                f"{yaml_file}: schema.columns cannot be empty"


def test_all_columns_have_descriptions():
    for yaml_file in yaml_files:
        with open(yaml_file) as f:
            config = yaml.safe_load(f)
        
        if config.get('schema', {}).get('mode') == 'manual':
            columns = config['schema']['columns']
            for col in columns:
                assert 'description' in col, \
                    f"{yaml_file}: Column '{col.get('name')}' missing description"
                assert len(col['description']) > 10, \
                    f"{yaml_file}: Column '{col.get('name')}' description too short"


def test_pii_columns_tagged():
    for yaml_file in yaml_files:
        with open(yaml_file) as f:
            config = yaml.safe_load(f)
        if config.get('metadata', {}).get('contains_pii', False):
            assert 'pii_columns' in config['metadata'], \
                f"{yaml_file}: contains_pii=true but pii_columns not defined"
            assert len(config['metadata']['pii_columns']) > 0, \
                f"{yaml_file}: pii_columns list is empty"


def test_validation_enabled():
    for yaml_file in yaml_files:
        with open(yaml_file) as f:
            config = yaml.safe_load(f)
        
        schema_validation = config.get('schema', {}).get('validation', {})
        assert schema_validation.get('enabled', False), \
            f"{yaml_file}: schema validation should be enabled"
        assert schema_validation.get('on_mismatch') == 'fail', \
            f"{yaml_file}: on_mismatch should be 'fail' for production"