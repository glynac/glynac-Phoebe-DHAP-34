"""
Test Silver Layer Configurations
"""

import pytest
import yaml
from pathlib import Path

SILVER_CONFIG_DIR = Path(__file__).parent.parent / 'config' / 'silver'

# Discover all table folders
table_folders = [f for f in SILVER_CONFIG_DIR.iterdir() if f.is_dir()]

@pytest.mark.parametrize('table_folder', table_folders, ids=[f.name for f in table_folders])
def test_required_files_exist(table_folder):
    """Test that all required files exist"""
    required_files = ['query.sql', 'schema.yaml', 'dag.yaml', 'tests.yaml']
    
    for file_name in required_files:
        file_path = table_folder / file_name
        assert file_path.exists(), f"Missing required file: {file_name}"


@pytest.mark.parametrize('table_folder', table_folders, ids=[f.name for f in table_folders])
def test_yaml_syntax_valid(table_folder):
    """Test that YAML files are valid"""
    for yaml_file in ['schema.yaml', 'dag.yaml', 'tests.yaml']:
        with open(table_folder / yaml_file) as f:
            config = yaml.safe_load(f)
        assert config is not None


@pytest.mark.parametrize('table_folder', table_folders, ids=[f.name for f in table_folders])
def test_sql_has_source_reference(table_folder):
    """Test that SQL has {{ source() }} references"""
    sql_path = table_folder / 'query.sql'
    with open(sql_path) as f:
        sql = f.read()
    
    assert '{{ source(' in sql or 'FROM ' in sql.upper(), \
        "SQL must reference source tables"


@pytest.mark.parametrize('table_folder', table_folders, ids=[f.name for f in table_folders])
def test_incremental_logic_present(table_folder):
    """Test that SQL has incremental loading logic"""
    sql_path = table_folder / 'query.sql'
    with open(sql_path) as f:
        sql = f.read()
    
    assert '{{ ds }}' in sql or 'processing_date' in sql.lower(), \
        "SQL should have incremental loading logic"