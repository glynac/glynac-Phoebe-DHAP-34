"""
Validation tests for all configuration files in the airflow-dag-configs repository.
These tests ensure YAML syntax, SQL syntax, and structural integrity of config files.
"""

import os
import yaml
import glob
import pytest
from pathlib import Path


# Get the repository root directory
REPO_ROOT = Path(__file__).parent.parent
CONFIG_DIR = REPO_ROOT / "config"

class TestYAMLValidation:
    """Test all YAML files for valid syntax"""

    def get_all_yaml_files(self):
        """Get all YAML files in the config directory"""
        yaml_files = []
        for pattern in ["**/*.yaml", "**/*.yml"]:
            yaml_files.extend(glob.glob(str(CONFIG_DIR / pattern), recursive=True))
        return yaml_files

    def test_yaml_files_exist(self):
        """Test that YAML files exist in the config directory"""
        yaml_files = self.get_all_yaml_files()
        assert len(yaml_files) > 0, "No YAML files found in config directory"

    def test_yaml_syntax_valid(self):
        """Test that all YAML files have valid syntax"""
        yaml_files = self.get_all_yaml_files()

        for yaml_file in yaml_files:
            try:
                with open(yaml_file, 'r') as f:
                    yaml.safe_load(f)
            except yaml.YAMLError as e:
                pytest.fail(f"Invalid YAML syntax in {yaml_file}: {e}")
            except Exception as e:
                pytest.fail(f"Error reading {yaml_file}: {e}")


class TestSQLValidation:
    """Test all SQL files for basic syntax checks"""

    def get_all_sql_files(self):
        """Get all SQL files in the config directory"""
        return glob.glob(str(CONFIG_DIR / "**/*.sql"), recursive=True)

    def test_sql_files_exist(self):
        """Test that SQL files exist in the config directory"""
        sql_files = self.get_all_sql_files()
        assert len(sql_files) > 0, "No SQL files found in config directory"

    def test_sql_not_empty(self):
        """Test that SQL files are not empty"""
        sql_files = self.get_all_sql_files()

        for sql_file in sql_files:
            with open(sql_file, 'r') as f:
                content = f.read().strip()
                assert len(content) > 0, f"SQL file is empty: {sql_file}"

    def test_sql_basic_syntax(self):
        """Test basic SQL syntax (contains SELECT, CREATE, INSERT, etc.)"""
        sql_files = self.get_all_sql_files()

        for sql_file in sql_files:
            with open(sql_file, 'r') as f:
                content = f.read().upper()
                # Check if file contains at least one SQL keyword
                sql_keywords = ['SELECT', 'CREATE', 'INSERT', 'UPDATE', 'DELETE', 'WITH']
                has_keyword = any(keyword in content for keyword in sql_keywords)
                assert has_keyword, f"SQL file doesn't contain any SQL keywords: {sql_file}"


class TestGlobalConfig:
    """Test global configuration files"""

    def test_global_config_exists(self):
        """Test that global_config.yaml exists"""
        global_config = CONFIG_DIR / "global_config.yaml"
        assert global_config.exists(), "global_config.yaml not found"

    def test_global_config_structure(self):
        """Test that global_config.yaml has required sections"""
        global_config = CONFIG_DIR / "global_config.yaml"

        with open(global_config, 'r') as f:
            config = yaml.safe_load(f)

        # Check for required top-level keys
        assert config is not None, "global_config.yaml is empty"
        assert isinstance(config, dict), "global_config.yaml must be a dictionary"

    def test_monitoring_config_exists(self):
        """Test that monitoring_config.yaml exists"""
        monitoring_config = CONFIG_DIR / "monitoring_config.yaml"
        assert monitoring_config.exists(), "monitoring_config.yaml not found"


class TestBronzeConfig:
    """Test Bronze layer configuration structure"""

    def test_bronze_directory_exists(self):
        """Test that bronze config directory exists"""
        bronze_dir = CONFIG_DIR / "bronze"
        assert bronze_dir.exists(), "bronze directory not found"

    def test_bronze_configs_exist(self):
        """Test that Bronze layer configs exist"""
        bronze_configs = glob.glob(str(CONFIG_DIR / "bronze/**/*.yaml"), recursive=True)
        assert len(bronze_configs) > 0, "No Bronze layer configs found"


class TestSilverConfig:
    """Test Silver layer configuration structure"""

    def test_silver_directory_exists(self):
        """Test that silver config directory exists"""
        silver_dir = CONFIG_DIR / "silver"
        assert silver_dir.exists(), "silver directory not found"

    def test_silver_configs_exist(self):
        """Test that Silver layer configs exist"""
        silver_configs = glob.glob(str(CONFIG_DIR / "silver/**/*.yaml"), recursive=True)
        assert len(silver_configs) > 0, "No Silver layer configs found"


class TestGoldConfig:
    """Test Gold layer configuration structure"""

    def test_gold_directory_exists(self):
        """Test that gold config directory exists"""
        gold_dir = CONFIG_DIR / "gold"
        assert gold_dir.exists(), "gold directory not found"

    def test_gold_configs_exist(self):
        """Test that Gold layer configs exist"""
        gold_configs = glob.glob(str(CONFIG_DIR / "gold/**/*.yaml"), recursive=True)
        assert len(gold_configs) > 0, "No Gold layer configs found"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
