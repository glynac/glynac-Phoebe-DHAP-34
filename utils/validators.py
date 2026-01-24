"""
Configuration validators for airflow-dag-configs.
Validates Bronze, Silver, and Gold layer configurations.
"""

from typing import Dict, List, Any


class ConfigValidator:
    """Validates configuration files for data pipeline layers."""

    # Required fields for Bronze configs (table OR view)
    BRONZE_REQUIRED_FIELDS = ['version', 'database']

    # Required fields for Silver configs (manual mode)
    SILVER_MANUAL_REQUIRED_FIELDS = ['version', 'layer', 'database', 'table']

    # Required fields for Silver configs (auto mode)
    SILVER_AUTO_REQUIRED_FIELDS = ['version', 'source', 'target']

    # Valid ClickHouse data types
    VALID_CLICKHOUSE_TYPES = [
        'String', 'Int8', 'Int16', 'Int32', 'Int64', 'UInt8', 'UInt16', 'UInt32', 'UInt64',
        'Float32', 'Float64', 'Decimal', 'Date', 'Date32', 'DateTime', 'DateTime64',
        'Bool', 'Boolean', 'UUID', 'IPv4', 'IPv6', 'Enum8', 'Enum16', 'Array', 'Tuple', 'Map',
        'Nullable', 'LowCardinality', 'FixedString', 'JSON', 'Object', 'SimpleAggregateFunction'
    ]

    # Valid engine types
    VALID_ENGINES = [
        'MergeTree', 'ReplacingMergeTree', 'SummingMergeTree', 'AggregatingMergeTree',
        'CollapsingMergeTree', 'VersionedCollapsingMergeTree', 'S3', 'Memory', 'Log'
    ]

    @classmethod
    def validate_bronze_config(cls, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate Bronze layer configuration.

        Args:
            config: The configuration dictionary to validate

        Returns:
            Dictionary with 'valid' (bool) and 'errors' (list of error messages)
        """
        errors = []

        if not config:
            return {'valid': False, 'errors': ['Config is empty or None']}

        # Check required fields
        for field in cls.BRONZE_REQUIRED_FIELDS:
            if field not in config:
                errors.append(f"Missing required field: {field}")

        # Must have either 'table' or 'view'
        if 'table' not in config and 'view' not in config:
            errors.append("Missing required field: 'table' or 'view'")

        # Validate version format
        if 'version' in config:
            version = str(config['version'])
            if not version.replace('.', '').isdigit():
                errors.append(f"Invalid version format: {version}")

        # Validate database name
        if 'database' in config:
            db_name = config['database']
            if not isinstance(db_name, str) or not db_name:
                errors.append("Database name must be a non-empty string")
            elif not db_name.replace('_', '').isalnum():
                errors.append(f"Invalid database name: {db_name}")

        # Validate table name
        if 'table' in config:
            table_name = config['table']
            if not isinstance(table_name, str) or not table_name:
                errors.append("Table name must be a non-empty string")
            elif not table_name.replace('_', '').isalnum():
                errors.append(f"Invalid table name: {table_name}")

        # Validate schema if present
        if 'schema' in config:
            schema_errors = cls._validate_schema(config['schema'])
            errors.extend(schema_errors)

        # Validate clickhouse settings if present
        if 'clickhouse' in config:
            ch_errors = cls._validate_clickhouse_settings(config['clickhouse'])
            errors.extend(ch_errors)

        # Validate metadata if present
        if 'metadata' in config:
            meta_errors = cls._validate_metadata(config['metadata'])
            errors.extend(meta_errors)

        return {'valid': len(errors) == 0, 'errors': errors}

    @classmethod
    def validate_silver_config(cls, config: Dict[str, Any], mode: str = 'auto') -> Dict[str, Any]:
        """
        Validate Silver layer configuration.

        Args:
            config: The configuration dictionary to validate
            mode: 'auto' or 'manual'

        Returns:
            Dictionary with 'valid' (bool) and 'errors' (list of error messages)
        """
        errors = []

        if not config:
            return {'valid': False, 'errors': ['Config is empty or None']}

        required_fields = cls.SILVER_AUTO_REQUIRED_FIELDS if mode == 'auto' else cls.SILVER_MANUAL_REQUIRED_FIELDS

        for field in required_fields:
            if field not in config:
                errors.append(f"Missing required field: {field}")

        # Validate source/target for auto mode
        if mode == 'auto':
            if 'source' in config:
                source = config['source']
                if 'database' not in source:
                    errors.append("source.database is required")
                if 'table' not in source:
                    errors.append("source.table is required")

            if 'target' in config:
                target = config['target']
                if 'database' not in target:
                    errors.append("target.database is required")
                if 'table' not in target:
                    errors.append("target.table is required")

        # Validate transforms if present
        if 'transforms' in config:
            transform_errors = cls._validate_transforms(config['transforms'])
            errors.extend(transform_errors)

        return {'valid': len(errors) == 0, 'errors': errors}

    @classmethod
    def _validate_schema(cls, schema: Dict[str, Any]) -> List[str]:
        """Validate schema configuration."""
        errors = []

        if not isinstance(schema, dict):
            return ["Schema must be a dictionary"]

        # Validate mode
        mode = schema.get('mode', 'auto')
        if mode not in ['auto', 'manual']:
            errors.append(f"Invalid schema mode: {mode}")

        # Manual mode requires columns
        if mode == 'manual':
            if 'columns' not in schema:
                errors.append("Manual schema mode requires 'columns' field")
            elif not isinstance(schema['columns'], list):
                errors.append("Schema columns must be a list")
            else:
                for i, col in enumerate(schema['columns']):
                    col_errors = cls._validate_column(col, i)
                    errors.extend(col_errors)

        return errors

    @classmethod
    def _validate_column(cls, column: Dict[str, Any], index: int) -> List[str]:
        """Validate a single column definition."""
        errors = []

        if not isinstance(column, dict):
            return [f"Column at index {index} must be a dictionary"]

        # Name is required
        if 'name' not in column:
            errors.append(f"Column at index {index} missing 'name'")
        elif not isinstance(column['name'], str):
            errors.append(f"Column name at index {index} must be a string")

        # Type is required
        if 'type' not in column:
            errors.append(f"Column '{column.get('name', index)}' missing 'type'")
        else:
            col_type = column['type']
            # Extract base type (handle Nullable, LowCardinality, etc.)
            base_type = col_type.split('(')[0] if '(' in col_type else col_type
            if base_type not in cls.VALID_CLICKHOUSE_TYPES:
                errors.append(f"Invalid column type '{col_type}' for column '{column.get('name', index)}'")

        return errors

    @classmethod
    def _validate_clickhouse_settings(cls, ch_config: Dict[str, Any]) -> List[str]:
        """Validate ClickHouse configuration."""
        errors = []

        if not isinstance(ch_config, dict):
            return ["ClickHouse config must be a dictionary"]

        # Validate engine
        if 'engine' in ch_config:
            engine = ch_config['engine']
            if engine not in cls.VALID_ENGINES:
                errors.append(f"Invalid ClickHouse engine: {engine}")

        # Validate partition_by
        if 'partition_by' in ch_config:
            partition_by = ch_config['partition_by']
            if not isinstance(partition_by, str):
                errors.append("partition_by must be a string")

        # Validate order_by
        if 'order_by' in ch_config:
            order_by = ch_config['order_by']
            if not isinstance(order_by, (str, list)):
                errors.append("order_by must be a string or list")

        return errors

    @classmethod
    def _validate_metadata(cls, metadata: Dict[str, Any]) -> List[str]:
        """Validate metadata configuration."""
        errors = []

        if not isinstance(metadata, dict):
            return ["Metadata must be a dictionary"]

        # Validate owner
        if 'owner' in metadata and not isinstance(metadata['owner'], str):
            errors.append("metadata.owner must be a string")

        # Validate tags
        if 'tags' in metadata:
            if not isinstance(metadata['tags'], list):
                errors.append("metadata.tags must be a list")

        # Validate PII settings consistency (warnings, not errors)
        # PII columns are recommended but not required
        # if metadata.get('contains_pii', False):
        #     if 'pii_columns' not in metadata:
        #         warnings.append("contains_pii=true but pii_columns not defined")
        pass

        return errors

    @classmethod
    def _validate_transforms(cls, transforms: Dict[str, Any]) -> List[str]:
        """Validate transform configurations."""
        errors = []

        if not isinstance(transforms, dict):
            return ["Transforms must be a dictionary"]

        valid_transforms = [
            'clean_string', 'parse_date', 'parse_money', 'normalize_status',
            'extract_domain', 'boolean_to_int', 'passthrough',
            'clean_email', 'clean_phone', 'trim', 'uppercase', 'lowercase',
            'parse_datetime', 'parse_timestamp', 'parse_boolean', 'parse_int',
            'parse_float', 'parse_decimal', 'coalesce', 'default_value'
        ]

        for column, transform_config in transforms.items():
            if isinstance(transform_config, dict):
                transform_type = transform_config.get('transform')
                if transform_type and transform_type not in valid_transforms:
                    errors.append(f"Invalid transform type '{transform_type}' for column '{column}'")
            elif isinstance(transform_config, str):
                if transform_config not in valid_transforms:
                    errors.append(f"Invalid transform type '{transform_config}' for column '{column}'")

        return errors
