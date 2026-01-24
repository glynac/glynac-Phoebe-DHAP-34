"""
Schema validation utilities for data pipelines
"""
import os
import yaml
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
import re


class SchemaValidator:
    """Validate DataFrame against YAML schema definition"""
    
    def __init__(self, schema_path: str):
        """Initialize with schema file path"""
        self.schema_path = schema_path
        self.schema = self._load_schema()
        
    def _load_schema(self) -> Dict[str, Any]:
        """Load schema from YAML file"""
        if not os.path.exists(self.schema_path):
            raise FileNotFoundError(f"Schema file not found: {self.schema_path}")
            
        with open(self.schema_path, 'r') as f:
            return yaml.safe_load(f)
    
    def validate_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate DataFrame against schema
        
        Returns:
            Dict with validation results and errors
        """
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'row_count': len(df),
            'column_count': len(df.columns)
        }
        
        # Check required columns
        expected_columns = [col['name'] for col in self.schema['columns']]
        missing_columns = set(expected_columns) - set(df.columns)
        extra_columns = set(df.columns) - set(expected_columns)
        
        if missing_columns:
            validation_results['valid'] = False
            validation_results['errors'].append(f"Missing columns: {missing_columns}")
            
        if extra_columns:
            validation_results['warnings'].append(f"Extra columns found: {extra_columns}")
        
        # Validate each column
        for col_def in self.schema['columns']:
            col_name = col_def['name']
            
            if col_name not in df.columns:
                continue
                
            # Check nullable constraints
            if not col_def.get('nullable', True):
                null_count = df[col_name].isnull().sum()
                if null_count > 0:
                    validation_results['valid'] = False
                    validation_results['errors'].append(
                        f"Column '{col_name}' has {null_count} null values but is not nullable"
                    )
            
            # Validate data types and constraints
            self._validate_column_type(df, col_name, col_def, validation_results)
            self._validate_column_constraints(df, col_name, col_def, validation_results)
        
        return validation_results
    
    def _validate_column_type(self, df: pd.DataFrame, col_name: str, 
                             col_def: Dict[str, Any], results: Dict[str, Any]):
        """Validate column data type"""
        expected_type = col_def['type']
        series = df[col_name].dropna()  # Skip nulls for type checking
        
        if len(series) == 0:
            return
            
        if expected_type == 'integer':
            if not pd.api.types.is_integer_dtype(series):
                # Check if values can be converted to integer
                try:
                    pd.to_numeric(series, errors='raise')
                except (ValueError, TypeError):
                    results['valid'] = False
                    results['errors'].append(f"Column '{col_name}' contains non-integer values")
        
        elif expected_type == 'decimal':
            if not pd.api.types.is_numeric_dtype(series):
                try:
                    pd.to_numeric(series, errors='raise')
                except (ValueError, TypeError):
                    results['valid'] = False
                    results['errors'].append(f"Column '{col_name}' contains non-numeric values")
        
        elif expected_type == 'string':
            if not pd.api.types.is_string_dtype(series) and not pd.api.types.is_object_dtype(series):
                results['warnings'].append(f"Column '{col_name}' may not be string type")
        
        elif expected_type == 'date':
            # Try to parse as date
            try:
                pd.to_datetime(series, errors='raise')
            except (ValueError, TypeError):
                results['valid'] = False
                results['errors'].append(f"Column '{col_name}' contains invalid date values")
    
    def _validate_column_constraints(self, df: pd.DataFrame, col_name: str, 
                                   col_def: Dict[str, Any], results: Dict[str, Any]):
        """Validate column constraints like min/max values, patterns, etc."""
        series = df[col_name].dropna()
        
        if len(series) == 0:
            return
        
        # Check min/max values for numeric types
        if col_def['type'] in ['integer', 'decimal']:
            if 'min_value' in col_def:
                min_violations = (series < col_def['min_value']).sum()
                if min_violations > 0:
                    results['valid'] = False
                    results['errors'].append(
                        f"Column '{col_name}' has {min_violations} values below minimum {col_def['min_value']}"
                    )
            
            if 'max_value' in col_def:
                max_violations = (series > col_def['max_value']).sum()
                if max_violations > 0:
                    results['valid'] = False
                    results['errors'].append(
                        f"Column '{col_name}' has {max_violations} values above maximum {col_def['max_value']}"
                    )
        
        # Check string patterns
        if col_def['type'] == 'string' and 'pattern' in col_def:
            pattern = col_def['pattern']
            try:
                invalid_values = ~series.astype(str).str.match(pattern)
                invalid_count = invalid_values.sum()
                if invalid_count > 0:
                    results['valid'] = False
                    results['errors'].append(
                        f"Column '{col_name}' has {invalid_count} values not matching pattern '{pattern}'"
                    )
            except Exception as e:
                results['warnings'].append(f"Could not validate pattern for '{col_name}': {e}")
        
        # Check max length for strings
        if col_def['type'] == 'string' and 'max_length' in col_def:
            max_length = col_def['max_length']
            long_values = series.astype(str).str.len() > max_length
            long_count = long_values.sum()
            if long_count > 0:
                results['valid'] = False
                results['errors'].append(
                    f"Column '{col_name}' has {long_count} values exceeding max length {max_length}"
                )


def convert_column_types(df: pd.DataFrame, schema: Dict[str, Any]) -> pd.DataFrame:
    """Convert DataFrame columns to proper types based on schema"""
    df_converted = df.copy()
    
    for col_def in schema['columns']:
        col_name = col_def['name']
        col_type = col_def['type']
        
        if col_name not in df_converted.columns:
            continue
        
        try:
            if col_type == 'integer':
                df_converted[col_name] = pd.to_numeric(df_converted[col_name], errors='coerce')
                df_converted[col_name] = df_converted[col_name].astype('Int64')  # Nullable integer
            
            elif col_type == 'decimal':
                df_converted[col_name] = pd.to_numeric(df_converted[col_name], errors='coerce')
            
            elif col_type == 'date':
                df_converted[col_name] = pd.to_datetime(df_converted[col_name], errors='coerce')
            
            elif col_type == 'string':
                df_converted[col_name] = df_converted[col_name].astype('string')
                
        except Exception as e:
            print(f"Warning: Could not convert column '{col_name}' to type '{col_type}': {e}")
    
    return df_converted
