# Dataset Manifest — education_dialogue

## Dataset Name
education_dialogue

## Description
Educational dialogue dataset capturing teaching topics, learning preferences,
emotional reactions, and full teacher-student conversations stored as JSON.

## Local CSV Path
airflow-dags/extraction/education_dialogue/sample_data/

## Target Table
public.education_dialogue

## Notes
- Each row represents a single educational conversation
- The `conversation` column contains a JSON array serialized as text
- CSV files must conform to `config/schema_expected.yaml`
