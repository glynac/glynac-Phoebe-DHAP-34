# Dataset Manifest — Email Thread Summary (Code 100)

## Dataset

- dataset_code: 100
- dataset_name: email_thread_summaries
- source: https://www.kaggle.com/datasets/marawanxmamdouh/email-thread-summary-dataset
- file: sample_data/email_thread_summaries.csv
- rows: 4167
- columns:
  - thread_id (integer, primary key)
  - summary (text)

## Target

- postgres_schema: public
- postgres_table: email_thread_summaries

## Notes

- `thread_id` is unique and non-null.
- `summary` is non-null text.
- This folder intentionally ingests only `email_thread_summaries.csv` (the zip also contains `email_thread_details.csv` which can be added later as a second dataset if needed).
