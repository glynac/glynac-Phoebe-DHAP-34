# Dataset Manifest

- **Dataset name:** customer_care_emails
- **Local CSV folder path (repo-relative):** airflow-dags/extraction/customer_care_emails/sample_data
- **Target table name (PostgreSQL):** public.customer_care_emails

## Notes
- Source: SharePoint (manual download).
- Expected CSV(s): `customer_care_emails.csv`
- Column rename: original `Unnamed: 0` → `id` (used as primary key).
