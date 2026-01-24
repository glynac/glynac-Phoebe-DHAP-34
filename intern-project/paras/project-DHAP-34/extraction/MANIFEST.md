# Dataset Manifest

## Dataset Information
- **Dataset Name**: customer_care_emails
- **Local CSV Folder Path**: `./sample_data/`
- **Target Table Name**: `public.customer_care_emails`

## Description
Customer care emails dataset from Hugging Face containing synthetically generated support emails for a middleware solutions company (Aetheros) with services including API development, monitoring, IAM, Mercury Language, and cloud management.

## Source
- **Origin**: Hugging Face Dataset (https://huggingface.co/datasets/rtweera/customer_care_emails)
- **Analysis Status**: Complete
- **Last Updated**: 2025-11-25
- **Size**: 2,259 records (complete dataset)
- **File Size**: 1.4MB
- **License**: GPL 3.0

## Files
- `customer_care_emails.csv` - Complete dataset file (all 2,259 records)
- Expected columns: subject, sender, receiver, timestamp, message_body, thread_id, email_types, email_status, email_criticality, product_types, agent_effectivity, agent_efficiency, customer_satisfaction

## Processing Notes
- Records with email_status='completed' should be skipped during processing (1,329 records)
- Records with email_status='ongoing' will be processed (930 records)
- Timestamp field needs proper datetime formatting
- Customer satisfaction scores range from -1 to +1
- Email types and product types are JSON arrays in the CSV
- Message body contains multi-line text which may span multiple CSV rows
