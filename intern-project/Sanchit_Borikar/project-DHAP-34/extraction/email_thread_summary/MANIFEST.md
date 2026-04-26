Dataset Manifest - Email Thread Summary

Dataset Name

email_thread_summary

Description

This dataset contains email threads and their corresponding summaries. It is created by merging email_thread_details and email_thread_summary datasets on thread_id.

Source

Kaggle - Email Thread Summary Dataset

Local Path

extraction/email_thread_summary/sample_data/final_dataset.csv

Target Table

public.email_thread_summary

File Format

CSV

Notes

timestamp converted to proper datetime format
from and to fields renamed to avoid SQL conflicts
synthetic primary key (id) added
