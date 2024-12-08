# -*- coding: utf-8 -*-
"""
Created on Sun Dec  8 00:23:07 2024

@author: Raphael
"""

import os
from google.cloud import bigquery

# Configure credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\{user}\{path}\{your_json}.json"

# Initialize BigQuery client
client = bigquery.Client()

# Source project
SOURCE_PROJECT_ID = "{source_project_id}"
SOURCE_DATASET_ID = "{source_dataset_id}"

# Destination project
DESTINATION_PROJECT_ID = "{destination_project_id}"
DESTINATION_DATASET_ID = "{destiantion_dataset_id}"
DESTINATION_DATASET_REGION = "{destination_location_name}"

# Function to check and create the dataset if it doesn't exist
def create_dataset_if_not_exists(project_id, dataset_id, region):
    dataset_ref = f"{project_id}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
        print(f"The dataset {dataset_ref} already exists.")
    except Exception:
        print(f"The dataset {dataset_ref} does not exist. Creating it...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = region
        client.create_dataset(dataset)
        print(f"Dataset {dataset_ref} created successfully.")

# Function to check if a table exists
def table_exists(project_id, dataset_id, table_id):
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    try:
        client.get_table(table_ref)
        return True
    except Exception:
        return False

# Function to copy a table
def copy_table(source_table_id, destination_table_id, current_index, total_count):
    if table_exists(DESTINATION_PROJECT_ID, DESTINATION_DATASET_ID, destination_table_id.split('.')[-1]):
        print(f"[{current_index} of {total_count}] Table {destination_table_id} already exists. Copy skipped.")
        return
    print(f"[{current_index} of {total_count}] Copying table {source_table_id} to {destination_table_id}...")
    copy_job = client.copy_table(source_table_id, destination_table_id)
    copy_job.result()  # Wait for the job to complete
    print(f"[{current_index} of {total_count}] Table successfully copied from {source_table_id} to {destination_table_id}")

# Function to list tables in the source dataset
def list_tables_in_dataset(project_id, dataset_id):
    dataset_ref = f"{project_id}.{dataset_id}"
    tables = client.list_tables(dataset_ref)
    table_names = [table.table_id for table in tables]
    return table_names

# Function to filter datasharded tables
def filter_datasharded_tables(table_names, prefix):
    return [table for table in table_names if table.startswith(prefix)]

# Main script
if __name__ == "__main__":
    # Create the destination dataset if it doesn't exist
    create_dataset_if_not_exists(DESTINATION_PROJECT_ID, DESTINATION_DATASET_ID, DESTINATION_DATASET_REGION)

    # List tables in the source dataset
    tables = list_tables_in_dataset(SOURCE_PROJECT_ID, SOURCE_DATASET_ID)
    
    # Filter tables with the datasharded prefix (e.g., "events_")
    prefix = "events_"  # Replace with the appropriate prefix for your tables
    datasharded_tables = filter_datasharded_tables(tables, prefix)
    
    # Total number of tables to copy
    total_tables = len(datasharded_tables)

    # Loop to copy each table
    for index, table_id in enumerate(datasharded_tables, start=1):
        source_table_id = f"{SOURCE_PROJECT_ID}.{SOURCE_DATASET_ID}.{table_id}"
        destination_table_id = f"{DESTINATION_PROJECT_ID}.{DESTINATION_DATASET_ID}.{table_id}"
        copy_table(source_table_id, destination_table_id, index, total_tables)
    
    print("All tables have been successfully copied.")
