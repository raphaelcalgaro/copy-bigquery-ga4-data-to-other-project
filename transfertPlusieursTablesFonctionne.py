# -*- coding: utf-8 -*-
"""
Created on Sat Dec  7 14:30:54 2024

@author: Raphael
"""

import os
from google.cloud import bigquery

# Configurer les credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Raphael\testTransferDataBQ\gainr-263816-05fb329857a4.json"

# Initialiser le client BigQuery
client = bigquery.Client()

# Projet source
SOURCE_PROJECT_ID = "bq-test-367602"
SOURCE_DATASET_ID = "analytics_265176981"

# Projet destination
DESTINATION_PROJECT_ID = "gainr-263816"
DESTINATION_DATASET_ID = "testDataTransfer2"

# Fonction pour copier une table
def copy_table(source_table_id, destination_table_id, current_index, total_count):
    print(f"[{current_index} de {total_count}] Copie de la table {source_table_id} vers {destination_table_id}...")
    copy_job = client.copy_table(source_table_id, destination_table_id)
    copy_job.result()  # Attendre la fin du job
    print(f"[{current_index} de {total_count}] Table copiée avec succès de {source_table_id} vers {destination_table_id}")

# Lister les tables dans le dataset source
def list_tables_in_dataset(project_id, dataset_id):
    dataset_ref = f"{project_id}.{dataset_id}"
    tables = client.list_tables(dataset_ref)
    table_names = [table.table_id for table in tables]
    return table_names

# Filtrer les tables datasharded
def filter_datasharded_tables(table_names, prefix):
    return [table for table in table_names if table.startswith(prefix)]

# Script principal
if __name__ == "__main__":
    # Lister les tables dans le dataset source
    tables = list_tables_in_dataset(SOURCE_PROJECT_ID, SOURCE_DATASET_ID)
    
    # Filtrer les tables avec le préfixe datasharded (ex. : "events_")
    prefix = "events_"  # Remplacez par le préfixe correspondant à vos tables
    datasharded_tables = filter_datasharded_tables(tables, prefix)
    
    # Nombre total de tables à copier
    total_tables = len(datasharded_tables)

    # Boucle pour copier chaque table
    for index, table_id in enumerate(datasharded_tables, start=1):
        source_table_id = f"{SOURCE_PROJECT_ID}.{SOURCE_DATASET_ID}.{table_id}"
        destination_table_id = f"{DESTINATION_PROJECT_ID}.{DESTINATION_DATASET_ID}.{table_id}"
        copy_table(source_table_id, destination_table_id, index, total_tables)
    
    print("Toutes les tables ont été copiées avec succès.")