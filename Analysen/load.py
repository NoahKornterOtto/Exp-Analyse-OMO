# Importiere die benötigten Bibliotheken
from google.cloud import bigquery
import pandas as pd

###########################################################################
# Tabellen aus GCP BQ:                                                               #                                        
# - artikelhierarchie: brain-soso-dev.basisdaten.artikelhierarchie        #    
# - item-cluster: brain-soso-dev.basisdaten.item_cluster                  #
# - clickstream: brain-clickstream-prd.datalake_v1_ov_lhotse_clickstream  #
###########################################################################


# Lege die Google Cloud Projekt-ID explizit fest
project_id = 'brain-soso-dev'

# Initialisiere den BigQuery-Client mit der angegebenen Projekt-ID
client = bigquery.Client(project=project_id)

def fetch_data_to_pkl(dataset_id, table_id, filename):
    # SQL-Abfrage nach season und limitiere die Ergebnisse 
    query = f"""
  SELECT
   ITEM_H_SK, 
   IS_DEALBUY, 
   IS_NOS, 
   CLUSTER_DIM1,
   CLUSTER_DIM2
FROM `{dataset_id}.{table_id}`
WHERE GESCHAEFTSJAHR_HALBJAHR = 202401
    """

    # Führe die Abfrage aus und konvertiere die Ergebnisse in ein pandas DataFrame
    query_job = client.query(query)  # API-Anfrage
    df = query_job.to_dataframe()

    # Speichere das DataFrame in einer lokalen Pickle-Datei
    df.to_pickle(filename)

# Beispiel für die Verwendung
dataset_id = 'basisdaten'  # Dataset-ID
table_id = 'item_cluster'  # Tabellen-ID
filename = './data/item_cluster.pkl'  # Gewünschter Pfad der Ausgabe-CSV-Datei
fetch_data_to_pkl(dataset_id, table_id, filename)
