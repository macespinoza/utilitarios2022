from googleapiclient.discovery import build
from google.cloud import bigquery

client = bigquery.Client();

tabla_bq = "personas";
table_id = "proyectolabde01.datalabde.personas";

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("nombre", "STRING"),
        bigquery.SchemaField("apellido", "STRING"),
        bigquery.SchemaField("edad", "INTEGER")
    ],
    skip_leading_rows=1,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.CSV,
)
job_config.field_delimiter = ";"
uri = "gs://proyectolabde01-csv/nombresejemplo.csv"
load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)