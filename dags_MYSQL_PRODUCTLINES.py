import datetime

from airflow import models
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.utils.dates import days_ago

#Table
tablaoriginal = "productlines"
bucket_path = 'gs://proyectolabde01-config' 
project_id = 'proyectolabde01' 
dataset = 'mac_dtlk_str_raw_prod'
table = tablaoriginal+'_raw'

gce_zone = 'us-west2-a' 
gce_region = 'us-west2' 

outputTable=project_id+":"+dataset+"."+table

#Query de ingesta
query_origen ='''select * from productlines'''


default_args = {
    "start_date": days_ago(1),
    "dataflow_default_options": {
        #"project": project_id,
        #"region": gce_region,
        "zone": gce_zone,
        "temp_location": bucket_path + "/temp_job1/",
        'bypassTempDirValidation': False,
        'numWorkers': 2,
        'tempLocation': 'gs://proyectolabde01-config/temp_job1',
        'network': 'default',
    },
}

with models.DAG(
    "MYSQL_PRODUCTLINES",
    default_args=default_args,
    schedule_interval='0 23 * * *',#datetime.timedelta(days=1),  
) as dag:

    start_template_job = DataflowTemplateOperator(
        task_id="comp-mysql-bq-pipeline-prod",
        job_name="comp-mysql-bq-pipeline-prod",
        template="gs://proyectolabde01-config/templates/templatemysqltobq.json",
        parameters={
        'connectionURL': 'jdbc:mysql://34.125.166.191:3306/classicmodels?zeroDateTimeBehavior=round',
        'driverClassName': 'com.mysql.cj.jdbc.Driver',
        'query': query_origen,
        'outputTable': outputTable,
        'bigQueryLoadingTemporaryDirectory': 'gs://proyectolabde01-config/temp_job1',
        'username': 'labfinalxs',
        'password': 'clase@patch'
        },
    )
