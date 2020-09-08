from airflow import DAG
from airflow.sensors.http_sensor import HttpSensor
from datetime import datetime, timedelta
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
import io
from io import BytesIO 
import pandas as pd 
from google.cloud import storage
import csv
import requests
import json

default_args = {
            "owner": "airflow",
            "start_date": datetime(2019, 12, 4),
            "depends_on_past": False,
            "email_on_failure": False,
            "email_on_retry": False,
            "email": "youremail@host.com",
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        }

# Download forex rates according to the currencies we want to watch
# described in the file forex_currencies.csv
def download_rates():
    with open('/usr/local/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for row in reader:
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get('https://api.exchangeratesapi.io/latest?base=' + base).json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]
            with open('/usr/local/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')
                
def upload_to_gcs() : 
    storage_client = storage.Client.from_service_account_json("/usr/local/airflow/dags/---------------------.json")
    #create bucket object 
    bucket = storage_client.get_bucket("data-main-stream")
    #create filename in bucket
    filename = "%s/%s" % ("forex2","forex_rates.json")
    blob = bucket.blob(filename)
    #open file in local
    with open('/usr/local/airflow/dags/files/forex_rates.json') as f : 
        blob.upload_from_file(f)

with DAG(dag_id="forex_data_pipeline_v_2", schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
    
    is_forex_rates_available = HttpSensor(
        task_id="is_forex_rates_available",
        method="GET",
        http_conn_id="forex_api",
        endpoint="latest",
        response_check=lambda response: "rates" in response.text,
        poke_interval=5,
        timeout=20
    )

    is_forex_currencies_file_available = FileSensor(
        task_id="is_forex_currencies_file_available",
        fs_conn_id="forex_path",
        filepath="forex_currencies.csv",
        poke_interval=5,
        timeout=20
    )

    # Parsing forex_pairs.csv and downloading the files
    downloading_rates = PythonOperator(
            task_id="downloading_rates",
            python_callable=download_rates
    )

    # Saving forex_rates.json in HDFS
    upload_gcs = PythonOperator(
        task_id="upload_to_gcs",
        python_callable = upload_to_gcs
    )
    
    # gcs_to_bigq = GoogleCloudStorageToBigQueryOperator (
    #     task_id = "GCS_to_bigquery" , 
    #     bucket = "data-main-stream ", 
    #     source_objects = ["gs://data-main-stream/forex_rates.json"], 
    #     source_format= 'NEWLINE_DELIMITED_JSON',
    #     schema_fields=None,
    #     schema_object='schema.json',
    #     destination_project_dataset_table = 'bymyself-284004:datawarehouse_covid19', 
    #     create_disposition='CREATE_IF_NEEDED', 
    #     write_disposition='WRITE_TRUNCATE', 
    #     bigquery_conn_id='my_gcp_conn2',
    #     google_cloud_storage_conn_id='my_gcp_conn2'
    #     )
    #https://stackoverflow.com/questions/52261904/airflow-googlecloudstoragetobigqueryoperator-does-not-render-templated-source
    #https://github.com/apache/airflow/pull/3763/files
    #https://groups.google.com/g/cloud-composer-discuss/c/Hx4QNng-mYc?pli=1

    #local to big query 
    #https://dev.to/jescudegodia/load-a-json-file-to-google-bigquery-using-python-2lj4


