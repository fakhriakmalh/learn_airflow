import airflow 
from airflow import DAG 
from datetime import timedelta, datetime
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

default_arg = {
            'owner' : 'airflow',
            'depends_on_past' : True,
            'start_date' : min(date)
               }


dag = DAG( 
        'airflow_covid19_italy',
        schedule_interval='None',
        default_args=default_arg )


# Config variables
#BQ_CONN_ID = "my_gcp_conn"
BQ_PROJECT = "bymyself-284004"
BQ_DATASET = "datawarehouse_covid19"        
        
        
        
t1 = BigQueryOperator(
        task_id = 'bq_operator',
        sql = 'select x1.* , (LAG(sum_per_day) OVER(PARTITION BY region_name ORDER BY date) - sum_per_day)* -1  as new_confirmed_cases from (SELECT date, region_name, sum(confirmed_cases) as sum_per_day FROM `bigquery-public-data.covid19_italy.data_by_province` group by date, region_name) x1',
        destination_dataset_table='{0}.{1}.datamart_covid_italy1'.format(
            BQ_PROJECT, BQ_DATASET),   
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id="bigquery_default",
        dag=dag)

t2 = BigQueryOperator(
        task_id = 'bq_operator2',
        sql = 'select * from `bigquery-public-data.covid19_italy.data_by_region` LIMIT 1000',
        destination_dataset_table='{0}.{1}.datamart_covid_italy2'.format(
            BQ_PROJECT, BQ_DATASET),   
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id="bigquery_default",
        dag=dag)

)
#set dependencies 
t1.set_downstream(t2)
        
#conn_id liat di settingan airflow nya / bigquery_default ???        
#destination_project_dataset_table (str) – The dotted (<project>.|<project>:)<dataset>.<table> BigQuery table to load data into. 
#If <project> is not included, project will be the project defined in the connection json. (templated)

