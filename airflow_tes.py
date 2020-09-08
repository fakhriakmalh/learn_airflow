import airflow 
from airflow import DAG 
from datetime import timedelta, datetime
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

default_arg = {
            'owner' : 'airflow',
            'depends_on_past' : False,
            'start_date' : datetime(2020,1,1), 
            "retry_delay": timedelta(minutes=5)
               }
# Config variables
#BQ_CONN_ID = "my_gcp_conn"
BQ_PROJECT = "bymyself-284004"
BQ_DATASET = "datawarehouse_covid19"    



with DAG(dag_id="airflow_local_test", schedule_interval="*/2 * * * *", default_args=default_arg, catchup=False) as dag:
        t1 = BigQueryOperator(
        task_id = 'bq_operator',
        sql = """SELECT * FROM `bymyself-284004.datawarehouse_covid19.covid19_italy_byprovince` LIMIT 1000 """,
        destination_dataset_table='{0}.{1}.datamart_covid_italy_byprovince'.format(
            BQ_PROJECT, BQ_DATASET),   
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id="my_gcp_conn2",
        dag=dag )

        t2 = BigQueryOperator(
        task_id = 'bq_operator2',
        sql = """ SELECT * FROM `bymyself-284004.datawarehouse_covid19.covid19_italy_byregion` LIMIT 1000 """ ,
        destination_dataset_table='{0}.{1}.datamart_covid_italy_byregion'.format(
            BQ_PROJECT, BQ_DATASET),   
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id="my_gcp_conn2",
        dag=dag)

        #set dependencies 
        t1 >> t2

        


        

        
#conn_id liat di settingan airflow nya / bigquery_default ???        
#destination_project_dataset_table (str) – The dotted (<project>.|<project>:)<dataset>.<table> BigQuery table to load data into. 
#If <project> is not included, project will be the project defined in the connection json. (templated)

