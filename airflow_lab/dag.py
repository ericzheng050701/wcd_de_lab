from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.amazon.aws.transfers import mysql_to_s3
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from functions import exec_snowflake, download_snowtable, upload_to_s3
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from pytz import timezone
import os

SPARK_STEPS = [
    {
        'Name': 'wcd_data_engineer',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                '--master', 'yarn',
                's3://de-exercise-data-bucket/scripts/airflow_lab_pyspark.py', ## the S3 folder store the pyspark script.
                '--spark_name', 'airflow_lab',
                '--input_file_url', 's3://de-exercise-data-bucket/input/orders_amount.csv',  ## the S3 folder get the file from RDS and input file to EMR
                '--output_file_url', 's3://de-exercise-data-bucket/output/orders_amount_output', ## the S3 folder get the file from EMR.
                '--avg_order_amount', "{{ task_instance.xcom_pull('get_avg_order_amount', key='avg_order_amount') }}" ## the value of avg_order_amount.
            ]
        }
    }

]

CLUSTER_ID = "j-XOWZJMOIEDUE"
S3_BUCKET = os.environ.get("RDS_INPUT_S3_BUCKET", "de-exercise-data-bucket")
S3_KEY = os.environ.get("RDS_INPUT_S3_KEY", "input/orders_amount.csv")



default_args={'depends_on_past': False,
             'email': ['airflow@example.com'],
             'email_on_failure': False,
             'email_on_retry': False,
             'retries': 1, 
             'retry_delay': timedelta(minutes=2),
             'schedule_interval':'0 0 * * *',
             'start_date':datetime(2022, 1, 1),
             'catchup':False,
             'backfill':False}

sql_orderAmount = """
                     select o.orderNumber, o.order_date, sum(od.quantityOrdered*od.PriceEach) order_amount 
                     from classicmodels.orders o
                     left join classicmodels.orderdetails od Using (orderNumber)
                     where o.order_date<=current_date()-1
                     group by 1,2
                     order by 2 desc
                     ;
                """

sql_avgOrderAmount = """
                     select avg(order_amount) avg_order_amount 
                     from 
	                     (select o.orderNumber, o.order_date, sum(od.quantityOrdered*od.PriceEach) order_amount 
	                      from classicmodels.orders o
	                      left join classicmodels.orderdetails od Using (orderNumber)
	                      where o.order_date<=current_date()-1
	                      group by 1,2
	                      order by 2 desc) as t;                               
                      """


copyTABLE = """
            truncate airflow_demo.emr.orders;
            copy into airflow_demo.emr.orders
            from (SELECT $2,$3,$4,$5 FROM @S3_de_exercise_data_bucket_STAGE/output/orders_amount_output/)
            file_format = CSV_COMMA
            pattern='.*part.*[.]csv';
            """

def query_mysql(**kwargs):
    mysql_hook = MySqlHook(mysql_conn_id='mysql_rds_ariflowlab')
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql_avgOrderAmount)
    result = cursor.fetchone()
    data = float(result[0])
    kwargs['ti'].xcom_push(key='avg_order_amount', value=data)
    

with DAG('b4_air_lab_demo', description = 'airflow-lab-dag-demo', default_args = default_args) as dag:
    t0 = DummyOperator(task_id='start')

    t1 = SqlToS3Operator(
        task_id="orderAmount_mysql_to_s3",
        sql_conn_id="mysql_rds_ariflowlab",
        query=sql_orderAmount,
        s3_bucket=S3_BUCKET,
        aws_conn_id="aws_conn",
        s3_key=S3_KEY,
        replace=True,

    )

    t2 = PythonOperator (task_id='get_avg_order_amount', provide_context = True, python_callable=query_mysql)

    t3 = EmrAddStepsOperator(
    task_id = 'add_emr_steps',
    job_flow_id = CLUSTER_ID,
    aws_conn_id = "aws_conn",
    steps = SPARK_STEPS
    )

    t4 = EmrStepSensor(
        task_id = 'run_emr_steps',
        job_flow_id = CLUSTER_ID,
        step_id = "{{ task_instance.xcom_pull('add_emr_steps', key='return_value')[0] }}",
        aws_conn_id = "aws_conn"
    )

    t5 = SnowflakeOperator(
        task_id='s3_to_snowflake', 
        sql=copyTABLE,
        database='AIRFLOW_DEMO',
        schema='EMR',
        snowflake_conn_id='snowflake_conn'
        )                    


    t0>>t1
    t0>>t2
    t1>>t3>>t4>>t5
    t2>>t3>>t4>>t5
    

