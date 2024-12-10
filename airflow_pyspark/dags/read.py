# read.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
import os


def run_spark_job():
    # Print the current working directory to debug
    print("Current working directory:", os.getcwd())
    
    spark = SparkSession.builder \
        .appName("PySpark Example") \
        .getOrCreate()

    df = spark.read.csv("/usr/local/airflow/include/data.csv", header=True)
    df.show()

    spark.stop()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id="pyspark_read_csv",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    run_job = PythonOperator(
        task_id="run_spark_job",
        python_callable=run_spark_job,
    )