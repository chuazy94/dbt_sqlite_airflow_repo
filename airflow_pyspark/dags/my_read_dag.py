from airflow.decorators import task, dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag (
    schedule=None,
    catchup=False
)
def my_spark_submit_dag():
    read_data = SparkSubmitOperator(
        task_id = "read_data_operator",
        application = "./include/scripts/read.py",
        conn_id = "my_spark_conn",
        verbose = True
    )
    read_data

my_spark_submit_dag()