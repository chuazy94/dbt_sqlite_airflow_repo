from airflow.decorators import task, dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession

@dag (
    schedule=None,
    catchup=False
)
def my_pyspark_task_dag():

    @task.pyspark(conn_id='my_spark_conn')
    def read_data(spark: SparkSession, sc: SparkContext) -> pd.DataFrame:
        df = spark.createDataFrame([
            ("2024-01-01","Laptop","Electronics",2,999.99,1999.98,"C001","North"),
            ("2024-01-01","Mouse","Electronics",5,29.99,149.95,"C002","South"),
            ("2024-01-02","Keyboard","Electronics",3,89.99,269.97,"C003","East")
        ],
        ["date","product","category","quantity","unit_price","total_sales","customer_id","region"])

        df.show()

        return df.toPandas()
    
    read_data()
     

my_pyspark_task_dag()