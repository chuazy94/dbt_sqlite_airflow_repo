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
    """
        Creates a DataFrame in PySpark and returns it as a pandas DataFrame.
    """

    @task.pyspark(conn_id='my_spark_conn')
    def check_spark_context(spark: SparkSession) -> str:
        """
        Checks if the Spark context is running and logs the status.
        """
        sc = spark.sparkContext
        if sc and not sc._jsc.sc().isStopped():
            print("✅ SparkContext is running!")
            print(f"Master URL: {sc.master}")
            print(f"App Name: {sc.appName}")
            return "SparkContext is running"
        else:
            print("❌ SparkContext is not running!")
            return "SparkContext is not running"

    @task.pyspark(conn_id='my_spark_conn')
    def create_data(spark: SparkSession, sc: SparkContext) -> pd.DataFrame:
        df = spark.createDataFrame([
            ("2024-01-01","Laptop","Electronics",2,999.99,1999.98,"C001","North"),
            ("2024-01-01","Mouse","Electronics",5,29.99,149.95,"C002","South"),
            ("2024-01-02","Keyboard","Electronics",3,89.99,269.97,"C003","East")
        ],
        ["date","product","category","quantity","unit_price","total_sales","customer_id","region"])

        df.show()

        return df.toPandas()
    
    @task
    def log_data(df: pd.DataFrame):
        """
        Logs pandas DataFrame for demonstration purposes.
        """
        print("DataFrame content:")
        print(df)
    
    # Task orchestration
    spark_status = check_spark_context()
    pandas_df = create_data()
    log_data(pandas_df)
     

my_pyspark_task_dag()