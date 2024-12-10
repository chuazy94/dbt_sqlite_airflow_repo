import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import datetime
from utils.sentiment import analyze_sentiment
import sqlite3

INPUT_FOLDER = "/usr/local/airflow/include/data/input/"
OUTPUT_FOLDER = "/usr/local/airflow/include/data/output/"
DB_PATH = "/usr/local/airflow/include/practice.db"

@task
def load_csv_files(folder):
    """Load all CSV files from a folder into a list of DataFrames."""
    csv_files = []
    for dirpath, dirnames, filenames in os.walk(INPUT_FOLDER):
        for file in filenames:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(dirpath, file))

    print(f"Found {len(csv_files)} files to process!")

    return csv_files

@task
def process_sentiment(csv_file, output_folder):
    """Process a single CSV file for sentiment analysis."""
    df = pd.read_csv(csv_file)
    if 'comments' not in df.columns:
        raise ValueError(f"'comments' column not found in {csv_file}")

    sentiment_results = df['comments'].apply(analyze_sentiment)
    sentiment_df = sentiment_results.apply(pd.Series)

    # concat the sentiment_df and original df
    df = pd.concat([df, sentiment_df], axis=1)
    output_file = os.path.join(output_folder, os.path.basename(csv_file))
    df.to_csv(output_file, index=False)
    return output_file

@task
def load_to_sqlite(output_folder, db_path):
    """Load processed CSV files into SQLite database."""
    conn = sqlite3.connect(db_path)
    for file in os.listdir(output_folder):
        if file.endswith('.csv'):
            df = pd.read_csv(os.path.join(output_folder, file))
            df.to_sql("reviews_with_sentiment_analysis", conn, if_exists="append", index=False)
    conn.close()

@dag(
    dag_id="sentiment_analysis",
    default_args={"start_date": datetime(2023, 1, 1)},
    schedule_interval=None)
def sentiment_analysis_dag():
     # Load CSV files
    csv_files = load_csv_files(INPUT_FOLDER)
    
    # Process sentiment analysis for each CSV file in parallel using expand
    output_files = process_sentiment.expand(csv_file=csv_files, output_folder=[OUTPUT_FOLDER])

    load_to_sqlite(output_folder=OUTPUT_FOLDER, db_path=DB_PATH).set_upstream(output_files)


sentiment_analysis_dag()
