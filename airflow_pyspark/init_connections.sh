#!/bin/bash

# Connection ID to check
CONN_ID='my_spark_conn'

# Check if the connection already exists
if airflow connections list | grep -q "$CONN_ID"; then
    echo "Connection '$CONN_ID' already exists. Skipping creation."
else
    # Add the connections automatically
    airflow connections add "$CONN_ID" \
        --conn-type 'spark' \
        --conn-host 'spark://spark-master' \
        --conn-port 7077 

fi