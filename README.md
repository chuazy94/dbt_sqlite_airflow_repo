# Overview
This project contains the pipeline to process Airbnb reviews and listings dataset based in London. 

# How the pipeline works
1. The pipeline firstly partitions data into year-month-day partitions
2. incrementally runs the NLTK VADER sentiment analysis on the reviews text.
3. Finally the dbt pipeline processes the data by running joins between the reviews and listings and generate fact style tables in a sqlite database

# Data
Data is available from https://insideairbnb.com/get-the-data/
