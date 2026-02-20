# This script runs a typical daily pipeline that ingests raw GDELT events files 
# for a single day (yesterday).


import datetime
from ingest_raw_data import *

if __name__ == "__main__":

    # Define the parameters for the pipeline, such as the table name and DBFS 
    # path.
    settings = {
        "bronze_table_name": "gdelt_project.bronze.events",
        "gdelt_url_prefix": "http://data.gdeltproject.org/gdeltv2/"
    }

    # Get yesterday's date to use as the ingestion date.
    download_date = datetime.date.today() - datetime.timedelta(days=1)

    # Run the ingestion function to ingest raw GDELT events files for 
    # yesterday's date.
    ingest_raw_data(settings, download_date)