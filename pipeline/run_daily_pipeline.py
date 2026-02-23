# This script runs a typical daily pipeline that ingests raw GDELT events files 
# for a single day (yesterday).


import datetime
from ingest_raw_data import ingest_raw_data
from update_silver_layer import update_silver_layer
from constants import SETTINGS

if __name__ == "__main__":

    # Get yesterday's date to use as the ingestion date.
    input_file_date = datetime.date.today() - datetime.timedelta(days=1)

    # Run the ingestion function to ingest raw GDELT events files for 
    # yesterday's date.
    ingest_raw_data(SETTINGS, input_file_date)

    # Run silver ingestion.
    update_silver_layer(SETTINGS, input_file_date)