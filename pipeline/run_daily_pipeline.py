# This script runs a typical daily pipeline that ingests raw GDELT events files 
# for a single day (yesterday).


import datetime
import calendar
from ingest_raw_data import ingest_raw_data
from update_silver_layer import update_silver_layer
from update_gold_layer import update_gold_layer
from constants import SETTINGS

if __name__ == "__main__":

    # Get yesterday's date to use as the ingestion date.
    input_file_date = datetime.date.today() - datetime.timedelta(days=1)

    # Run the ingestion function to ingest raw GDELT events files for 
    # yesterday's date.
    ingest_raw_data(SETTINGS, input_file_date)

    # Run silver ingestion.
    update_silver_layer(SETTINGS, input_file_date)

    # Update SURI scores. Only run if the input_file_date is the last day of 
    # the month.
    days_in_month = calendar.monthrange(input_file_date.year, 
                                    input_file_date.month)[1]
    if input_file_date.day == days_in_month:
        update_gold_layer(SETTINGS, input_file_date)
    else:
        print(f"Skipping gold ingestion for {input_file_date} because it's not the first of the month.")