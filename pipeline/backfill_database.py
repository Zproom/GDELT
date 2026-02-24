# This script adds history to the events databases, bringing in data starting
# in January 2026. It uses the same functions defined in the daily pipeline, 
# but it runs them in a loop over all the dates in the backfill period.


import datetime
from ingest_raw_data import ingest_raw_data
from update_silver_layer import update_silver_layer
from constants import SETTINGS

if __name__ == "__main__":

    # Get all the dates starting from January 1, 2026 until yesterday's date.
    start_date = datetime.date(2026, 1, 1)
    end_date = datetime.date.today() - datetime.timedelta(days=1)
    date_range = (end_date - start_date).days
    all_input_file_dates = [start_date + datetime.timedelta(days=i) for i in range(date_range)]
    print(all_input_file_dates)
    for input_file_date in all_input_file_dates:

        # Run the ingestion function to ingest raw GDELT events files for 
        # the current date in the loop.
        ingest_raw_data(SETTINGS, input_file_date)

        # Run silver ingestion.
        update_silver_layer(SETTINGS, input_file_date)