# This script adds history to the events databases, bringing in data starting
# in January 2025. It uses the same functions defined in the daily pipeline, 
# but it runs them in a loop over all the dates in the backfill period.


import datetime
import calendar
from ingest_raw_data import ingest_raw_data
from update_silver_layer import update_silver_layer
from update_gold_layer import update_gold_layer
from constants import SETTINGS

if __name__ == "__main__":

    # Skip these dates because they have no data available on the GDELT site.
    # There was a GDELT outage in June 2025.
    skip_dates = [datetime.date(2025, 6, 15),
                  datetime.date(2025, 6, 16),
                  datetime.date(2025, 6, 17),
                  datetime.date(2025, 6, 18),
                  datetime.date(2025, 6, 19),
                  datetime.date(2025, 6, 20),
                  datetime.date(2025, 6, 21),
                  datetime.date(2025, 6, 22),
                  datetime.date(2025, 6, 23),
                  datetime.date(2025, 6, 24),
                  datetime.date(2025, 6, 25),
                  datetime.date(2025, 6, 26),
                  datetime.date(2025, 6, 27),
                  datetime.date(2025, 6, 28),
                  datetime.date(2025, 6, 29),
                  datetime.date(2025, 6, 30),
                  datetime.date(2025, 7, 1)]

    # Get all the dates starting from January 1, 2025 until yesterday's date.
    start_date = datetime.date(2025, 1, 1)
    end_date = datetime.date.today() - datetime.timedelta(days=1)
    date_range = (end_date - start_date).days
    all_input_file_dates = [start_date + datetime.timedelta(days=i) for i in range(date_range)]
    require_full_month = True
    for input_file_date in all_input_file_dates:
        days_in_month = calendar.monthrange(input_file_date.year, 
                                            input_file_date.month)[1]
        if input_file_date.day == 1:

            # Reset at the beginning of each month.
            require_full_month = True
        if input_file_date in skip_dates:
            require_full_month = False
            print(f"Skipping the following date because it's in skip_dates: {input_file_date}")
            if input_file_date.day == days_in_month:
                update_gold_layer(SETTINGS, input_file_date, require_full_month)
            continue
        
        # Run the ingestion function to ingest raw GDELT events files for 
        # the current date in the loop.
        ingest_raw_data(SETTINGS, input_file_date)

        # Run silver ingestion.
        update_silver_layer(SETTINGS, input_file_date)

        # Update SURI scores. Only run if the input_file_date is the last day of 
        # the month.
        if input_file_date.day == days_in_month:
            update_gold_layer(SETTINGS, input_file_date, require_full_month)
        else:
            print(f"Skipping gold ingestion for {input_file_date} because it's not the last day of the month.")