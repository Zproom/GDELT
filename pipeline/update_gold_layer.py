# This script contains a function that builds the gold layer incrementally 
# from the silver GDELT events table. It processes data for a single day, 
# computes SURI scores, and overwrites only the corresponding partition in the 
# gold table.


import datetime
from pyspark.sql import SparkSession

# 10: Demand, 13: Threaten, 14: Protest.
UNREST_CAMEOEVENT_CODES = ["10", "13", "14"]

def update_gold_layer(settings: dict[str, str], 
                      download_date: datetime.date) -> None:
    """
    This function builds the gold layer incrementally for a single 
    download_date.

    Args:
        settings: A dictionary containing various settings needed for the 
        gold layer update process, such as table names.
        download_date: The date of the data being ingested (typically, 
        yesterday's date).

    Returns:
        Nothing.
    """
    spark = SparkSession.builder.getOrCreate()
    
