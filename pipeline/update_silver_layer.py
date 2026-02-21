# This script contains a function that builds the silver layer incrementally 
# from the bronze GDELT events table. It processes data for a single day, 
# applies cleaning and filtering logic, and overwrites only the corresponding 
# partition in the silver table.


import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

EXPECTED_SILVER_COLUMNS = [
    "GlobalEventID", "event_date",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode", 
    "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode",
    "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "EventCode", "EventBaseCode", "EventRootCode", "QuadClass",
    "GoldsteinScale", "NumMentions", "NumSources", "NumArticles", "AvgTone",
    "download_date", "ingested_at"
]

# 10: Demand, 13: Threaten, 14: Protest.
UNREST_CAMEOEVENT_CODES = ["10", "13", "14"]

# These columns can't have missing values. They are essential for computing 
# SURI scores.
ESSENTIAL_COLUMNS = ["event_date", "Actor1CountryCode", 
                     "Actor2CountryCode", "EventRootCode"]

def validate_silver(df: pyspark.sql.DataFrame, 
                    download_date: datetime.date) -> None:
    """
    This function performs basic data quality checks on the silver DataFrame.

    Args:
        df: The DataFrame to validate.

    Returns:
        Nothing. Raises an error if validation fails.
    """
    if df.columns != EXPECTED_SILVER_COLUMNS:
        raise ValueError(
            f"The actual silver events schema does not match the expected schema.\n"
            f"Expected: {EXPECTED_SILVER_COLUMNS}\n"
            f"Actual:   {df.columns}"
        )
    if df.count() == 0:
        raise ValueError(f"No silver data produced for {download_date}")
    if df.select("download_date").distinct().count() > 1:
        raise ValueError("Silver ingestion contains multiple download dates.")
    dupes = (
        df.groupBy("GlobalEventID")
        .count()
        .filter("count > 1")
        .limit(1)
        .count()
    )
    if dupes > 0:
        raise ValueError("Duplicate GlobalEventID values found in silver data")

def update_silver_layer(settings: dict[str, str], 
                        download_date: datetime.date) -> None:
    """
    Builds the silver layer incrementally for a single download_date.

    Args:
        settings: A dictionary containing various settings needed for the 
        silver layer update process, such as table names.
        download_date: The download_date to process (typically yesterday's date).

    Returns:
        Nothing.
    """
    print(f"Beginning silver update for the following date: {download_date}.")
    spark = SparkSession.builder.getOrCreate()

    # Read only the relevant bronze partition.
    bronze_df = (
        spark.table(settings["bronze_table_name"])
        .filter(col("download_date") == download_date)
    )
    silver_df = (
        bronze_df

        # Normalize event date.
        .withColumn(
            "event_date",
            to_date(col("Day").cast("string"), "yyyyMMdd")
        )

        # Filter to unrest-related events.
        .filter(col("EventRootCode").isin(UNREST_CAMEOEVENT_CODES))

        # Drop rows missing essential columns for SURI calculation.
        .dropna(subset=ESSENTIAL_COLUMNS)
    )

    # Perform data validation before writing to Delta table.
    validate_silver(silver_df, download_date)

    # Write to Delta table.
    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"ingestion_date = '{download_date}'")
        .saveAsTable(settings["silver_table_name"])
    )
    print("Silver ingestion is complete!")