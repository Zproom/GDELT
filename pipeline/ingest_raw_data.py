# This script contains a function that ingest raw GDELT events files for a 
# single day, saves the source CSVs to DBFS, and appends the data to a bronze 
# Delta table. The bronze layer preserves raw source data with minimal 
# validation (e.g., file size and expected columns) and applies no 
# transformations. The table is partitioned by ingestion date.


import datetime
import requests
import zipfile
import os
import tempfile
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lit,
    current_timestamp
)

EXPECTED_GDELT_COLUMNS = [
    "GlobalEventID", "Day", "MonthYear", "Year", "FractionDate",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode", "Actor1EthnicCode",
    "Actor1Religion1Code", "Actor1Religion2Code", "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode", "Actor2EthnicCode",
    "Actor2Religion1Code", "Actor2Religion2Code", "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode", "QuadClass",
    "GoldsteinScale", "NumMentions", "NumSources", "NumArticles", "AvgTone",
    "Actor1Geo_Type", "Actor1Geo_Fullname", "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code",
    "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID",
    "Actor2Geo_Type", "Actor2Geo_Fullname", "Actor2Geo_CountryCode", "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code",
    "Actor2Geo_Lat", "Actor2Geo_Long", "Actor2Geo_FeatureID",
    "ActionGeo_Type", "ActionGeo_Fullname", "ActionGeo_CountryCode", "ActionGeo_ADM1Code", "ActionGeo_ADM2Code",
    "ActionGeo_Lat", "ActionGeo_Long", "ActionGeo_FeatureID",
    "DATEADDED", "SOURCEURL"
]

def get_gdelt_file_urls(settings: dict[str, str], 
                        download_date: datetime.date) -> list[str]:
    """
    This function generates all the expected GDELT events file URLs for a 
    given date.

    Args:
        download_date: The date for which to generate the file names 
        (typically, yesterday's date).
    
    Returns:
        A list of expected GDELT events file names for the given date.
    """
    gdelt_file_names = []
    for hour in range(24):
        for minute in range(0, 60, 15):
            ts = datetime.datetime(download_date.year, 
                          download_date.month, 
                          download_date.day, 
                          hour, 
                          minute).strftime("%Y%m%d%H%M%S")
            url = f"{settings["gdelt_url_prefix"]}{ts}.export.CSV.zip"
            gdelt_file_names.append(url)
    return gdelt_file_names

def validate_bronze(df):
    """
    Validates the schema and count of the bronze layer DataFrame.

    Args:
        df: The DataFrame to validate.

    Returns:
        Nothing. Raises an error if validation fails.
    """
    if df.columns[:len(EXPECTED_GDELT_COLUMNS)] != EXPECTED_GDELT_COLUMNS:
        raise ValueError("The actual GDELT events schema does not match the expected schema.")
    if df.count() == 0:
        raise ValueError("No rows ingested into Bronze layer.")

def ingest_raw_data(settings: dict[str, str], 
                    download_date: datetime.date) -> None:
    """
    This function ingests raw GDELT events files for a single day, saves the 
    source CSVs to DBFS, and appends the data to a bronze Delta table.

    Args:
        settings: A dictionary containing various settings needed for the 
        ingestion process, such as the table name and DBFS path.
        download_date: The date of the data being ingested (typically, 
        yesterday's date).

    Returns:
        Nothing.
    """
    print(f"Beginning bronze ingestion for the following date: {download_date}.")
    spark = SparkSession.builder.getOrCreate()
    gdelt_urls = get_gdelt_file_urls(settings, download_date)
    all_dfs = []
    for url in gdelt_urls:
        tmp_dir = tempfile.mkdtemp()
        zip_path = os.path.join(tmp_dir, "gdelt.zip")
        try:
            r = requests.get(url, timeout=30)
            if r.status_code != 200:
                print(f"Skipping missing file: {url}.")
                continue

            # Write .zip file locally.
            with open(zip_path, "wb") as f:
                f.write(r.content)

            # Extract the .csv.
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(tmp_dir)

            csv_files = [
                os.path.join(tmp_dir, f)
                for f in os.listdir(tmp_dir)
                if f.endswith(".CSV")
            ]

            if not csv_files:
                print(f"No .csv file found in: {url}. Skipping this file.")
                continue

            df = (
                spark.read
                .option("header", "false")
                .option("delimiter", "\t")
                .csv([f"file:{p}" for p in csv_files])
                .toDF(*EXPECTED_GDELT_COLUMNS)
            )

            df = (
                df
                .withColumn("ingestion_date", lit(download_date))
                .withColumn("ingested_at", current_timestamp())
                .withColumn("source_url", lit(url))
            )

            all_dfs.append(df)

        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    if not all_dfs:
        raise RuntimeError("No GDELT files were ingested.")

    final_df = all_dfs[0]
    for df in all_dfs[1:]:
        final_df = final_df.unionByName(df)

    validate_bronze(final_df)

    (
        final_df.write
        .format("delta")
        .mode("append")
        .partitionBy("ingestion_date")
        .saveAsTable(settings["bronze_table_name"])
    )
    print("Bronze ingestion is complete!")