# This script contains a function that ingest raw GDELT events files for a 
# single day, saves the source CSVs to DBFS, and appends the data to a bronze 
# Delta table. The bronze layer preserves raw source data with minimal 
# validation (e.g., file size and expected columns) and applies no 
# transformations. The table is partitioned by ingestion date.


import datetime
import requests
import zipfile
import io
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lit,
    current_timestamp,
    input_file_name
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

def download_and_extract_to_dbfs(settings: dict[str, str], url: str) -> None:
    """
    This function downloads a GDELT events file from the given URL, extracts 
    the CSV file from the ZIP archive, and saves it to DBFS.

    Args:
        url: The URL of the GDELT events file to download.
        dbfs_path: The DBFS path where the extracted CSV file should be saved.
    
    Returns:
        Nothing.
    """
    response = requests.get(url, timeout=30)

    if response.status_code != 200:
        print(f"Skipping missing file: {url}")
        return

    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        for name in z.namelist():
            csv_bytes = z.read(name)
            output_path = f"{settings["raw_data_path"]}{name}"

            with open(output_path.replace("dbfs:/", "/dbfs/"), "wb") as f:
                f.write(csv_bytes)

def validate_bronze(df):
    """
    Validates the schema and count of the bronze layer DataFrame.

    Args:
        df: The DataFrame to validate.

    Returns:
        Nothing. Raises an error if validation fails.
    """
    if df.columns[:len(EXPECTED_GDELT_COLUMNS)] != EXPECTED_GDELT_COLUMNS:
        raise ValueError("Unexpected GDELT Events schema")
    if df.count() == 0:
        raise ValueError("No rows ingested into Bronze layer")

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
    spark = SparkSession.builder.getOrCreate()
    gdelt_urls = get_gdelt_file_urls(settings, download_date)

    # Download and extract the file to DBFS.
    for url in gdelt_urls:
        download_and_extract_to_dbfs(settings, url)
    df = (
        spark.read
        .option("header", "false")
        .option("delimiter", "\t")
        .csv(settings["raw_data_path"])
    )
    df = df.toDF(*EXPECTED_GDELT_COLUMNS)

    # Perform data validation before writing to the Bronze table.
    validate_bronze(df)

    # Add ingestion metadata.
    df = (
        df
        .withColumn("ingestion_date", lit(download_date))
        .withColumn("ingested_at", current_timestamp())
        .withColumn("source_file", input_file_name())
    )

    # Write Bronze Delta table.
    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("ingestion_date")
        .saveAsTable(settings["bronze_table_name"])
    )