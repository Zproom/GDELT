# This script contains a function that ingests raw GDELT events files for a 
# single day, downloads them to a Unity Catalog Volume, and appends the data 
# to a bronze Delta table. The bronze layer preserves raw source data with 
# minimal validation (e.g., file size and expected columns) and applies no 
# transformations. The table is partitioned by download date.


import datetime
import requests
import os
import shutil
import zipfile
from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    lit,
    current_timestamp,
    col,
    expr
)
from pyspark.sql.types import (
    LongType,
    IntegerType,
    DoubleType
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
                        input_file_date: datetime.date) -> list[str]:
    """
    This function generates all the expected GDELT events file URLs for a 
    given date.

    Args:
        settings: A dictionary containing various settings needed for the 
        ingestion process, such as the table names.
        input_file_date: The date for which to generate the file names 
        (typically, yesterday's date).
    
    Returns:
        A list of expected GDELT events file URLs for the given date.
    """
    gdelt_file_urls = []
    for hour in range(24):
        for minute in range(0, 60, 15):
            ts = datetime.datetime(input_file_date.year, 
                          input_file_date.month, 
                          input_file_date.day, 
                          hour, 
                          minute).strftime("%Y%m%d%H%M%S")
            url = f"{settings['gdelt_url_prefix']}{ts}.export.CSV.zip"
            gdelt_file_urls.append(url)
    return gdelt_file_urls

def download_file(url: str, staging_path: str) -> tuple[str, str, bool]:
    """
    This function downloads a single GDELT file to the staging directory and 
    extracts it.
    
    Args:
        url: The URL to download from.
        staging_path: Path to save files to (Unity Catalog Volume path).
        
    Returns:
        Tuple of (url, csv_file_path, success).
    """
    try:
        filename = url.split('/')[-1]
        zip_path = f"{staging_path}/{filename}"
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            print(f"Skipping missing file: {url}")
            return (url, None, False)
        
        # Write zip file to Unity Catalog Volume.
        with open(zip_path, "wb") as f:
            f.write(r.content)
        
        # Extract the CSV file from the zip.
        with zipfile.ZipFile(zip_path, "r") as z:
            csv_files = [name for name in z.namelist() if name.endswith(".CSV")]
            if not csv_files:
                print(f"No CSV file found in {filename}")
                return (url, None, False)
            
            # Extract the first CSV file.
            z.extract(csv_files[0], staging_path)
            csv_path = f"{staging_path}/{csv_files[0]}"
        
        # Remove the zip file to save space.
        os.remove(zip_path)
        return (url, csv_path, True)
        
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return (url, None, False)

def validate_bronze(df: DataFrame) -> None:
    """
    This function validates the schema and count of the bronze layer DataFrame.

    Args:
        df: The DataFrame to validate.

    Returns:
        Nothing. Raises an error if validation fails.
    """
    if df.columns[:len(EXPECTED_GDELT_COLUMNS)] != EXPECTED_GDELT_COLUMNS:
        raise ValueError(
                f"The actual GDELT events schema does not match the expected schema.\n"
                f"Expected: {EXPECTED_GDELT_COLUMNS}\n"
                f"Actual:   {df.columns}"
            )
    if df.count() == 0:
        raise ValueError("No rows ingested into Bronze layer.")
    if df.select("input_file_date").distinct().count() > 1:
        raise ValueError("Bronze ingestion contains multiple input file dates.")

def ingest_raw_data(settings: dict[str, str], 
                    input_file_date: datetime.date) -> None:
    """
    This function ingests raw GDELT events files for a single day and appends 
    the data to a bronze Delta table.

    Args:
        settings: A dictionary containing various settings needed for the 
        ingestion process, such as the table names.
        input_file_date: The date of the data being ingested (typically, 
        yesterday's date).

    Returns:
        Nothing.
    """
    print(f"Beginning bronze ingestion for the following date: {input_file_date}.")
    spark = SparkSession.builder.getOrCreate()
    
    # Use Unity Catalog Volume for staging (accessible to all cluster nodes).
    staging_path = f"/Volumes/gdelt_project/bronze/staging_files/{input_file_date}"
    os.makedirs(staging_path, exist_ok=True)
    
    # Get all URLs for the download date.
    gdelt_urls = get_gdelt_file_urls(settings, input_file_date)
    
    # Download files in parallel.
    successful_downloads = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(download_file, url, staging_path) for url in gdelt_urls]
        for future in as_completed(futures):
            url, file_path, success = future.result()
            if success:
                successful_downloads.append((url, file_path))
    if not successful_downloads:
        raise RuntimeError("No GDELT files were successfully downloaded.")
    
    # Read all downloaded CSV files with Spark.
    file_paths = [path for _, path in successful_downloads]
    df = (
        spark.read
        .option("header", "false")
        .option("delimiter", "\t")
        .csv(file_paths)
        .toDF(*EXPECTED_GDELT_COLUMNS)
    )
    
    # Cast columns to correct types to match table schema.
    # Using try_cast to handle malformed data gracefully (converts to NULL).
    df = (
        df
        .withColumn("GlobalEventID", expr("try_cast(GlobalEventID as BIGINT)"))
        .withColumn("Day", expr("try_cast(Day as INT)"))
        .withColumn("MonthYear", expr("try_cast(MonthYear as INT)"))
        .withColumn("Year", expr("try_cast(Year as INT)"))
        .withColumn("FractionDate", expr("try_cast(FractionDate as DOUBLE)"))
        .withColumn("IsRootEvent", expr("try_cast(IsRootEvent as INT)"))
        .withColumn("QuadClass", expr("try_cast(QuadClass as INT)"))
        .withColumn("GoldsteinScale", expr("try_cast(GoldsteinScale as DOUBLE)"))
        .withColumn("NumMentions", expr("try_cast(NumMentions as INT)"))
        .withColumn("NumSources", expr("try_cast(NumSources as INT)"))
        .withColumn("NumArticles", expr("try_cast(NumArticles as INT)"))
        .withColumn("AvgTone", expr("try_cast(AvgTone as DOUBLE)"))
        .withColumn("Actor1Geo_Type", expr("try_cast(Actor1Geo_Type as INT)"))
        .withColumn("Actor1Geo_Lat", expr("try_cast(Actor1Geo_Lat as DOUBLE)"))
        .withColumn("Actor1Geo_Long", expr("try_cast(Actor1Geo_Long as DOUBLE)"))
        .withColumn("Actor2Geo_Type", expr("try_cast(Actor2Geo_Type as INT)"))
        .withColumn("Actor2Geo_Lat", expr("try_cast(Actor2Geo_Lat as DOUBLE)"))
        .withColumn("Actor2Geo_Long", expr("try_cast(Actor2Geo_Long as DOUBLE)"))
        .withColumn("ActionGeo_Type", expr("try_cast(ActionGeo_Type as INT)"))
        .withColumn("ActionGeo_Lat", expr("try_cast(ActionGeo_Lat as DOUBLE)"))
        .withColumn("ActionGeo_Long", expr("try_cast(ActionGeo_Long as DOUBLE)"))
    )
    
    # Add metadata columns.
    df = (
        df
        .withColumn("input_file_date", lit(input_file_date))
        .withColumn("ingested_at", current_timestamp())
    )

    # Perform data validation before writing to Delta table.
    validate_bronze(df)
    
    # Write to Delta table.
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("input_file_date")
        .saveAsTable(settings["bronze_table_name"])
    )
    
    # Clean up the staging directory.
    shutil.rmtree(staging_path, ignore_errors=True)
    print("Bronze ingestion is complete!")
