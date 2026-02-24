# This script contains a function that builds the silver layer incrementally 
# from the bronze GDELT events table. It processes data for a single day, 
# applies cleaning and filtering logic, and overwrites only the corresponding 
# partition in the silver table.


import datetime
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date

EXPECTED_SILVER_COLUMNS = [
    "GlobalEventID", "event_date",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode", 
    "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode",
    "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "EventCode", "EventBaseCode", "EventRootCode", "QuadClass",
    "GoldsteinScale", "NumMentions", "NumSources", "NumArticles", "AvgTone",
    "input_file_date", "ingested_at"
]

# Only store data for the following countries, which dominate international 
# news coverage and drive geopolitical risk. In plain English, the country 
# names are:
# USA, China, Russia, UK, France, Germany,
# Ukraine, Israel, Palestine, Iran, Syria,
# India, Brazil, Mexico, Turkey,
# Democratic Republic of the Congo, Rwanda, Ethiopia, Eritrea, Eritrea, 
# Pakistan, Egypt, Venezuela, Argentina, North Korea, Taiwan, South Korea
FOCUS_CAMEOCOUNTRY_CODES = [
    "USA", "CHN", "RUS", "GBR", "FRA", "DEU",
    "UKR", "ISR", "PSE", "IRN", "SYR",
    "IND", "BRA", "MEX", "TUR",
    "COD", "RWA", "ETH", "ERI",
    "PAK", "EGY", "VEN", "ARG", "PRK", "TWN", "KOR"
]

# These columns can't have missing values. They are essential for computing 
# SURI scores. Actor1CountryCode is allowed to be missing because domestic
# protestors and other non-state actors may not have a country code.
ESSENTIAL_COLUMNS = ["event_date", "Actor1CountryCode",
                     "Actor2CountryCode", "EventRootCode"]

def validate_silver(df: DataFrame, 
                    input_file_date: datetime.date) -> None:
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
        raise ValueError("No rows ingested into Silver layer.")
    if df.select("input_file_date").distinct().count() > 1:
        raise ValueError("Silver ingestion contains multiple input file dates.")
    dupes = (
        df.groupBy("GlobalEventID")
        .count()
        .filter("count > 1")
        .limit(1)
        .count()
    )
    if dupes > 0:
        raise ValueError("Duplicate GlobalEventID values found in silver data.")

def update_silver_layer(settings: dict[str, str], 
                        input_file_date: datetime.date) -> None:
    """
    This function builds the silver layer incrementally for a single 
    input_file_date.

    Args:
        settings: A dictionary containing various settings needed for the 
        silver layer update process, such as table names.
        input_file_date: The date of the data being ingested (typically, 
        yesterday's date).

    Returns:
        Nothing.
    """
    print(f"Beginning silver ingestion for the following date: {input_file_date}.")
    spark = SparkSession.builder.getOrCreate()

    # Read only the relevant bronze partition.
    bronze_df = (
        spark.table(settings["bronze_table_name"])
        .filter(col("input_file_date") == input_file_date)
    )
    silver_df = (
        bronze_df

        # Convert the event date column to a proper date type.
        .withColumn(
            "event_date",
            to_date(col("Day").cast("string"), "yyyyMMdd")
        )
        .select(*EXPECTED_SILVER_COLUMNS)

        # Filter the data to focus countries. Actor2CountryCode must be a focus 
        # country, but Actor1CountryCode doesn't have to be (can be missing), 
        # so non-state actors like domestic protestors are included.
        .filter(
            col("Actor1CountryCode").isin(FOCUS_CAMEOCOUNTRY_CODES) &
            col("Actor2CountryCode").isin(FOCUS_CAMEOCOUNTRY_CODES)
        )

        # Drop rows missing essential columns for the SURI calculation.
        .dropna(subset=ESSENTIAL_COLUMNS)

        # Drop duplicate rows.
        .dropDuplicates(["GlobalEventID"])
    )

    # Perform data validation before writing to Delta table.
    validate_silver(silver_df, input_file_date)

    # Write to Delta table.
    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("event_date")
        .saveAsTable(settings["silver_table_name"])
    )

    # It's expected queries will primarily use the event_date, 
    # Actor1CountryCode, and Actor2CountryCode columns, so these should be 
    # optimized for retrieval.
    spark.sql(f"""
        OPTIMIZE {settings["silver_table_name"]}
        ZORDER BY (Actor1CountryCode, Actor2CountryCode)
    """)
    print("Silver ingestion is complete!")