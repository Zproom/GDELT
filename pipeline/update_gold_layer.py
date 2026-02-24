# This script contains a function that builds the gold layer incrementally 
# from the silver GDELT events table. It processes data for a single day, 
# computes SURI scores, and overwrites only the corresponding partition in the 
# gold table.


import datetime
import calendar
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# 10: Demand, 13: Threaten, 14: Protest
UNREST_CAMEOEVENT_CODES = ["10", "13", "14"]

EXPECTED_GOLD_COLUMNS = [
    "year_month", "Actor1CountryCode", "Actor2CountryCode",
    "total_events", "geo_unrest_score",
    "total_gov_gov_events", "total_ngov_gov_events", "total_gov_ngov_events",
    "pol_involve_score", "suri_score",
    "avg_goldstein", "total_mentions", "total_sources", "total_articles", "avg_tone",
    "ingested_at"
]

def check_full_month_available(spark: SparkSession,
                                silver_table_name: str,
                                year_month: datetime.date) -> bool:
    """
    This function checks if a full month of data is available in the silver 
    table for the given year_month.

    Args:
        spark: The SparkSession object.
        silver_table_name: The name of the silver table.
        year_month: The year_month to check (should have day=1).

    Returns:
        True if a full month of data is available, False otherwise.
    """
    # Calculate the number of days in the month
    days_in_month = calendar.monthrange(year_month.year, year_month.month)[1]
    
    # Calculate the last day of the month
    last_day_of_month = year_month.replace(day=days_in_month)
    
    # Get distinct event_dates in the silver table for this month
    available_dates = (
        spark.table(silver_table_name)
        .filter(F.trunc(F.col("event_date"), "month") == year_month)
        .select("event_date")
        .distinct()
        .collect()
    )
    
    # Extract the dates into a set
    available_date_set = {row.event_date for row in available_dates}
    
    # Check if we have all days of the month
    expected_dates = {
        year_month + datetime.timedelta(days=i) 
        for i in range(days_in_month)
    }
    
    missing_dates = expected_dates - available_date_set
    
    if missing_dates:
        print(f"Missing {len(missing_dates)} day(s) of data for {year_month.strftime('%Y-%m')}:")
        print(f"  Missing dates: {sorted(missing_dates)[:5]}{'...' if len(missing_dates) > 5 else ''}")
        return False
    
    print(f"Full month of data available for {year_month.strftime('%Y-%m')} ({days_in_month} days).")
    return True

def validate_gold(df: DataFrame, 
                  year_month: datetime.date) -> None:
    """
    This function performs data quality checks on the gold DataFrame.

    Args:
        df: The DataFrame to validate.
        year_month: The year_month partition being validated.

    Returns:
        Nothing. Raises an error if validation fails.
    """
    if df.columns != EXPECTED_GOLD_COLUMNS:
        raise ValueError(
            f"The actual gold SURI schema does not match the expected schema.\n"
            f"Expected: {EXPECTED_GOLD_COLUMNS}\n"
            f"Actual:   {df.columns}"
        )
    if df.count() == 0:
        raise ValueError("No rows ingested into Gold layer.")
    if df.select("year_month").distinct().count() > 1:
        raise ValueError("Gold ingestion contains multiple year_month partitions.")
    if df.filter(F.col("total_events") <= 0).count() > 0:
        raise ValueError("Found rows with total_events <= 0.")
    if df.filter(F.col("geo_unrest_score") < 0).count() > 0:
        raise ValueError("Found rows with negative geo_unrest_score.")
    if df.filter((F.col("pol_involve_score") < 0) | (F.col("pol_involve_score") > 1)).count() > 0:
        raise ValueError("Found rows with pol_involve_score outside [0, 1] range.")
    if df.filter(F.col("suri_score") < 0).count() > 0:
        raise ValueError("Found rows with negative suri_score.")
    
    # Validate SURI calculation: suri_score = geo_unrest_score * pol_involve_score.
    suri_check = df.withColumn(
        "suri_check",
        F.abs(F.col("suri_score") - (F.col("geo_unrest_score") * F.col("pol_involve_score")))
    ).filter(F.col("suri_check") > 0.01)  # Allow small floating point errors
    if suri_check.count() > 0:
        raise ValueError("SURI score calculation mismatch detected.")
    
def update_gold_layer(settings: dict[str, str], 
                      input_file_date: datetime.date,
                      require_full_month: bool = True) -> None:
    """
    This function builds the gold layer incrementally for a single day.

    Args:
        settings: A dictionary containing various settings needed for the 
        gold layer update process, such as table names.
        input_file_date: The date of the data being ingested (typically, 
        yesterday's date).
        require_full_month: If True, only process if a full month of data 
        is available in silver. Default is True.

    Returns:
        Nothing.
    """
    print(f"Beginning gold ingestion for the following date: {input_file_date}.")
    spark = SparkSession.builder.getOrCreate()
    
    # Calculate year_month for partitioning.
    year_month = input_file_date.replace(day=1)
    
    # Check if full month of data is available
    if require_full_month:
        if not check_full_month_available(spark, settings["silver_table_name"], year_month):
            print(f"Skipping gold ingestion: Full month of data not yet available for {year_month.strftime('%Y-%m')}.")
            return
    
    # Read silver events for the entire month (to recalculate monthly aggregates).
    silver_df = spark.table(settings["silver_table_name"]).filter(
        F.trunc(F.col("event_date"), "month") == year_month
    )
    
    # Calculate SURI scores and aggregate metrics.
    suri_df = silver_df.groupBy(
        F.trunc(F.col("event_date"), "month").alias("year_month"),
        F.col("Actor1CountryCode"),
        F.col("Actor2CountryCode")
    ).agg(
        F.count("*").alias("total_events"),
        
        # Geopolitical unrest score: count of unrest events (codes 10, 13, 14).
        F.sum(
            F.when(F.col("EventRootCode").isin(UNREST_CAMEOEVENT_CODES), 1)
            .otherwise(0)
        ).alias("geo_unrest_score"),
        
        # Government-to-government events
        F.sum(
            F.when(
                (F.col("Actor1Type1Code") == "GOV") & 
                (F.col("Actor2Type1Code") == "GOV"), 1
            ).otherwise(0)
        ).alias("total_gov_gov_events"),
        
        # Non-government to government events
        F.sum(
            F.when(
                (F.col("Actor1Type1Code") != "GOV") & 
                (F.col("Actor2Type1Code") == "GOV"), 1
            ).otherwise(0)
        ).alias("total_ngov_gov_events"),
        
        # Government to non-government events
        F.sum(
            F.when(
                (F.col("Actor1Type1Code") == "GOV") & 
                (F.col("Actor2Type1Code") != "GOV"), 1
            ).otherwise(0)
        ).alias("total_gov_ngov_events"),
        
        # Additional aggregate metrics
        F.avg("GoldsteinScale").alias("avg_goldstein"),
        F.sum("NumMentions").alias("total_mentions"),
        F.sum("NumSources").alias("total_sources"),
        F.sum("NumArticles").alias("total_articles"),
        F.avg("AvgTone").alias("avg_tone")
    )
    
    # Calculate political involvement score and SURI score.
    suri_df = suri_df.withColumn(
        "pol_involve_score",
        (F.col("total_gov_gov_events") + 
         F.col("total_ngov_gov_events") + 
         F.col("total_gov_ngov_events")) / F.col("total_events")
    ).withColumn(
        "suri_score",
        F.col("geo_unrest_score") * F.col("pol_involve_score")
    ).withColumn(
        "ingested_at",
        F.current_timestamp()
    )
    
    # Select columns in the correct order matching the gold table schema.
    final_df = suri_df.select(*EXPECTED_GOLD_COLUMNS)
    
    # Perform data validation before writing to Delta table.
    validate_gold(final_df, year_month)
    
    # Write to gold table, overwriting only the partition for this year_month
    (
        final_df.write
        .format("delta")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("year_month")
        .saveAsTable(settings["gold_table_name"])
    )
    print("Gold ingestion is complete!")
