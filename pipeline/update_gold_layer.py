# This script contains a function that builds the gold layer incrementally 
# from the silver GDELT events table. It processes data for a single day, 
# computes SURI scores, and overwrites only the corresponding partition in the 
# gold table.


import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
    
    # Extract table names from settings
    silver_table = settings.get("silver_table", "gdelt_project.silver.events")
    gold_table = settings.get("gold_table", "gdelt_project.gold.suri")
    
    # Calculate year_month for partitioning
    year_month = download_date.replace(day=1)
    
    # Read silver events for the specified download_date
    df = spark.table(silver_table).filter(
        F.col("event_date") == download_date
    )
    
    # Calculate SURI scores and aggregate metrics
    suri_df = df.groupBy(
        F.trunc(F.col("event_date"), "month").alias("year_month"),
        F.col("Actor1CountryCode").alias("source_actor"),
        F.col("Actor2CountryCode").alias("target_actor")
    ).agg(
        # Total events
        F.count("*").alias("total_events"),
        
        # Geopolitical unrest score: count of unrest events (codes 10, 13, 14)
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
    
    # Calculate political involvement score and SURI score
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
    
    # Select columns in the correct order matching the gold table schema
    final_df = suri_df.select(
        "year_month",
        "source_actor",
        "target_actor",
        "total_events",
        "geo_unrest_score",
        "total_gov_gov_events",
        "total_ngov_gov_events",
        "total_gov_ngov_events",
        "pol_involve_score",
        "suri_score",
        "avg_goldstein",
        "total_mentions",
        "total_sources",
        "total_articles",
        "avg_tone",
        "ingested_at"
    )
    
    # Write to gold table, overwriting only the partition for this year_month
    final_df.write.mode("overwrite").format("delta").partitionBy(
        "year_month"
    ).option(
        "replaceWhere", f"year_month = '{year_month}'"
    ).saveAsTable(gold_table)
    
    print(f"Gold layer updated for {download_date} (partition: {year_month})")
