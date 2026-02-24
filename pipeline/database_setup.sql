-- This file contains the initial database setup for the GDELT project. This 
-- should only need to be run once, but it's safe to run multiple times. The
-- script assumes you have already created a catalog called gdelt_project.


USE CATALOG gdelt_project;

-- Schemas
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Bronze layer: raw source data';

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Silver layer: cleaned and filtered data';

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Gold layer: analytics-ready data';

-- Tables
CREATE TABLE IF NOT EXISTS bronze.events (
    GlobalEventID         BIGINT,
    Day                   INT,
    MonthYear             INT,
    Year                  INT,
    FractionDate          DOUBLE,

    Actor1Code            STRING,
    Actor1Name            STRING,
    Actor1CountryCode     STRING,
    Actor1KnownGroupCode  STRING,
    Actor1EthnicCode      STRING,
    Actor1Religion1Code   STRING,
    Actor1Religion2Code   STRING,
    Actor1Type1Code       STRING,
    Actor1Type2Code       STRING,
    Actor1Type3Code       STRING,

    Actor2Code            STRING,
    Actor2Name            STRING,
    Actor2CountryCode     STRING,
    Actor2KnownGroupCode  STRING,
    Actor2EthnicCode      STRING,
    Actor2Religion1Code   STRING,
    Actor2Religion2Code   STRING,
    Actor2Type1Code       STRING,
    Actor2Type2Code       STRING,
    Actor2Type3Code       STRING,

    IsRootEvent           INT,
    EventCode             STRING,
    EventBaseCode         STRING,
    EventRootCode         STRING,
    QuadClass             INT,

    GoldsteinScale        DOUBLE,
    NumMentions           INT,
    NumSources            INT,
    NumArticles           INT,
    AvgTone               DOUBLE,

    Actor1Geo_Type        INT,
    Actor1Geo_Fullname    STRING,
    Actor1Geo_CountryCode STRING,
    Actor1Geo_ADM1Code    STRING,
    Actor1Geo_ADM2Code    STRING,
    Actor1Geo_Lat         DOUBLE,
    Actor1Geo_Long        DOUBLE,
    Actor1Geo_FeatureID   STRING,

    Actor2Geo_Type        INT,
    Actor2Geo_Fullname    STRING,
    Actor2Geo_CountryCode STRING,
    Actor2Geo_ADM1Code    STRING,
    Actor2Geo_ADM2Code    STRING,
    Actor2Geo_Lat         DOUBLE,
    Actor2Geo_Long        DOUBLE,
    Actor2Geo_FeatureID   STRING,

    ActionGeo_Type        INT,
    ActionGeo_Fullname    STRING,
    ActionGeo_CountryCode STRING,
    ActionGeo_ADM1Code    STRING,
    ActionGeo_ADM2Code    STRING,
    ActionGeo_Lat         DOUBLE,
    ActionGeo_Long        DOUBLE,
    ActionGeo_FeatureID   STRING,

    DATEADDED             STRING,
    SOURCEURL             STRING,

    -- Ingestion metadata
    input_file_date       DATE COMMENT 'The date of the GDELT events file \
    this observation came from. This date is derived from the filename of the \
    ingested file.',
    ingested_at           TIMESTAMP COMMENT 'The timestamp when the data was \
    ingested into the database.'
)
USING DELTA
PARTITIONED BY (input_file_date)
COMMENT 'Raw GDELT events data. See the GDELT documentation for descriptions \ 
about columns included in the input data. These columns are in all caps. \
Derived columns are in snake_case.';

CREATE TABLE IF NOT EXISTS silver.events (
    GlobalEventID         BIGINT,
    event_date            DATE COMMENT 'The date of the event. This is \
    derived from the Day column in the raw data.',

    Actor1Code            STRING,
    Actor1Name            STRING,
    Actor1CountryCode     STRING,
    Actor1KnownGroupCode  STRING,
    Actor1Type1Code       STRING,
    Actor1Type2Code       STRING,
    Actor1Type3Code       STRING,

    Actor2Code            STRING,
    Actor2Name            STRING,
    Actor2CountryCode     STRING,
    Actor2KnownGroupCode  STRING,
    Actor2Type1Code       STRING,
    Actor2Type2Code       STRING,
    Actor2Type3Code       STRING,

    EventCode             STRING,
    EventBaseCode         STRING,
    EventRootCode         STRING,
    QuadClass             INT,

    GoldsteinScale        DOUBLE,
    NumMentions           INT,
    NumSources            INT,
    NumArticles           INT,
    AvgTone               DOUBLE,

    input_file_date       DATE COMMENT 'The date of the GDELT events file \
    this observation came from. This date is derived from the filename of the \
    ingested file.',
    ingested_at           TIMESTAMP COMMENT 'The timestamp when the data was \
    ingested into the database.'
)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'Cleaned and filtered GDELT events data. See the GDELT documentation \
for descriptions about columns included in the input data. These columns are \
in all caps. Derived columns are in snake_case.';

CREATE TABLE IF NOT EXISTS gold.suri (
    year_month            DATE COMMENT 'The year and month of the \
    observation. The day is set to 1.',
    Actor1CountryCode          STRING,
    Actor2CountryCode          STRING,

    total_events          INT COMMENT 'The total number of events performed \
    by the source actor on the target actor.',
    geo_unrest_score      INT COMMENT 'The geopolitical unrest score, which \
    is the total number of unrest-related events (CAMEO event codes 10, 13, \
    and 14) performed by the source actor on the target actor.',
    total_gov_gov_events  INT COMMENT 'The total number of events where the \
    source Type1Code is "GOV" and the target Type1Code is "GOV".',
    total_ngov_gov_events INT COMMENT 'The total number of events where the \
    source Type1Code is not "GOV" and the target Type1Code is "GOV".',
    total_gov_ngov_events INT COMMENT 'The total number of events where the \
    source Type1Code is "GOV" and the target Type1Code is not "GOV".',
    pol_involve_score     DOUBLE COMMENT 'The political involvement score, \
    which measures how involved the government is in all events of a \
    relationship. Is is the sum of total_gov_gov_events, \
    total_ngov_gov_events, and total_gov_ngov_events, divided by \
    total_events.',
    suri_score            DOUBLE COMMENT 'The Social Unrest Risk Index (SURI) \
    score, which is the product of the geo_unrest_score and the \
    pol_involve_score.',
    
    avg_goldstein         DOUBLE COMMENT 'The average GoldsteinScale across \
    all events performed by the source actor on the target actor.',
    total_mentions        INT COMMENT 'The total number of mentions across \
    all events performed by the source actor on the target actor.',
    total_sources         INT COMMENT 'The total number of sources across \
    all events performed by the source actor on the target actor.',
    total_articles        INT COMMENT 'The total number of articles across \
    all events performed by the source actor on the target actor.',
    avg_tone              DOUBLE COMMENT 'The average AvgTone across all \
    events performed by the source actor on the target actor.',

    ingested_at           TIMESTAMP COMMENT 'The timestamp when the data was \
    ingested into the database.'
)
USING DELTA
PARTITIONED BY (year_month)
COMMENT 'Monthly directional Social Unrest Risk Index (SURI) scores derived \
from GDELT unrest events. See the GDELT documentation for descriptions about \
columns included in the input data. These columns are in all caps. Derived \
columns are in snake_case.'';

-- Volumes
CREATE VOLUME IF NOT EXISTS bronze.staging_files;