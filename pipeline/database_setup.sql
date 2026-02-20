-- This file contains the initial database setup for the GDELT project. This 
-- should only need to be run once, but it's safe to run multiple times.


-- Catalog
CREATE CATALOG IF NOT EXISTS gdelt_project
COMMENT 'Catalog for GDELT-based social unrest analysis project';

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
    download_date         DATE,
    ingested_at           TIMESTAMP,
    data_file_url         STRING
)
USING DELTA
PARTITIONED BY (download_date)
COMMENT 'Raw GDELT events data';