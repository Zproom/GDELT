-- This file contains the initial catalog and schema setup for the GDELT 
-- project. This should only need to be run once.
CREATE CATALOG IF NOT EXISTS gdelt_project;

CREATE SCHEMA IF NOT EXISTS gdelt_project.bronze;
CREATE SCHEMA IF NOT EXISTS gdelt_project.silver;
CREATE SCHEMA IF NOT EXISTS gdelt_project.gold;

CREATE VOLUME gdelt_project.bronze.raw_data;