-- Create database for news API data
CREATE DATABASE news_api;

-- Switch to the news_api database
USE news_api;

-- Create file format for parquet
CREATE FILE FORMAT parquet_format TYPE=parquet;

-- Create storage integration to connect with Google Cloud Storage
CREATE OR REPLACE STORAGE INTEGRATION news_data_gcs_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = GCS
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = ('gcs://snowflake-projects-test-gds/news_data_analysis/')
;

-- Display storage integration details
DESC INTEGRATION news_data_gcs_integration;

-- Create external stage pointing to GCS location
CREATE OR REPLACE STAGE gcs_raw_data_stage
URL = 'gcs://snowflake-projects-test-gds/news_data_analysis/'
STORAGE_INTEGRATION = news_data_gcs_integration
FILE_FORMAT = (TYPE = 'PARQUET')
; 

-- List all stages in current database
show stages;

-- Query news data ordered by title
select * from news_api_data order by "newsTitle";