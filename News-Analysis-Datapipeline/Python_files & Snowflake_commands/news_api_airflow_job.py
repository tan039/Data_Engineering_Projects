# Import required Airflow modules and operators
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, date
from fetch_news import fetch_news_data
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# =============================================================================
# DAG CONFIGURATION
# =============================================================================
# Default arguments for all tasks in the DAG
default_args = {
    'owner': 'growdataskills',           # DAG owner for monitoring and alerts
    'depends_on_past': False,            # Don't wait for previous DAG runs to complete
    'email_on_failure': False,           # Disable email notifications on failure
    'email_on_retry': False,             # Disable email notifications on retry
    'retries': 0,                        # Number of retry attempts for failed tasks
    'retry_delay': timedelta(minutes=5), # Wait time between retries
}

# =============================================================================
# DAG DEFINITION
# =============================================================================
# Create the main DAG object with scheduling and metadata
dag = DAG(
    'newsapi_data_processing_dag',                                    # Unique DAG identifier
    default_args=default_args,                           # Apply default arguments to all tasks
    description='Fetch news articles and save as Parquet in GCS',  # DAG description
    schedule_interval=timedelta(days=1),                 # Run daily at the same time
    start_date=datetime(2025, 10, 3),                   # DAG start date
    catchup=False,                                       # Don't run historical instances
)

# =============================================================================
# TASK 1: NEWS DATA EXTRACTION
# =============================================================================
# Extract news articles from NewsAPI and save to GCS as Parquet files
# This task calls the fetch_news_data function which handles:
# - API key retrieval from Airflow Variables
# - Pagination to fetch all available articles
# - Data processing and cleaning
# - Upload to Google Cloud Storage
fetch_news_data_task = PythonOperator(
    task_id='newsapi_data_to_gcs',                       # Unique task identifier
    python_callable=fetch_news_data,                     # Function to execute
    dag=dag,                                             # Associate with DAG
)

# =============================================================================
# TASK 2: SNOWFLAKE TABLE CREATION
# =============================================================================
# Create Snowflake table with auto-inferred schema from Parquet files in GCS
# This uses Snowflake's INFER_SCHEMA function to automatically detect column types
# and structure from the Parquet files in the external stage
snowflake_create_table = SQLExecuteQueryOperator(
    task_id="snowflake_create_table",                    # Task identifier
    sql="""CREATE TABLE IF NOT EXISTS news_api.PUBLIC.news_api_data USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(INFER_SCHEMA (
                    LOCATION => '@news_api.PUBLIC.gcs_raw_data_stage',
                    FILE_FORMAT => 'news_api.PUBLIC.parquet_format'
                ))
            )""",
    conn_id="snowflake_conn"                             # Snowflake connection ID from Airflow
)

# =============================================================================
# TASK 3: DATA LOADING FROM GCS TO SNOWFLAKE
# =============================================================================
# Copy data from GCS external stage into the Snowflake table
# Uses MATCH_BY_COLUMN_NAME for flexible column mapping
snowflake_copy = SQLExecuteQueryOperator(
    task_id="snowflake_copy_from_stage",                 # Task identifier
    sql="""COPY INTO news_api.PUBLIC.news_api_data 
            FROM @news_api.PUBLIC.gcs_raw_data_stage 
            MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
            FILE_FORMAT = (FORMAT_NAME = 'news_api.PUBLIC.parquet_format')
            """,
    conn_id="snowflake_conn"                             # Snowflake connection ID
)

# =============================================================================
# TASK 4: NEWS SOURCE SUMMARY TABLE
# =============================================================================
# Create summary table with news source statistics
# Provides insights into:
# - Article count per news source
# - Date range of articles from each source
# - Source popularity ranking
news_summary_task = SQLExecuteQueryOperator(
    task_id="create_or_replace_news_summary_tb",         # Task identifier
    sql="""
        CREATE OR REPLACE TABLE news_api.PUBLIC.summary_news AS
        SELECT
            "source" AS news_source,                    
            COUNT(*) AS article_count,                   
            MAX("timestamp") AS latest_article_date,     
            MIN("timestamp") AS earliest_article_date    
        FROM news_api.PUBLIC.news_api_data as tb
        GROUP BY "source"                               
        ORDER BY article_count DESC;                     
    """,
    conn_id="snowflake_conn"                             # Snowflake connection ID
)

# =============================================================================
# TASK 5: AUTHOR ACTIVITY ANALYSIS TABLE
# =============================================================================
# Create author activity summary table
# Provides insights into:
# - Article count per author
# - Author activity across different news sources
# - Most prolific authors
author_activity_task = SQLExecuteQueryOperator(
    task_id="create_or_replace_author_activity_tb",      # Task identifier
    sql="""
        CREATE OR REPLACE TABLE news_api.PUBLIC.author_activity AS
        SELECT
            "author",                                    
            COUNT(*) AS article_count,                  
            MAX("timestamp") AS latest_article_date,     
            COUNT(DISTINCT "source") AS distinct_sources 
        FROM news_api.PUBLIC.news_api_data as tb
        WHERE "author" IS NOT NULL                       
        GROUP BY "author"                               
        ORDER BY article_count DESC;                     
    """,
    conn_id="snowflake_conn"                             # Snowflake connection ID
)

# =============================================================================
# TASK DEPENDENCIES
# =============================================================================
# Define the execution order of tasks:
# 1. First: Extract news data and save to GCS
# 2. Second: Create Snowflake table with inferred schema
# 3. Third: Load data from GCS into Snowflake table
# 4. Fourth: Create summary tables in parallel (both can run simultaneously)

# Sequential execution: fetch → create table → copy data
fetch_news_data_task >> snowflake_create_table >> snowflake_copy

# Parallel execution: both summary tasks run after data loading is complete
snowflake_copy >> [news_summary_task, author_activity_task]