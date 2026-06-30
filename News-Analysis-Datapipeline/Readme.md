# 📰 News Data Analysis Project

## 📋 Project Overview

This project demonstrates a comprehensive **news data extraction and analysis pipeline** using **NewsAPI**, **Apache Airflow**, **Google Cloud Storage**, and **Snowflake**. The system automatically fetches news articles, processes them, and creates analytical tables for news source and author activity analysis.

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   NewsAPI       │───▶│  Apache Airflow  │───▶│ Google Cloud    │───▶│   Snowflake     │
│                 │    │                  │    │ Storage (GCS)   │    │                 │
│ • Real-time     │    │ • Daily DAG      │    │ • Parquet Files │    │ • Data Warehouse│
│   News Data     │    │ • Data Pipeline  │    │ • Raw Storage   │    │ • Analytics     │
│ • Pagination    │    │ • Orchestration  │    │ • Auto-cleanup  │    │ • Summary Tables│
└─────────────────┘    └──────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🎯 Key Features

- **🔄 Automated Data Extraction**: Daily news fetching with pagination support
- **📊 Real-time Processing**: Live news data from NewsAPI with comprehensive coverage
- **☁️ Cloud Integration**: Seamless GCS and Snowflake integration
- **🎛️ Airflow Orchestration**: Scheduled data pipeline with error handling
- **📈 Analytics Ready**: Pre-built summary tables for news source and author analysis
- **🔧 Schema Inference**: Automatic table creation from Parquet files
- **📝 Data Quality**: Content cleaning and validation

## 📁 Project Structure

```
News-Data-Analysis-Project/
├── fetch_news.py                    # News API data extraction script
├── news_api_airflow_job.py          # Airflow DAG for orchestration
├── snowflake_commands.sql           # Snowflake setup and configuration
└── README.md                        # This documentation
```

## 🗄️ Data Model

### **Source Data Structure:**
```json
{
    "newsTitle": "Article Title",
    "timestamp": "2025-10-03T10:30:00Z",
    "url_source": "https://example.com/article",
    "content": "Article content...",
    "source": "News Source Name",
    "author": "Author Name",
    "urlToImage": "https://example.com/image.jpg",
    "processed_at": "2025-10-03T10:35:00Z"
}
```

### **Analytical Tables:**

#### **1. Main Data Table: `news_api_data`**
- **Purpose**: Raw news articles with full content
- **Schema**: Auto-inferred from Parquet files
- **Content**: All article fields including title, content, source, author, timestamps

#### **2. News Source Summary: `summary_news`**
- **Purpose**: News source analytics and statistics
- **Metrics**: Article count, date range, source popularity
- **Use Cases**: Source performance analysis, content volume tracking

#### **3. Author Activity: `author_activity`**
- **Purpose**: Author productivity and distribution analysis
- **Metrics**: Article count, latest activity, source diversity
- **Use Cases**: Author performance tracking, content creator analysis

## 🔧 Technical Components

### **1. News Data Extraction (`fetch_news.py`)**

**Core Functions:**
- **`get_api_key_from_airflow()`**: Secure API key retrieval from Airflow Variables
- **`fetch_news_from_api()`**: Paginated news fetching with comprehensive coverage
- **`process_articles_to_dataframe()`**: Data cleaning and structure conversion
- **`upload_to_gcs()`**: Cloud storage integration with automatic cleanup

**Key Features:**
- **Pagination Support**: Fetches all available articles (not just first 100)
- **Content Cleaning**: Intelligent content trimming and validation
- **Error Handling**: Comprehensive exception handling and logging
- **Airflow Integration**: Seamless integration with Airflow Variables

### **2. Airflow Orchestration (`news_api_airflow_job.py`)**

**DAG Configuration:**
- **Schedule**: Daily execution
- **Start Date**: October 3, 2025
- **Retry Logic**: Configurable retry attempts
- **Error Handling**: Comprehensive failure management

**Task Pipeline:**
```
newsapi_data_to_gcs → snowflake_create_table → snowflake_copy_from_stage
                                                      ↓
                                    [news_summary_task, author_activity_task]
```

**Task Details:**
1. **`newsapi_data_to_gcs`**: Extract and upload news data
2. **`snowflake_create_table`**: Auto-create table with inferred schema
3. **`snowflake_copy_from_stage`**: Load data from GCS to Snowflake
4. **`create_or_replace_news_summary_tb`**: Generate source analytics
5. **`create_or_replace_author_activity_tb`**: Generate author analytics

### **3. Snowflake Setup (`snowflake_commands.sql`)**

**Database Setup:**
- **Database**: `news_api` dedicated database
- **File Format**: Parquet format for efficient storage
- **Storage Integration**: GCS integration for external data access
- **External Stage**: Direct access to GCS bucket

**Key Components:**
- **Storage Integration**: Secure GCS connection
- **External Stage**: `gcs_raw_data_stage` for data access
- **File Format**: Optimized Parquet format
- **Query Examples**: Sample queries for data exploration

## 🚀 Getting Started

### **Prerequisites:**
- Google Cloud Platform account with GCS access
- Snowflake account with appropriate permissions
- Apache Airflow environment
- NewsAPI account and API key

### **Setup Steps:**

#### **1. NewsAPI Setup:**
```bash
# Get API key from https://newsapi.org/
# Set up Airflow Variable
airflow variables set NEWS_API_KEY "your_api_key_here"
```

#### **2. Snowflake Setup:**
```sql
-- Run the setup script
-- File: snowflake_commands.sql
```

#### **3. GCS Configuration:**
```bash
# Ensure bucket exists and is accessible
gsutil ls gs://snowflake-projects-test-gds/news_data_analysis/
```

#### **4. Airflow Configuration:**
- Configure Snowflake connection: `snowflake_conn`
- Set up GCP service account permissions
- Deploy DAG to Airflow environment

### **Execution:**

#### **Manual Trigger:**
```bash
# Trigger DAG manually
airflow dags trigger newsapi_data_processing_dag
```

#### **Direct Execution:**
```python
# Run extraction script directly
python fetch_news.py
```

## 📊 Data Processing Pipeline

### **1. Data Extraction:**
- **API Call**: Fetch news articles with pagination
- **Content Processing**: Clean and validate article content
- **DataFrame Creation**: Structure data for analysis
- **File Generation**: Save as Parquet format

### **2. Cloud Storage:**
- **Upload**: Transfer Parquet files to GCS
- **Cleanup**: Remove local files after upload
- **Organization**: Structured file naming with timestamps

### **3. Data Loading:**
- **Schema Inference**: Automatic table structure detection
- **Data Transfer**: Copy from GCS to Snowflake
- **Validation**: Ensure data integrity

### **4. Analytics Generation:**
- **Source Analysis**: News source performance metrics
- **Author Analysis**: Author activity and distribution
- **Real-time Updates**: Daily refresh of analytical tables

## 📈 Analytics & Insights

### **News Source Analytics:**
```sql
-- Top news sources by article count
SELECT news_source, article_count, latest_article_date
FROM summary_news
ORDER BY article_count DESC;
```

### **Author Activity Analysis:**
```sql
-- Most prolific authors
SELECT author, article_count, distinct_sources
FROM author_activity
WHERE author IS NOT NULL
ORDER BY article_count DESC;
```

### **Content Analysis:**
```sql
-- Recent articles by source
SELECT "source", "newsTitle", "timestamp"
FROM news_api_data
WHERE "timestamp" >= CURRENT_DATE() - 1
ORDER BY "timestamp" DESC;
```

## 🔍 Data Quality & Validation

### **Content Processing:**
- **Text Cleaning**: Remove extra whitespace and format content
- **Length Validation**: Intelligent content trimming at sentence boundaries
- **Null Handling**: Comprehensive null value management
- **Data Types**: Proper type conversion and validation

### **Error Handling:**
- **API Failures**: Retry logic and error logging
- **Data Validation**: Content quality checks
- **Upload Failures**: GCS upload error handling
- **Processing Errors**: Individual article processing error handling

## 🛠️ Configuration

### **Environment Variables:**
```bash
# NewsAPI Configuration
NEWS_API_KEY=your_api_key_here

# GCS Configuration
GCS_BUCKET=snowflake-projects-test-gds
GCS_PATH=news_data_analysis/

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_DATABASE=news_api
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
```

### **Airflow Connections:**
- **Snowflake Connection**: `snowflake_conn`
- **GCP Connection**: `google_cloud_default`

### **Customizable Parameters:**
- **Search Query**: Default "technology", configurable
- **Days Back**: Default 1 day, adjustable
- **Page Size**: Default 100 articles per request
- **Content Length**: Default 200 characters max

## 🔧 Troubleshooting

### **Common Issues:**

#### **1. API Key Errors:**
- Verify API key in Airflow Variables
- Check NewsAPI account status and limits
- Validate API key permissions

#### **2. GCS Upload Failures:**
- Verify bucket permissions and access
- Check GCP service account configuration
- Validate file paths and naming

#### **3. Snowflake Connection Issues:**
- Verify connection parameters
- Check network access and firewall rules
- Validate user permissions and roles

#### **4. Data Quality Issues:**
- Review content cleaning logic
- Check for API response changes
- Validate data type conversions

## 📚 Learning Objectives

This project demonstrates:

1. **API Integration**: NewsAPI integration with pagination
2. **Data Processing**: Content cleaning and validation
3. **Cloud Storage**: GCS integration and file management
4. **Orchestration**: Airflow DAG design and task dependencies
5. **Data Warehousing**: Snowflake setup and data loading
6. **Analytics**: Summary table creation and insights
7. **Error Handling**: Comprehensive error management
8. **Security**: Secure API key management

## 🎯 Business Value

- **Content Intelligence**: Automated news monitoring and analysis
- **Source Tracking**: News source performance and reliability analysis
- **Author Insights**: Content creator activity and distribution analysis
- **Trend Analysis**: Daily news pattern identification
- **Data Quality**: Clean, structured news data for analysis
- **Scalability**: Cloud-native architecture for growth
- **Cost Efficiency**: Automated processing reduces manual effort
