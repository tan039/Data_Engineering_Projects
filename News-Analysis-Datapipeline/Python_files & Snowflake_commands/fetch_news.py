import pandas as pd
import json
import requests
import datetime
from datetime import date, timedelta
import uuid
import os
import logging
from typing import Optional, Dict, Any
from google.cloud import storage
from airflow.models import Variable

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def upload_to_gcs(bucket_name: str, destination_blob_name: str, source_file_name: str) -> bool:
    """
    Upload a file to Google Cloud Storage bucket.
    
    Args:
        bucket_name (str): Name of the GCS bucket
        destination_blob_name (str): Destination path in the bucket
        source_file_name (str): Local file path to upload
        
    Returns:
        bool: True if upload successful, False otherwise
        
    Raises:
        Exception: If upload fails
    """
    try:
        logger.info(f"Starting upload of {source_file_name} to GCS bucket: {bucket_name}")
        
        # Initialize GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        # Upload file
        blob.upload_from_filename(source_file_name)
        
        logger.info(f"Successfully uploaded {source_file_name} to {destination_blob_name}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to upload {source_file_name} to GCS: {str(e)}")
        raise


def get_api_key_from_airflow() -> str:
    """
    Retrieve NewsAPI key from Airflow Variables.
    
    Returns:
        str: NewsAPI key
        
    Raises:
        ValueError: If API key is not found or empty
    """
    try:
        # Try to get API key from Airflow Variable
        api_key = Variable.get("NEWS_API_KEY")
        
        if not api_key or api_key.strip() == "":
            raise ValueError("NEWS_API_KEY variable is empty or not set in Airflow")
            
        logger.info("Successfully retrieved API key from Airflow Variables")
        return api_key.strip()
        
    except Exception as e:
        logger.error(f"Failed to retrieve API key from Airflow: {str(e)}")
        raise ValueError("API key not found in Airflow Variables or environment variables")


def clean_article_content(content: Optional[str]) -> str:
    """
    Clean and trim article content to a reasonable length.
    
    Args:
        content (Optional[str]): Raw article content
        
    Returns:
        str: Cleaned and trimmed content
    """
    if not content:
        return ""
    
    # Remove extra whitespace
    cleaned_content = content.strip()
    
    # If content is longer than 200 characters, try to trim at sentence boundary
    if len(cleaned_content) > 200:
        # Find the last complete sentence
        last_period_index = cleaned_content.rfind('.')
        if last_period_index > 100:  # Ensure we don't trim too much
            return cleaned_content[:last_period_index + 1]
        else:
            # If no good sentence boundary, trim at 199 characters
            return cleaned_content[:199]
    
    return cleaned_content


def fetch_news_from_api(api_key: str, search_query: str, days_back: int = 1) -> Dict[str, Any]:
    """
    Fetch news articles from NewsAPI with pagination to get all available articles.
    
    Args:
        api_key (str): NewsAPI key
        search_query (str): Search term for news articles
        days_back (int): Number of days to look back for articles
        
    Returns:
        Dict[str, Any]: API response data with all articles from all pages
        
    Raises:
        requests.RequestException: If API request fails
        ValueError: If API response is invalid
    """
    try:
        # Calculate date range
        # To date
        today = date.today()

        # From date
        start_date = today - timedelta(days=days_back)
        
        # Construct API URL
        base_url = "https://newsapi.org/v2/everything"
        
        # Base parameters for all requests
        base_params = {
            'q': search_query,
            'from': start_date.isoformat(),
            'to': today.isoformat(),
            'sortBy': 'popularity',
            'apiKey': api_key,
            'language': 'en',
            'pageSize': 100  # Maximum articles per request
        }
        
        logger.info(f"Fetching news for query: '{search_query}' from {start_date} to {today}")
        
        all_articles = []
        page = 1
        total_results = 0
        
        while True:
            # Add page parameter for pagination
            params = base_params.copy()
            params['page'] = page
            
            logger.info(f"Fetching page {page}...")
            
            # Make API request
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()  # Raise exception for HTTP errors
            
            data = response.json()
            
            # Validate response
            if data.get('status') != 'ok':
                raise ValueError(f"API returned error status: {data.get('message', 'Unknown error')}")
            
            # Get articles from current page
            articles = data.get('articles', [])
            
            # If no articles on this page, we've reached the end
            if not articles:
                logger.info(f"No more articles found on page {page}")
                break
            
            # Add articles to our collection
            all_articles.extend(articles)
            
            # Get total results from first page only
            if page == 1:
                total_results = data.get('totalResults', 0)
                logger.info(f"Total articles available: {total_results}")
            
            # Check if we've fetched all available articles
            if len(all_articles) >= total_results:
                logger.info(f"Fetched all {total_results} available articles")
                break
            
            # Check if we got fewer articles than pageSize (indicates last page)
            if len(articles) < base_params['pageSize']:
                logger.info(f"Last page reached (got {len(articles)} articles)")
                break

            break
            # Move to next page
            # page += 1
            
            # Safety check to prevent infinite loops (NewsAPI has limits)
            # if page > 10:  # Maximum 10 pages (1000 articles)
            #     logger.warning(f"Reached maximum page limit ({page-1}). Stopping pagination.")
            #     break
        
        # Create final response data structure
        final_data = {
            'status': 'ok',
            'totalResults': len(all_articles),
            'articles': all_articles
        }
        
        logger.info(f"Successfully fetched {len(all_articles)} articles across {page} pages")
        
        return final_data
        
    except requests.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error processing API response: {str(e)}")
        raise


def process_articles_to_dataframe(articles: list) -> pd.DataFrame:
    """
    Process raw article data into a structured DataFrame.
    
    Args:
        articles (list): List of article dictionaries from API
        
    Returns:
        pd.DataFrame: Processed articles as DataFrame
    """
    logger.info(f"Processing {len(articles)} articles into DataFrame")
    
    # Initialize DataFrame with required columns
    df = pd.DataFrame(columns=[
        'newsTitle', 'timestamp', 'url_source', 'content', 
        'source', 'author', 'urlToImage', 'processed_at'
    ])
    
    processed_count = 0
    
    for article in articles:
        try:
            # Extract article fields with null safety
            news_title = article.get('title', '').strip()
            timestamp = article.get('publishedAt', '')
            url_source = article.get('url', '')
            source_name = article.get('source', {}).get('name', '') if article.get('source') else ''
            author = article.get('author', '').strip() if article.get('author') else ''
            url_to_image = article.get('urlToImage', '')
            raw_content = article.get('content', '')
            
            # Clean and process content
            cleaned_content = clean_article_content(raw_content)
            
            # Create new row
            new_row = pd.DataFrame({
                'newsTitle': [news_title],
                'timestamp': [timestamp],
                'url_source': [url_source],
                'content': [cleaned_content],
                'source': [source_name],
                'author': [author],
                'urlToImage': [url_to_image],
                'processed_at': [datetime.datetime.now().isoformat()]
            })
            
            # Append to main DataFrame
            df = pd.concat([df, new_row], ignore_index=True)
            processed_count += 1
            
        except Exception as e:
            logger.warning(f"Failed to process article: {str(e)}")
            continue
    
    logger.info(f"Successfully processed {processed_count} articles")
    return df


def save_dataframe_to_parquet(df: pd.DataFrame, filename: str) -> str:
    """
    Save DataFrame to Parquet file.
    
    Args:
        df (pd.DataFrame): DataFrame to save
        filename (str): Output filename
        
    Returns:
        str: Full path to saved file
        
    Raises:
        Exception: If file save fails
    """
    try:
        logger.info(f"Saving DataFrame with {len(df)} rows to {filename}")
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(filename) if os.path.dirname(filename) else '.', exist_ok=True)
        
        # Save to Parquet
        df.to_parquet(filename, index=False, engine='pyarrow')
        
        # Verify file was created
        if os.path.exists(filename):
            file_size = os.path.getsize(filename)
            logger.info(f"Successfully saved {filename} ({file_size} bytes)")
            return filename
        else:
            raise Exception(f"File {filename} was not created")
            
    except Exception as e:
        logger.error(f"Failed to save DataFrame to {filename}: {str(e)}")
        raise


def fetch_news_data(search_query: str = "technology", days_back: int = 1) -> str:
    """
    Main function to fetch news data from NewsAPI, process it, and upload to GCS.
    
    Args:
        search_query (str): Search term for news articles (default: "technology")
        days_back (int): Number of days to look back for articles (default: 1)
        
    Returns:
        str: GCS path of uploaded file
        
    Raises:
        Exception: If any step in the data extraction process fails
    """
    try:
        logger.info(f"Starting news data extraction for query: '{search_query}' (last {days_back} days)")
        
        # Get API key from Airflow Variables
        api_key = get_api_key_from_airflow()
        
        # Fetch news data from API
        api_response = fetch_news_from_api(api_key, search_query, days_back)
        articles = api_response.get('articles', [])
        
        if not articles:
            logger.warning(f"No articles found for query '{search_query}' in the last {days_back} days")
            return None
        
        # Process articles into DataFrame
        df = process_articles_to_dataframe(articles)
        
        if df.empty:
            logger.warning("No articles were successfully processed into DataFrame")
            return None
        
        # Generate filename with timestamp
        current_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'news_data_{search_query}_{current_time}.parquet'
        
        # Save DataFrame to Parquet
        local_file_path = save_dataframe_to_parquet(df, filename)
        
        # Upload to GCS
        bucket_name = 'snowflake-projects-test-gds'
        destination_blob_name = f'news_data_analysis/{filename}'
        
        upload_success = upload_to_gcs(bucket_name, destination_blob_name, local_file_path)
        
        if upload_success:
            # Clean up local file
            os.remove(local_file_path)
            logger.info(f"Cleaned up local file: {local_file_path}")
            
            gcs_path = f"gs://{bucket_name}/{destination_blob_name}"
            logger.info(f"News data extraction completed successfully. {len(df)} articles saved to: {gcs_path}")
            return gcs_path
        else:
            raise Exception("Failed to upload file to GCS")
            
    except Exception as e:
        logger.error(f"News data extraction failed: {str(e)}")
        raise


# For backward compatibility and direct execution
if __name__ == "__main__":
    try:
        result = fetch_news_data("apple", 1)
        print(f"News API Data Extraction completed")
    except Exception as e:
        print(f"News API Data Extraction failed: {str(e)}")
        exit(1)
