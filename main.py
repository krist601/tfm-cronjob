import boto3
import pandas as pd
from io import StringIO, BytesIO
from datetime import datetime
import logging
import os
from dotenv import load_dotenv
import requests

# Load the environment variables
load_dotenv()

# Get the credentials from the environment
k_id = ""
s_access_k = ""
region = ""

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class S3NewsProcessor:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=k_id,
            aws_secret_access_key=s_access_k,
            region_name=region
        )

    def fetch_csv_from_s3(self, bucket_name, file_key):
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=file_key)
            csv_content = response['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_content))
            return df
        except Exception as e:
            logger.error(f"Failed to fetch CSV from S3 bucket: {bucket_name}, file: {file_key}, error: {e}")
            raise

    def summarize_news(self, news_text):
        try:
            summary_response = requests.post(
                "http://13.48.177.124/summarize",#"http://127.0.0.1:8000/summarize",#
                json={"text": news_text}
            )
            logger.error(news_text)
            if summary_response.status_code == 200:
                summary_data = summary_response.json()
                return summary_data.get('summary', '')
            else:
                logger.error(f"Failed to get summary for news, status code: {summary_response.status_code}")
                return ''
        except Exception as e:
            logger.error(f"Failed to summarize news, error: {e}")
            return ''

    def process_and_store_news(self, input_bucket, input_key, output_bucket, output_key):
        news_df = self.fetch_csv_from_s3(input_bucket, input_key)
        summaries = []
        
        for index, (i, row) in enumerate(news_df.iterrows()):
            if i >= 3:
                break
            summary = self.summarize_news(row['body'])
            summaries.append({
                'title': row['title'],
                'url': row['url'],
                'summary': summary
            })

        summary_df = pd.DataFrame(summaries)
        csv_buffer = StringIO()
        summary_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        try:
            self.s3_client.put_object(
                Bucket=output_bucket,
                Key=output_key,
                Body=csv_buffer.getvalue()
            )
            logger.info(f"CSV file uploaded to S3 bucket: {output_bucket}/{output_key}")
        except Exception as e:
            logger.error(f"Failed to upload CSV to S3 bucket: {output_bucket}, file: {output_key}, error: {e}")
            raise

# Example usage
if __name__ == "__main__":
    input_bucket_name = 'complu-bucket'
    input_file_key = 'model-news/news_2024_06_26__10_44_39.csv'
    output_bucket_name = 'complu-bucket'
    output_file_key = f"summaries/news_summaries.csv"
    
    processor = S3NewsProcessor()
    processor.process_and_store_news(input_bucket_name, input_file_key, output_bucket_name, output_file_key)
