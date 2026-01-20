import os
import sys
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError
from google.cloud import bigquery
from google.oauth2 import service_account

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.utils import load_kafka_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET', 'news_sentiment')
BIGQUERY_TABLE = os.getenv('BIGQUERY_TABLE', 'apple_news')
GCP_CREDENTIALS_PATH = os.path.join('credentials', 'gcp.json')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'apple_news')
KAFKA_GROUP_ID = 'news-sentiment-consumer-group'

def get_bigquery_client():
    try:
        credentials = service_account.Credentials.from_service_account_file(
            GCP_CREDENTIALS_PATH,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        client = bigquery.Client(
            credentials=credentials,
            project=GCP_PROJECT_ID
        )
        logger.info("BigQuery client initialized successfully")
        return client
    except Exception as e:
        logger.error(f"Error initializing BigQuery client: {e}")
        return None

def create_table_if_not_exists(client):
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
    schema = [
        bigquery.SchemaField("article_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("content", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("source_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("source_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("source_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("source_icon", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("published_at", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("language", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("image_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("video_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("keywords", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("creator", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("sentiment_polarity", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("sentiment_subjectivity", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("sentiment_label", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("processed_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("inserted_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    try:
        client.get_table(table_id)
        logger.info(f"Table {table_id} already exists")
    except Exception:
        table = client.create_table(table)
        logger.info(f"Created table {table_id}")

def insert_into_bigquery(client, records):
    if not records:
        return False
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
    for record in records:
        record['inserted_at'] = datetime.utcnow().isoformat()
    try:
        errors = client.insert_rows_json(table_id, records)
        if errors:
            logger.error(f"Errors inserting rows: {errors}")
            return False
        logger.info(f"Successfully inserted {len(records)} records into BigQuery")
        return True
    except Exception as e:
        logger.error(f"Error inserting into BigQuery: {e}")
        return False

def process_message(message):
    try:
        record = json.loads(message.value().decode('utf-8'))
        if not record.get('article_id'):
            return None
        return record
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None

def main():
    bq_client = get_bigquery_client()
    if not bq_client: return
    create_table_if_not_exists(bq_client)
    kafka_config = load_kafka_config()
    if not kafka_config: return
    kafka_config['group.id'] = KAFKA_GROUP_ID
    kafka_config['auto.offset.reset'] = 'earliest'
    kafka_config['enable.auto.commit'] = True
    consumer = Consumer(kafka_config)
    consumer.subscribe([KAFKA_TOPIC])
    batch_size = 10
    batch = []
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if batch:
                    insert_into_bigquery(bq_client, batch)
                    batch = []
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Kafka error: {msg.error()}")
                continue
            record = process_message(msg)
            if record:
                batch.append(record)
                if len(batch) >= batch_size:
                    insert_into_bigquery(bq_client, batch)
                    batch = []
    except KeyboardInterrupt:
        pass
    finally:
        if batch: insert_into_bigquery(bq_client, batch)
        consumer.close()

if __name__ == "__main__":
    main()
