import os
import sys
from dotenv import load_dotenv
from google.cloud import bigquery
from confluent_kafka import Producer

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
load_dotenv()

def test_kafka():
    print("--- Testing Kafka Connection ---")
    try:
        from common.utils import load_kafka_config
        config = load_kafka_config()
        if not config:
            print("❌ Could not load Kafka config.")
            return
        
        test_config = config.copy()
        test_config.pop('request.required.acks', None)
        
        p = Producer(test_config)
        p.list_topics(timeout=10)
        print("✅ Kafka Connection Successful!")
    except Exception as e:
        print(f"❌ Kafka Connection Failed: {e}")

def test_bigquery():
    print("\n--- Testing BigQuery Connection ---")
    try:
        client = bigquery.Client()
        datasets = list(client.list_datasets())
        print(f"✅ BigQuery Connection Successful! Found {len(datasets)} datasets.")
        
        project = os.getenv('GCP_PROJECT_ID')
        dataset = os.getenv('BIGQUERY_DATASET')
        table = os.getenv('BIGQUERY_TABLE')
        table_ref = f"{project}.{dataset}.{table}"
        
        client.get_table(table_ref)
        print(f"✅ Table Access Verified: {table_ref}")
    except Exception as e:
        print(f"❌ BigQuery Connection Failed: {e}")

if __name__ == "__main__":
    test_kafka()
    test_bigquery()
