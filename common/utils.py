import os
import logging

logger = logging.getLogger(__name__)

CREDENTIALS_PATH = os.path.join('credentials', 'client.properties')

def load_kafka_config():
    if not os.path.exists(CREDENTIALS_PATH):
        logger.error(f"Kafka credentials file not found at {CREDENTIALS_PATH}")
        return None
    
    try:
        kafka_config = {}
        with open(CREDENTIALS_PATH, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    if '=' in line:
                        key, value = line.split('=', 1)
                        kafka_config[key.strip()] = value.strip()
        
        if kafka_config:
            logger.info("Kafka configuration loaded successfully")
            return kafka_config
        else:
            logger.error("Kafka configuration file is empty or invalid")
            return None
            
    except Exception as e:
        logger.error(f"Error loading Kafka configuration: {e}")
        return None

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def validate_env_vars():
    required_vars = [
        'NEWSDATA_API_KEY',
        'GCP_PROJECT_ID',
        'BIGQUERY_DATASET',
        'BIGQUERY_TABLE',
        'KAFKA_TOPIC'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return False
    
    logger.info("All required environment variables are set")
    return True