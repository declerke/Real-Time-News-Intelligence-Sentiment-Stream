import os
import sys
import json
import time
import hashlib
import requests
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Producer
from textblob import TextBlob
import logging

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.utils import load_kafka_config, delivery_report

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# NewsData.io Configuration
NEWSDATA_API_KEY = os.getenv('NEWSDATA_API_KEY')
NEWSDATA_BASE_URL = 'https://newsdata.io/api/1/news'

# Kafka Configuration
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'apple_news')

# News Query Configuration
NEWS_QUERY = os.getenv('NEWS_QUERY', 'kenya')
NEWS_LANGUAGE = os.getenv('NEWS_LANGUAGE', 'en')
NEWS_CATEGORY = os.getenv('NEWS_CATEGORY', 'top')
MAX_RESULTS = int(os.getenv('MAX_RESULTS', '10'))

# Continuous Polling Configuration
POLLING_ENABLED = os.getenv('POLLING_ENABLED', 'false').lower() == 'true'
POLLING_INTERVAL = int(os.getenv('POLLING_INTERVAL', '300'))  # Default: 5 minutes

# Deduplication Configuration
DEDUP_CACHE_SIZE = int(os.getenv('DEDUP_CACHE_SIZE', '10000'))
DEDUP_METHOD = os.getenv('DEDUP_METHOD', 'url')  # 'url', 'content_hash', or 'article_id'

# Global deduplication cache
seen_articles = set()


def generate_article_hash(article, method='url'):
    
    if method == 'article_id':
        # Use NewsData.io's article_id if available
        article_id = article.get('article_id', '')
        if article_id:
            return article_id
        # Fallback to URL-based hash
        method = 'url'
    
    if method == 'url':
        # Hash based on URL (most reliable for duplicate detection)
        url = article.get('link', article.get('url', ''))
        if url:
            return hashlib.md5(url.encode('utf-8')).hexdigest()
        # Fallback to content hash if no URL
        method = 'content_hash'
    
    if method == 'content_hash':
        # Hash based on title + description + published date
        title = article.get('title', '')
        description = article.get('description', '')
        pub_date = article.get('pubDate', article.get('published_at', ''))
        
        content = f"{title}|{description}|{pub_date}"
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    # Default fallback
    return hashlib.md5(str(article).encode('utf-8')).hexdigest()


def is_duplicate(article, method='url'):
    
    article_hash = generate_article_hash(article, method)
    
    if article_hash in seen_articles:
        return True
    
    # Add to cache
    seen_articles.add(article_hash)
    
    # Implement LRU-like behavior: remove oldest entries if cache is too large
    if len(seen_articles) > DEDUP_CACHE_SIZE:
        # Remove approximately 10% of oldest entries
        remove_count = DEDUP_CACHE_SIZE // 10
        for _ in range(remove_count):
            seen_articles.pop()
    
    return False


def get_sentiment_analysis(text):
    
    if not text or text.strip() == '':
        return {
            'polarity': 0.0,
            'subjectivity': 0.0,
            'sentiment': 'neutral'
        }
    
    try:
        blob = TextBlob(text)
        polarity = blob.sentiment.polarity
        subjectivity = blob.sentiment.subjectivity
        
        # Classify sentiment based on polarity
        if polarity > 0.1:
            sentiment = 'positive'
        elif polarity < -0.1:
            sentiment = 'negative'
        else:
            sentiment = 'neutral'
        
        return {
            'polarity': round(polarity, 4),
            'subjectivity': round(subjectivity, 4),
            'sentiment': sentiment
        }
    except Exception as e:
        logger.error(f"Sentiment analysis error: {e}")
        return {
            'polarity': 0.0,
            'subjectivity': 0.0,
            'sentiment': 'neutral'
        }


def fetch_news_from_newsdata(query, language='en', category=None, max_results=10):
    
    if not NEWSDATA_API_KEY:
        logger.error("NEWSDATA_API_KEY not found in environment variables")
        return []
    
    params = {
        'apikey': NEWSDATA_API_KEY,
        'q': query,
        'language': language,
        'size': min(max_results, 10)  # NewsData.io free tier max per request
    }
    
    if category and category != 'top':
        params['category'] = category
    
    try:
        logger.info(f"Fetching news for query: '{query}' in language: '{language}'")
        response = requests.get(NEWSDATA_BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get('status') == 'success':
            articles = data.get('results', [])
            logger.info(f"Successfully fetched {len(articles)} articles from API")
            return articles
        else:
            logger.error(f"API returned error: {data.get('message', 'Unknown error')}")
            return []
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching news from NewsData.io: {e}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON response: {e}")
        return []


def process_article(article):
    
    # Combine title and description for sentiment analysis
    text_for_sentiment = f"{article.get('title', '')} {article.get('description', '')}"
    
    sentiment_data = get_sentiment_analysis(text_for_sentiment)
    
    # Generate unique article hash for deduplication
    article_hash = generate_article_hash(article, DEDUP_METHOD)
    
    processed = {
        'article_id': article.get('article_id', article_hash),
        'article_hash': article_hash,
        'title': article.get('title', ''),
        'description': article.get('description', ''),
        'content': article.get('content', '')[:500] if article.get('content') else '',  # Limit content length
        'url': article.get('link', ''),
        'source_id': article.get('source_id', ''),
        'source_name': article.get('source_name', ''),
        'source_url': article.get('source_url', ''),
        'source_icon': article.get('source_icon', ''),
        'published_at': article.get('pubDate', ''),
        'country': ','.join(article.get('country', [])) if article.get('country') else '',
        'category': ','.join(article.get('category', [])) if article.get('category') else '',
        'language': article.get('language', ''),
        'image_url': article.get('image_url', ''),
        'video_url': article.get('video_url', ''),
        'keywords': ','.join(article.get('keywords', [])) if article.get('keywords') else '',
        'creator': ','.join(article.get('creator', [])) if article.get('creator') else '',
        'sentiment_polarity': sentiment_data['polarity'],
        'sentiment_subjectivity': sentiment_data['subjectivity'],
        'sentiment_label': sentiment_data['sentiment'],
        'processed_at': datetime.utcnow().isoformat()
    }
    
    return processed


def produce_to_kafka(producer, topic, articles):
    
    produced_count = 0
    
    for article in articles:
        try:
            # Convert article to JSON
            message = json.dumps(article)
            
            # Produce to Kafka
            producer.produce(
                topic,
                key=article['article_hash'],
                value=message,
                callback=delivery_report
            )
            
            # Trigger delivery reports
            producer.poll(0)
            produced_count += 1
            
        except Exception as e:
            logger.error(f"Error producing article to Kafka: {e}")
    
    # Wait for all messages to be delivered
    producer.flush()
    
    if produced_count > 0:
        logger.info(f"Successfully produced {produced_count} articles to Kafka topic '{topic}'")
    
    return produced_count


def fetch_and_process_batch(producer, topic):
    
    # Fetch news articles
    articles = fetch_news_from_newsdata(
        query=NEWS_QUERY,
        language=NEWS_LANGUAGE,
        category=NEWS_CATEGORY if NEWS_CATEGORY != 'top' else None,
        max_results=MAX_RESULTS
    )
    
    stats = {
        'fetched': len(articles),
        'duplicates': 0,
        'processed': 0,
        'produced': 0
    }
    
    if not articles:
        logger.warning("No articles fetched from API")
        return stats
    
    # Filter out duplicates
    unique_articles = []
    for article in articles:
        if is_duplicate(article, DEDUP_METHOD):
            stats['duplicates'] += 1
            logger.debug(f"Skipping duplicate article: {article.get('title', 'Unknown')[:50]}...")
        else:
            unique_articles.append(article)
    
    logger.info(f"Found {len(unique_articles)} unique articles out of {len(articles)} fetched ({stats['duplicates']} duplicates)")
    
    if not unique_articles:
        logger.info("No new articles to process")
        return stats
    
    # Process articles with sentiment analysis
    logger.info("Processing articles and performing sentiment analysis...")
    processed_articles = [process_article(article) for article in unique_articles]
    stats['processed'] = len(processed_articles)
    
    # Log sentiment distribution
    sentiment_counts = {}
    for article in processed_articles:
        sentiment = article['sentiment_label']
        sentiment_counts[sentiment] = sentiment_counts.get(sentiment, 0) + 1
    
    logger.info(f"Sentiment distribution: {sentiment_counts}")
    
    # Produce to Kafka
    stats['produced'] = produce_to_kafka(producer, topic, processed_articles)
    
    return stats


def run_continuous_polling(producer, topic):
    
    logger.info(f"Starting continuous polling mode (interval: {POLLING_INTERVAL}s)")
    logger.info(f"Deduplication method: {DEDUP_METHOD}")
    logger.info("Press Ctrl+C to stop")
    
    iteration = 0
    total_stats = {
        'total_fetched': 0,
        'total_duplicates': 0,
        'total_processed': 0,
        'total_produced': 0
    }
    
    try:
        while True:
            iteration += 1
            logger.info(f"\n{'='*60}")
            logger.info(f"Polling iteration #{iteration} started at {datetime.utcnow().isoformat()}")
            logger.info(f"{'='*60}")
            
            # Fetch and process batch
            stats = fetch_and_process_batch(producer, topic)
            
            # Update total statistics
            total_stats['total_fetched'] += stats['fetched']
            total_stats['total_duplicates'] += stats['duplicates']
            total_stats['total_processed'] += stats['processed']
            total_stats['total_produced'] += stats['produced']
            
            # Log iteration summary
            logger.info(f"\nIteration #{iteration} Summary:")
            logger.info(f"  - Fetched: {stats['fetched']}")
            logger.info(f"  - Duplicates: {stats['duplicates']}")
            logger.info(f"  - Processed: {stats['processed']}")
            logger.info(f"  - Produced: {stats['produced']}")
            
            logger.info(f"\nCumulative Statistics:")
            logger.info(f"  - Total Fetched: {total_stats['total_fetched']}")
            logger.info(f"  - Total Duplicates: {total_stats['total_duplicates']}")
            logger.info(f"  - Total Processed: {total_stats['total_processed']}")
            logger.info(f"  - Total Produced: {total_stats['total_produced']}")
            logger.info(f"  - Cache Size: {len(seen_articles)}")
            
            # Wait for next iteration
            logger.info(f"\nWaiting {POLLING_INTERVAL}s until next poll...")
            time.sleep(POLLING_INTERVAL)
            
    except KeyboardInterrupt:
        logger.info("\n\nContinuous polling stopped by user")
        logger.info(f"\nFinal Statistics:")
        logger.info(f"  - Total Iterations: {iteration}")
        logger.info(f"  - Total Fetched: {total_stats['total_fetched']}")
        logger.info(f"  - Total Duplicates: {total_stats['total_duplicates']}")
        logger.info(f"  - Total Processed: {total_stats['total_processed']}")
        logger.info(f"  - Total Produced: {total_stats['total_produced']}")
        logger.info(f"  - Deduplication Rate: {(total_stats['total_duplicates'] / max(total_stats['total_fetched'], 1)) * 100:.2f}%")


def main():
    
    logger.info("="*70)
    logger.info("Starting News Sentiment Analysis Producer")
    logger.info("="*70)
    logger.info(f"Configuration:")
    logger.info(f"  - News Query: {NEWS_QUERY}")
    logger.info(f"  - Language: {NEWS_LANGUAGE}")
    logger.info(f"  - Category: {NEWS_CATEGORY}")
    logger.info(f"  - Max Results: {MAX_RESULTS}")
    logger.info(f"  - Kafka Topic: {KAFKA_TOPIC}")
    logger.info(f"  - Polling Enabled: {POLLING_ENABLED}")
    logger.info(f"  - Polling Interval: {POLLING_INTERVAL}s")
    logger.info(f"  - Deduplication Method: {DEDUP_METHOD}")
    logger.info(f"  - Deduplication Cache Size: {DEDUP_CACHE_SIZE}")
    logger.info("="*70)
    
    # Load Kafka configuration
    kafka_config = load_kafka_config()
    if not kafka_config:
        logger.error("Failed to load Kafka configuration")
        return
    
    # Create Kafka producer
    producer = Producer(kafka_config)
    logger.info("Kafka producer initialized successfully\n")
    
    if POLLING_ENABLED:
        # Run in continuous polling mode
        run_continuous_polling(producer, KAFKA_TOPIC)
    else:
        # Run single batch mode
        logger.info("Running in single-batch mode (polling disabled)")
        stats = fetch_and_process_batch(producer, KAFKA_TOPIC)
        
        logger.info("\nBatch Processing Summary:")
        logger.info(f"  - Fetched: {stats['fetched']}")
        logger.info(f"  - Duplicates: {stats['duplicates']}")
        logger.info(f"  - Processed: {stats['processed']}")
        logger.info(f"  - Produced: {stats['produced']}")
    
    logger.info("\n" + "="*70)
    logger.info("Producer completed successfully")
    logger.info("="*70)


if __name__ == "__main__":
    main()