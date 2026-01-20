# 🧠 Sentira: Real-Time News Intelligence & Sentiment Stream

**Sentira** is a high-performance, real-time data engineering pipeline designed to monitor global discourse on specific topics. By bridging the gap between live news APIs and cloud-scale analytics, it provides an automated window into public sentiment, using a modern distributed architecture to handle high-velocity data streams.

---

## 🎯 Project Goal
To build a scalable, production-grade ETL/ELT pipeline that fetches, deduplicates, and analyzes news sentiment in real-time, utilizing **Kafka** for message orchestration and **Google BigQuery** for high-performance analytical storage.



---

## 🧬 System Architecture
The pipeline follows a modular "Stream-Enrich-Store-Visualize" logic:

1.  **Live Ingestion:** Multi-threaded polling of the **NewsData.io** API with configurable search parameters.
2.  **Deduplication Engine:** A URL-based hashing layer using MD5 fingerprints to prevent redundant data processing and optimize cloud storage costs.
3.  **NLP Enrichment:** Real-time sentiment analysis using **TextBlob** to assign polarity and subjectivity scores to every incoming article.
4.  **Message Orchestration:** Distributed queuing via **Apache Kafka** to decouple data collection from database ingestion, ensuring zero data loss during spikes.
5.  **Warehouse Loading:** A specialized consumer service that streams enriched JSON payloads into **BigQuery** with schema validation.
6.  **Intelligence Layer:** A live **Streamlit** dashboard for monitoring sentiment trends and volume spikes.

---

## 🛠️ Technical Stack
| Layer | Tools | Purpose |
| :--- | :--- | :--- |
| **Containerization** | Docker, Docker-Compose | Environment isolation and service orchestration |
| **Stream Processing** | Apache Kafka, Confluent-Kafka | Scalable message brokering and buffering |
| **Data Engineering** | Python 3.9, Pandas | ETL logic, deduplication, and API handling |
| **Machine Learning** | TextBlob (NLP) | Sentiment polarity and subjectivity scoring |
| **Cloud Warehouse** | Google BigQuery | Distributed analytical storage and SQL engine |
| **Business Intelligence** | Streamlit | Real-time web visualization and KPI tracking |

---

## 📊 Performance & Features
* **Zero-Waste Pipeline:** Implemented a persistent cache that successfully identifies and filters out **70%+** of redundant news cycles.
* **Latency:** Achieved near real-time ingestion, moving data from API to Dashboard in under **5 seconds**.
* **Sentiment Metrics:** Categorizes news into **Positive, Neutral, and Negative** segments with associated confidence scores.
* **Scalability:** Dockerized architecture allows for easy horizontal scaling of consumers to handle higher volume topics.

---

## 📂 Project Structure
```text
news_sentiment/
├── producer/
│   └── producer.py         # News API client, NLP engine, & Kafka producer
├── consumer/
│   └── consumer.py         # BigQuery streamer & Kafka consumer
├── dashboard/
│   └── app.py              # Streamlit web application
├── common/
│   └── utils.py            # Shared Kafka & config utilities
├── credentials/
│   └── gcp.json            # Google Cloud Service Account (GitIgnored)
├── docker-compose.yml      # Multi-container orchestration
├── .env                    # API keys and system variables
└── requirements.txt        # Python dependency list
```

---

## ⚙️ Installation & Setup

### 1. Environment Configuration
Create a `.env` file in the root directory:
```env
NEWSDATA_API_KEY=your_key_here
GCP_PROJECT_ID=your_project_id
BIGQUERY_DATASET=news_sentiment
KAFKA_TOPIC=news_stream
POLLING_INTERVAL=300
```

### 2. Launch Services
Start the entire ecosystem using Docker:
```bash
docker-compose up -d --build
```

### 3. Monitor Streams
Follow the live logs to see deduplication and sentiment analysis in action:
```bash
docker-compose logs -f python
```

---

## 🎓 Skills Demonstrated
* **Distributed Systems:** Managing producer-consumer patterns using a message broker (Kafka).
* **Cloud Engineering:** Designing schemas and managing streaming inserts into BigQuery.
* **Stream Processing:** Real-time data enrichment and stateful deduplication.
* **Problem Solving:** Successfully managed "Data Drift" by implementing dynamic schema migrations for new NLP features.
* **DevOps:** Leveraging Docker for infrastructure-as-code and environment consistency.
