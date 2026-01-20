.PHONY: help build up down restart logs producer consumer dashboard clean test

help:
	@echo "News Sentiment Analysis Pipeline - Available Commands:"
	@echo ""
	@echo "  make build       - Build Docker images"
	@echo "  make up          - Start all services"
	@echo "  make down        - Stop all services"
	@echo "  make restart     - Restart all services"
	@echo "  make logs        - View logs from all services"
	@echo "  make producer    - Run the news producer"
	@echo "  make consumer    - Run the Kafka consumer"
	@echo "  make dashboard   - Start the Streamlit dashboard"
	@echo "  make clean       - Remove containers and volumes"
	@echo "  make test        - Test the complete pipeline"
	@echo ""

build:
	@echo "Building Docker images..."
	docker-compose build

up:
	@echo "Starting services..."
	docker-compose up -d
	@echo "Services started. Dashboard available at http://localhost:8501"

down:
	@echo "Stopping services..."
	docker-compose down

restart:
	@echo "Restarting services..."
	docker-compose restart

logs:
	@echo "Viewing logs (Ctrl+C to exit)..."
	docker-compose logs -f

producer:
	@echo "Running news producer..."
	docker-compose run --rm python python producer/producer.py

consumer:
	@echo "Running Kafka consumer (Ctrl+C to stop)..."
	docker-compose run --rm python python consumer/consumer.py

dashboard:
	@echo "Starting Streamlit dashboard..."
	@echo "Dashboard will be available at http://localhost:8501"
	docker-compose up dashboard

clean:
	@echo "Cleaning up containers and volumes..."
	docker-compose down -v
	docker system prune -f

test:
	@echo "Testing complete pipeline..."
	@echo "1. Running producer..."
	docker-compose run --rm python python producer/producer.py
	@echo ""
	@echo "2. Starting consumer for 30 seconds..."
	timeout 30 docker-compose run --rm python python consumer/consumer.py || true
	@echo ""
	@echo "3. Test complete! Check BigQuery for data."
	@echo "4. Start dashboard with 'make dashboard'"