"""
consumer_josiah_randleman.py
"""

#####################################
# Import Modules
#####################################

import json
import os
import pathlib
import sys
import time
import sqlite3
import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Import Kafka consumer utilities
from kafka import KafkaConsumer

# Import from local modules
import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available

# Ensure the parent directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from consumers.db_sqlite_josiah_randleman import init_db, insert_message

#####################################
# Set up Live Visualization
#####################################

plt.ion()  # Enable interactive mode

fig, axes = plt.subplots(3, 1, figsize=(12, 12))  # Three subplots for real-time updates

# Path to the SQLite database
DB_PATH = config.get_sqlite_path()

#####################################
# Function to fetch real-time data from SQLite
#####################################

def fetch_data():
    """Fetch the latest sentiment data from the SQLite database."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()

            # Fetch latest sentiment messages (limit to last 100 for performance)
            cursor.execute("SELECT timestamp, sentiment FROM sentiment_messages ORDER BY timestamp DESC LIMIT 100")
            sentiment_data = cursor.fetchall()

            # Fetch author sentiment averages
            cursor.execute("SELECT author, avg_sentiment FROM author_sentiment ORDER BY avg_sentiment DESC")
            author_data = cursor.fetchall()

            # Fetch category sentiment averages
            cursor.execute("SELECT category, avg_sentiment FROM category_sentiment ORDER BY avg_sentiment DESC")
            category_data = cursor.fetchall()

        return sentiment_data, author_data, category_data

    except Exception as e:
        logger.error(f"Database error: {e}")
        return [], [], []

#####################################
# Function to update the visualization
#####################################

def update_dashboard():
    """Fetch latest data and update charts dynamically."""
    while True:
        sentiment_data, author_data, category_data = fetch_data()

        # Clear previous plots
        for ax in axes:
            ax.clear()

        # ✅ Plot 1: Sentiment Messages Over Time
        if sentiment_data:
            timestamps, sentiments = zip(*sentiment_data)
            timestamps = [datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") for ts in timestamps]  # Convert to datetime

            axes[0].plot(timestamps, sentiments, color="green", marker="o", linestyle="-", linewidth=2)
            axes[0].set_title("Sentiment Messages Over Time")
            axes[0].set_ylabel("Sentiment Score")
            axes[0].set_ylim(-1, 1)  # Sentiment range
            axes[0].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))  # Format x-axis for time
            plt.xticks(rotation=45)

        # ✅ Plot 2: Author Sentiment
        if author_data:
            authors, author_sentiments = zip(*author_data)
            axes[1].bar(authors, author_sentiments, color="skyblue")
            axes[1].set_title("Average Sentiment by Author")
            axes[1].set_ylabel("Sentiment Score")
            axes[1].set_ylim(-1, 1)  # Sentiment range

        # ✅ Plot 3: Category Sentiment
        if category_data:
            categories, category_sentiments = zip(*category_data)
            axes[2].bar(categories, category_sentiments, color="lightcoral")
            axes[2].set_title("Average Sentiment by Category")
            axes[2].set_ylabel("Sentiment Score")
            axes[2].set_ylim(-1, 1)  # Sentiment range

        # Refresh plot
        plt.draw()
        plt.pause(2)  # Update every 2 seconds

#####################################
# Kafka Message Processing
#####################################

def process_message(message: dict) -> None:
    """
    Process and transform a single JSON message.
    Converts message fields to appropriate data types.
    """
    logger.info(f"Processing message: {message}")

    try:
        processed_message = {
            "message": message.get("message"),
            "author": message.get("author"),
            "timestamp": message.get("timestamp"),
            "category": message.get("category"),
            "sentiment": float(message.get("sentiment", 0.0)),
            "keyword_mentioned": message.get("keyword_mentioned"),
            "message_length": int(message.get("message_length", 0)),
        }
        logger.info(f"Processed message: {processed_message}")

        # Insert message into the database
        insert_message(processed_message, DB_PATH)

    except Exception as e:
        logger.error(f"Error processing message: {e}")

#####################################
# Kafka Consumer Function
#####################################

def consume_messages_from_kafka(topic: str, kafka_url: str, group: str):
    """
    Consume new messages from Kafka topic and process them.
    Each message is expected to be JSON-formatted.
    """
    logger.info(f"Starting Kafka Consumer: {topic}")

    try:
        consumer: KafkaConsumer = create_kafka_consumer(
            topic,
            group,
            value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
        )
    except Exception as e:
        logger.error(f"ERROR: Could not create Kafka consumer: {e}")
        sys.exit(1)

    try:
        for message in consumer:
            process_message(message.value)

    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Could not consume messages from Kafka: {e}")
        sys.exit(1)

#####################################
# Define Main Function
#####################################

def main():
    """
    Main function to run the consumer process.
    """
    logger.info("Starting Consumer...")

    try:
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address()
        group_id = config.get_kafka_consumer_group_id()

        # Initialize database
        init_db(DB_PATH)

        # Start the Kafka consumer in a separate thread
        import threading
        consumer_thread = threading.Thread(target=consume_messages_from_kafka, args=(topic, kafka_url, group_id))
        consumer_thread.daemon = True
        consumer_thread.start()

        # Start the real-time dashboard
        update_dashboard()

    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Consumer shutting down.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
