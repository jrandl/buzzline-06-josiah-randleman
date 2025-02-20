"""
producer_randleman.py
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import os
import pathlib
import random
import sys
import time
from datetime import datetime

# import external modules
from kafka import KafkaProducer

# import from local modules
import utils.utils_config as config
from utils.utils_producer import verify_services, create_kafka_topic
from utils.utils_logger import logger

#####################################
# Stub Sentiment Analysis Function
#####################################


def assess_sentiment(text: str) -> float:
    """
    Stub for sentiment analysis.
    Returns a random float between 0 and 1 for now.
    """
    return round(random.uniform(0, 1), 2)


#####################################
# Define Message Generator
#####################################


def generate_messages():
    """
    Generate a stream of JSON messages.
    """
    USERS = ["Alice", "Bob", "Charlie", "Eve", "Frank", "Grace"]
    MERCHANT_CATEGORIES = ["Grocery", "Gas Station", "Online Shopping", "Restaurant", "Retail Store"]
    PURCHASE_LOCATION = [64401, 64448, 64439, 64506, 64436, 64048, 64469, 64456, 64461, 64739, 64730, 66767, 64067, 64633, 65326, 65803, 65742, 66006, 66032]
    HOME_LOCATION = [64401, 64448, 64439, 64506, 64436, 64048, 64469, 64456, 64461, 64739, 64730, 66767, 64067, 64633, 65326, 65803, 65742, 66006, 66032]
    CARD_TYPES = ["Debit", "Credit"]

    while True:
        users = random.choice(USERS)
        merchants_category = random.choice(MERCHANT_CATEGORIES)
        purchase_location = random.choice(PURCHASE_LOCATION)
        home_location = random.choice(HOME_LOCATION)
        card_type = random.choice(CARD_TYPES)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Generate a random transaction amount between $1.00 and $500.00
        amount = round(random.uniform(1.00, 1000.00), 2)

        # Create JSON message
        json_message = {
            "name": users,
            "merchant": merchants_category,
            "amount": amount,
            "purchase_location": purchase_location,
            "home_location": home_location,
            "type": card_type,
            "timestamp": timestamp,
        }

        yield json_message


#####################################
# Define Main Function
#####################################


def main() -> None:

    logger.info("Starting Producer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    logger.info("STEP 1. Read required environment variables.")

    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        topic: str = config.get_kafka_topic()
        kafka_server: str = config.get_kafka_broker_address()
        live_data_path: pathlib.Path = config.get_live_data_path()
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete the live data file if exists to start fresh.")

    try:
        if live_data_path.exists():
            live_data_path.unlink()
            logger.info("Deleted existing live data file.")

        logger.info("STEP 3. Build the path folders to the live data file if needed.")
        os.makedirs(live_data_path.parent, exist_ok=True)
    except Exception as e:
        logger.error(f"ERROR: Failed to delete live data file: {e}")
        sys.exit(2)

    logger.info("STEP 4. Try to create a Kafka producer and topic.")
    producer = None

    try:
        verify_services()
        producer = KafkaProducer(
            bootstrap_servers=kafka_server,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        logger.info(f"Kafka producer connected to {kafka_server}")
    except Exception as e:
        logger.warning(f"WARNING: Kafka connection failed: {e}")
        producer = None

    if producer:
        try:
            create_kafka_topic(topic)
            logger.info(f"Kafka topic '{topic}' is ready.")
        except Exception as e:
            logger.warning(f"WARNING: Failed to create or verify topic '{topic}': {e}")
            producer = None

    logger.info("STEP 5. Generate messages continuously.")
    try:
        for message in generate_messages():
            logger.info(message)

            with live_data_path.open("a") as f:
                f.write(json.dumps(message) + "\n")
                logger.info(f"STEP 4a Wrote message to file: {message}")

            # Send to Kafka if available
            if producer:
                producer.send(topic, value=message)
                logger.info(f"STEP 4b Sent message to Kafka topic '{topic}': {message}")

            time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("WARNING: Producer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        if producer:
            producer.close()
            logger.info("Kafka producer closed.")
        logger.info("TRY/FINALLY: Producer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
