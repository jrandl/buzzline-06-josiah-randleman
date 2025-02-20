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
from consumers.sql_lite_config_randleman import init_db, insert_message, insert_fraud, insert_legit_transaction

# Define Fraud Detection Function
def is_fraudulent(transaction):
    """
    Determine if a transaction is fraudulent based on predefined rules.
    
    Args:
        transaction (dict): The transaction data.
    
    Returns:
        bool: True if fraudulent, False otherwise.
    """
    amount = transaction.get("amount", 0)
    purchase_location = transaction.get("purchase_location")
    home_location = transaction.get("home_location")
    merchant = transaction.get("merchant")
    card_type = transaction.get("type")

    # Rule 1: Large transactions (Over $900 are suspicious)
    if amount > 900:
        return True

    # Rule 2: Location mismatch (Far from home and large purchase)
    if purchase_location != home_location and amount > 500:
        return True

    # Rule 3: Suspicious Merchant Categories (Large purchases at uncommon places)
    risky_merchants = ["Online Shopping", "Retail Store"]
    if merchant in risky_merchants and amount > 700:
        return True

    # Rule 4: Unusual Card Type Usage (Sudden high use of debit for big transactions)
    if card_type == "Debit" and amount > 800:
        return True

    return False

#####################################
# Function to process a single message
# #####################################


def process_message(message: dict) -> None:
    """
    Process and transform a single JSON message.
    Converts message fields to appropriate data types.

    Args:
        message (dict): The JSON message as a Python dictionary.
    """
    logger.info("Called process_message() with:")
    logger.info(f"   {message=}")
    try:
        processed_message = {
            "name": message.get("name"),
            "merchant": message.get("merchant"),
            "amount": float(message.get("amount", 0.0)),
            "purchase_location": int(message.get("purchase_location", 0)),
            "home_location": int(message.get("home_location", 0)),
            "type": message.get("type"),
            "timestamp": message.get("timestamp"),
        }
        logger.info(f"Processed message: {processed_message}")
        return processed_message
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None


#####################################
# Consume Messages from Kafka Topic
#####################################


def consume_messages_from_kafka(
    topic: str,
    kafka_url: str,
    group: str,
    sql_path: pathlib.Path,
    interval_secs: int,
):
    """
    Consume new messages from Kafka topic and process them.
    Each message is expected to be JSON-formatted.

    Args:
    - topic (str): Kafka topic to consume messages from.
    - kafka_url (str): Kafka broker address.
    - group (str): Consumer group ID for Kafka.
    - sql_path (pathlib.Path): Path to the SQLite database file.
    - interval_secs (int): Interval between reads from the file.
    """
    logger.info("Called consume_messages_from_kafka() with:")
    logger.info(f"   {topic=}")
    logger.info(f"   {kafka_url=}")
    logger.info(f"   {group=}")
    logger.info(f"   {sql_path=}")
    logger.info(f"   {interval_secs=}")

    logger.info("Step 1. Verify Kafka Services.")
    try:
        verify_services()
    except Exception as e:
        logger.error(f"ERROR: Kafka services verification failed: {e}")
        sys.exit(11)

    logger.info("Step 2. Create a Kafka consumer.")
    try:
        consumer: KafkaConsumer = create_kafka_consumer(
            topic,
            group,
            value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
        )
    except Exception as e:
        logger.error(f"ERROR: Could not create Kafka consumer: {e}")
        sys.exit(11)

    logger.info("Step 3. Verify topic exists.")
    if consumer is not None:
        try:
            is_topic_available(topic)
            logger.info(f"Kafka topic '{topic}' is ready.")
        except Exception as e:
            logger.error(
                f"ERROR: Topic '{topic}' does not exist. Please run the Kafka producer. : {e}"
            )
            sys.exit(13)

    logger.info("Step 4. Process messages.")

    if consumer is None:
        logger.error("ERROR: Consumer is None. Exiting.")
        sys.exit(13)

    try:
        # consumer is a KafkaConsumer
        # message is a kafka.consumer.fetcher.ConsumerRecord
        # message.value is a Python dictionary
        for message in consumer:
            processed_message = process_message(message.value)
            is_fraud = is_fraudulent(processed_message) # added

            if is_fraud: # added
                logger.warning(f"🚨 FRAUD DETECTED: {processed_message}") # added
                insert_fraud(processed_message, sql_path)
            else: # added
                logger.info(f"✅ Legitimate Transaction: {processed_message}") # added
                insert_legit_transaction(processed_message, sql_path)

            if processed_message:
                insert_message(processed_message, sql_path)



    except Exception as e:
        logger.error(f"ERROR: Could not consume messages from Kafka: {e}")
        raise


#####################################
# Define Main Function
#####################################


def main():
    """
    Main function to run the consumer process.

    Reads configuration, initializes the database, and starts consumption.
    """
    logger.info("Starting Consumer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    logger.info("STEP 1. Read environment variables using new config functions.")
    try:
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address()
        group_id = config.get_kafka_consumer_group_id()
        interval_secs: int = config.get_message_interval_seconds_as_int()
        sqlite_path: pathlib.Path = config.get_sqlite_path()
        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete any prior database file for a fresh start.")
    if sqlite_path.exists():
        try:
            sqlite_path.unlink()
            logger.info("SUCCESS: Deleted database file.")
        except Exception as e:
            logger.error(f"ERROR: Failed to delete DB file: {e}")
            sys.exit(2)

    logger.info("STEP 3. Initialize a new database with an empty table.")
    try:
        init_db(sqlite_path)
    except Exception as e:
        logger.error(f"ERROR: Failed to create db table: {e}")
        sys.exit(3)

    logger.info("STEP 4. Begin consuming and storing messages.")
    try:
        consume_messages_from_kafka(
            topic, kafka_url, group_id, sqlite_path, interval_secs
        )
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
