""" db_sqlite_josiah_randleman.py 

Has the following functions:
- init_db(config): Initialize the SQLite database and create the 'streamed_messages' table if it doesn't exist.
- insert_message(message, config): Insert a single processed message into the SQLite database.

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

"""

#####################################
# Import Modules
#####################################

# import from standard library
import os
import pathlib
import sqlite3

# import from local modules
import utils.utils_config as config
from utils.utils_logger import logger

#####################################
# Define Function to Initialize SQLite Database
#####################################


def init_db(db_path: pathlib.Path):
    """
    Initialize the SQLite database -
    if it doesn't exist, create the 'streamed_messages' table
    and if it does, recreate it.

    Args:
    - db_path (pathlib.Path): Path to the SQLite database file.

    """
    logger.info("Calling SQLite init_db() with {db_path=}.")
    try:
        # Ensure the directories for the db exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            logger.info("SUCCESS: Got a cursor to execute SQL.")

            cursor.execute("DROP TABLE IF EXISTS streamed_messages;")

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS streamed_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message TEXT,
                    author TEXT,
                    timestamp TEXT,
                    category TEXT,
                    sentiment REAL,
                    keyword_mentioned TEXT,
                    message_length INTEGER
                )
            """
            )

            # Table to store each message
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS sentiment_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT,
                author TEXT,
                sentiment REAL,
                timestamp TEXT
            )
            """)

            # Table to store aggregated category sentiment
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS category_sentiment (
                category TEXT PRIMARY KEY,
                avg_sentiment REAL
            )
            """)

            # Table to store sentiment trends for authors
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS author_sentiment (
                author TEXT PRIMARY KEY,
                avg_sentiment REAL
            )
            """)

            conn.commit()
        logger.info(f"SUCCESS: Database initialized and table ready at {db_path}.")
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize a sqlite database at {db_path}: {e}")


#####################################
# Define Function to Insert a Processed Message into the Database
#####################################


def insert_message(message: dict, db_path: pathlib.Path) -> None:
    """
    Insert a single processed message into the SQLite database.

    Args:
    - message (dict): Processed message to insert.
    - db_path (pathlib.Path): Path to the SQLite database file.
    """
    logger.info("Calling SQLite insert_message() with:")
    logger.info(f"{message=}")
    logger.info(f"{db_path=}")

    try:
        # Extract fields safely from the message dictionary
        category = message.get("category", "unknown")
        author = message.get("author", "anonymous")
        sentiment = float(message.get("sentiment", 0.0))
        timestamp = message.get("timestamp", "unknown")
        message_text = message.get("message", "")
        keyword_mentioned = message.get("keyword_mentioned", "")
        message_length = int(message.get("message_length", 0))

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Insert the raw message into streamed_messages table
            cursor.execute("""
                INSERT INTO streamed_messages (message, author, timestamp, category, sentiment, keyword_mentioned, message_length)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (message_text, author, timestamp, category, sentiment, keyword_mentioned, message_length))

            # Insert sentiment analysis data into sentiment_messages
            cursor.execute("""
                INSERT INTO sentiment_messages (category, author, sentiment, timestamp)
                VALUES (?, ?, ?, ?)
            """, (category, author, sentiment, timestamp))

            # Update category sentiment (calculate average)
            cursor.execute("""
                INSERT INTO category_sentiment (category, avg_sentiment)
                VALUES (?, ?)
                ON CONFLICT(category) DO UPDATE SET avg_sentiment = (
                    SELECT AVG(sentiment) FROM sentiment_messages WHERE category = ?
                )
            """, (category, sentiment, category))

            # Update author sentiment (calculate average)
            cursor.execute("""
                INSERT INTO author_sentiment (author, avg_sentiment)
                VALUES (?, ?)
                ON CONFLICT(author) DO UPDATE SET avg_sentiment = (
                    SELECT AVG(sentiment) FROM sentiment_messages WHERE author = ?
                )
            """, (author, sentiment, author))

            conn.commit()
            logger.info("Inserted one message into the database.")

    except Exception as e:
        logger.error(f"ERROR: Failed to insert message into the database: {e}")



#####################################
# Define Function to Delete a Message from the Database
#####################################


def delete_message(message_id: int, db_path: pathlib.Path) -> None:
    """
    Delete a message from the SQLite database by its ID.

    Args:
    - message_id (int): ID of the message to delete.
    - db_path (pathlib.Path): Path to the SQLite database file.
    """
    STR_PATH = str(db_path)
    try:
        with sqlite3.connect(STR_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM streamed_messages WHERE id = ?", (message_id,))
            conn.commit()
        logger.info(f"Deleted message with id {message_id} from the database.")
    except Exception as e:
        logger.error(f"ERROR: Failed to delete message from the database: {e}")


#####################################
# Define main() function for testing
#####################################
def main():
    logger.info("Starting db testing.")

    # Use config to make a path to a parallel test database
    DATA_PATH: pathlib.path = config.get_base_data_path
    TEST_DB_PATH: pathlib.Path = DATA_PATH / "test_buzz.sqlite"

    # Initialize the SQLite database by passing in the path
    init_db(TEST_DB_PATH)
    logger.info(f"Initialized database file at {TEST_DB_PATH}.")

    test_message = {
        "message": "I just shared a meme! It was amazing.",
        "author": "Charlie",
        "timestamp": "2025-01-29 14:35:20",
        "category": "humor",
        "sentiment": 0.87,
        "keyword_mentioned": "meme",
        "message_length": 42,
    }

    insert_message(test_message, TEST_DB_PATH)

    # Retrieve the ID of the inserted test message
    try:
        with sqlite3.connect(TEST_DB_PATH, timeout=1.0) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM streamed_messages WHERE message = ? AND author = ?",
                (test_message["message"], test_message["author"]),
            )
            row = cursor.fetchone()
            if row:
                test_message_id = row[0]
                # Delete the test message
                delete_message(test_message_id, TEST_DB_PATH)
            else:
                logger.warning("Test message not found; nothing to delete.")
    except Exception as e:
        logger.error(f"ERROR: Failed to retrieve or delete test message: {e}")

    logger.info("Finished testing.")


# #####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
