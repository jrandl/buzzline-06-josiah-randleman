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

            cursor.execute("DROP TABLE IF EXISTS transactions;")

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    merchant TEXT,
                    amount REAL,
                    purchase_location INTEGER,
                    home_location INTEGER,
                    type TEXT,
                    timestamp TEXT
                )
                """
            )


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

            cursor.execute(
                """
                INSERT INTO transactions (name, merchant, amount, purchase_location, home_location, type, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message.get("name"),
                    message.get("merchant"),
                    float(message.get("amount", 0.0)),
                    int(message.get("purchase_location", 0)),
                    int(message.get("home_location", 0)),
                    message.get("type"),
                    message.get("timestamp"),
                ),
            )

            

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
            cursor.execute("DELETE FROM transactions WHERE id = ?", (message_id,))
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
