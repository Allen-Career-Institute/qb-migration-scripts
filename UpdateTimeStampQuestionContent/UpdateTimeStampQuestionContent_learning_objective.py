import mysql.connector
import csv
import logging
import time
import os
import sys
from datetime import datetime
from collections import Counter
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Set up logging
def setup_logging():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(base_dir, 'qbUpdate', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_filename = os.path.join(log_dir, f'question_update_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )

    return logging.getLogger(__name__)

# Validate CSV file
def validate_csv_file(csv_file_path, logger):
    if not os.path.exists(csv_file_path):
        logger.error(f"CSV file not found: {csv_file_path}")
        return False
    if os.path.getsize(csv_file_path) == 0:
        logger.error(f"CSV file is empty: {csv_file_path}")
        return False

    try:
        with open(csv_file_path, 'r') as file:
            csv_reader = csv.reader(file)
            first_row = next(csv_reader, None)
            if not first_row:
                logger.error("CSV file has no data rows")
                return False
    except Exception as e:
        logger.error(f"Error reading CSV file: {str(e)}")
        return False

    return True

# Process CSV and update database
def process_and_update_csv(csv_file_path, cursor, connection, update_query, logger):
    total_records = 0
    successful_updates = 0
    failed_updates = 0
    duplicate_ids = 0
    batch_size = 50
    current_batch = []
    processed_ids = set()

    # Check for duplicate question IDs in CSV
    all_ids = []
    with open(csv_file_path, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            if row and len(row) > 0:
                all_ids.append(row[0])

    id_counts = Counter(all_ids)
    duplicates = {id: count for id, count in id_counts.items() if count > 1}
    if duplicates:
        logger.warning(f"Found {len(duplicates)} duplicate question IDs: {duplicates}")

    # Read and process CSV file
    with open(csv_file_path, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            if row and len(row) > 0:
                qns_id = row[0]
                total_records += 1

                if qns_id in processed_ids:
                    logger.warning(f"Skipping duplicate question ID: {qns_id}")
                    duplicate_ids += 1
                    continue

                processed_ids.add(qns_id)
                current_batch.append(qns_id)

                if len(current_batch) == batch_size:
                    process_batch(cursor, connection, current_batch, update_query, logger)
                    successful_updates_batch, failed_updates_batch = get_batch_results(cursor)
                    successful_updates += successful_updates_batch
                    failed_updates += failed_updates_batch
                    current_batch.clear()
                    logger.info("Waiting 5 seconds before next batch...")
                    time.sleep(5)

                    if not connection.is_connected():
                        logger.warning("Connection lost. Attempting to reconnect...")
                        reconnect_database(connection, logger)

    if current_batch:
        logger.info(f"Processing final batch of {len(current_batch)} records...")
        process_batch(cursor, connection, current_batch, update_query, logger)
        successful_updates_batch, failed_updates_batch = get_batch_results(cursor)
        successful_updates += successful_updates_batch
        failed_updates += failed_updates_batch

    # Log final summary
    logger.info("=== Update Summary ===")
    logger.info(f"Total records found: {total_records}")
    logger.info(f"Duplicate IDs skipped: {duplicate_ids}")
    logger.info(f"Unique records processed: {total_records - duplicate_ids}")
    logger.info(f"Successful updates: {successful_updates}")
    logger.info(f"Failed updates: {failed_updates}")
    if total_records > 0:
        logger.info(f"Success rate: {(successful_updates/total_records)*100:.2f}%")

# Update questions in the database
def update_questions(csv_file_path):
    logger = setup_logging()

    if not validate_csv_file(csv_file_path, logger):
        return

    try:
        logger.info("Attempting to connect to database...")
        connection = mysql.connector.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connection_timeout=30
        )
        logger.info("Database connection successful")

        cursor = connection.cursor()

        update_query = """UPDATE question_pool.question_content
                          SET learning_objective = CONCAT(learning_objective, ' ')
                          WHERE qns_id = %s"""

        process_and_update_csv(csv_file_path, cursor, connection, update_query, logger)

    except mysql.connector.Error as e:
        logger.error(f"Database error: {str(e)}")
        handle_database_error(e, logger)

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        logger.exception("Full traceback:")

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            logger.info("MySQL connection closed")

# Process a batch of updates
def process_batch(cursor, connection, batch, update_query, logger):
    logger.info(f"Processing batch of {len(batch)} records...")
    for qns_id in batch:
        try:
            cursor.execute(update_query, (qns_id,))
        except mysql.connector.Error as e:
            logger.error(f"Error updating question ID {qns_id}: {str(e)}")

    try:
        connection.commit()
    except mysql.connector.Error as e:
        logger.error(f"Error committing batch: {str(e)}")
        connection.rollback()
        logger.info("Batch rolled back due to commit error")

# Get batch update results
def get_batch_results(cursor):
    successful = cursor.rowcount if cursor.rowcount > 0 else 0
    failed = max(0, cursor.rowcount) if cursor.rowcount < 0 else 0
    return successful, failed

# Reconnect to database if connection is lost
def reconnect_database(connection, logger, max_attempts=3):
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        try:
            connection.reconnect(attempts=1, delay=0)
            logger.info("Database reconnection successful")
            return True
        except mysql.connector.Error as e:
            logger.error(f"Reconnection attempt {attempt} failed: {str(e)}")
            time.sleep(5)

    logger.critical("Failed to reconnect to database after multiple attempts")
    return False

# Handle different database errors
def handle_database_error(error, logger):
    error_code = getattr(error, 'errno', None)
    if error_code == 2003:
        logger.critical("Cannot connect to MySQL server. Please check if the server is running.")
    elif error_code == 1044:
        logger.critical("Access denied to the database. Please check credentials.")
    elif error_code == 1049:
        logger.critical("Database does not exist.")
    elif error_code == 2006:
        logger.critical("MySQL server connection was lost. Please check server stability.")
    else:
        logger.critical(f"Database error occurred: {str(error)}")

# Main execution
if __name__ == "__main__":
    csv_file_path = sys.argv[1] if len(sys.argv) > 1 else './ids.csv'
    update_questions(csv_file_path)