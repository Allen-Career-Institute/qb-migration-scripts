#!/usr/bin/env python3
"""
Script to migrate questions from 'questions' collection to 'new_questions' collection.
Reads oldQuestionIds from a CSV file and for each:
1. Fetches the latest version document from 'questions' collection
2. Deletes all existing documents with that oldQuestionId from 'new_questions' collection
3. Copies the exact document to 'new_questions' collection
"""

import csv
import sys
from pymongo import MongoClient
from datetime import datetime
import logging
from typing import List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class QuestionMigrator:
    def __init__(self, mongo_uri: str, database_name: str):
        """
        Initialize the migrator with MongoDB connection details.
        
        Args:
            mongo_uri: MongoDB connection URI
            database_name: Name of the database
        """
        try:
            # Add connection parameters for better reliability
            self.client = MongoClient(
                mongo_uri,
                serverSelectionTimeoutMS=60000,  # 60 seconds timeout
                connectTimeoutMS=60000,
                socketTimeoutMS=60000,
                retryWrites=True,
                w='majority'
            )
            # Test the connection
            self.client.admin.command('ping')
            logger.info(f"Successfully connected to MongoDB")
            
            self.db = self.client[database_name]
            self.questions_collection = self.db['questions']
            self.new_questions_collection = self.db['new_questions']
            logger.info(f"Connected to database: {database_name}")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            logger.error("Please check:")
            logger.error("1. MongoDB URI is correct")
            logger.error("2. Your IP address is whitelisted in MongoDB Atlas")
            logger.error("3. Network connectivity to MongoDB cluster")
            raise
    
    def read_old_question_ids_from_csv(self, csv_file_path: str) -> List[str]:
        """
        Read oldQuestionIds from CSV file.
        
        Args:
            csv_file_path: Path to the CSV file
            
        Returns:
            List of oldQuestionIds
        """
        old_question_ids = []
        try:
            with open(csv_file_path, 'r') as csvfile:
                csv_reader = csv.reader(csvfile)
                # Skip header if present
                header = next(csv_reader, None)
                logger.info(f"CSV Header: {header}")
                
                for row in csv_reader:
                    if row:  # Skip empty rows
                        # Handle comma-separated values in a single cell or multiple cells
                        for cell in row:
                            if cell.strip():
                                # Split by comma in case multiple IDs are in one cell
                                ids = [id.strip() for id in cell.split(',') if id.strip()]
                                old_question_ids.extend(ids)
            
            logger.info(f"Read {len(old_question_ids)} oldQuestionIds from CSV")
            return old_question_ids
        except FileNotFoundError:
            logger.error(f"CSV file not found: {csv_file_path}")
            raise
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise
    
    def get_latest_version_document(self, old_question_id: str) -> dict:
        """
        Fetch the latest version document from 'questions' collection.
        
        Args:
            old_question_id: The oldQuestionId to search for (will be converted to int)
            
        Returns:
            The latest version document or None if not found
        """
        try:
            # Convert oldQuestionId to integer since it's stored as int64 in MongoDB
            old_question_id_int = int(old_question_id)
            
            # Find all documents with the given oldQuestionId and sort by version descending
            document = self.questions_collection.find_one(
                {'oldQuestionId': old_question_id_int},
                sort=[('version', -1)]
            )
            
            if document:
                logger.info(f"Found latest version document for oldQuestionId: {old_question_id_int} "
                          f"(version: {document.get('version', 'N/A')}, _id: {document.get('_id')})")
            else:
                logger.warning(f"No document found for oldQuestionId: {old_question_id_int}")
            
            return document
        except ValueError:
            logger.error(f"Invalid oldQuestionId format (not a number): {old_question_id}")
            return None
        except Exception as e:
            logger.error(f"Error fetching document for oldQuestionId {old_question_id}: {e}")
            return None
    
    def delete_from_new_questions(self, old_question_id: str) -> int:
        """
        Delete all documents with the given oldQuestionId from 'new_questions' collection.
        
        Args:
            old_question_id: The oldQuestionId to delete (will be converted to int)
            
        Returns:
            Number of documents deleted
        """
        try:
            # Convert oldQuestionId to integer since it's stored as int64 in MongoDB
            old_question_id_int = int(old_question_id)
            
            result = self.new_questions_collection.delete_many({'oldQuestionId': old_question_id_int})
            deleted_count = result.deleted_count
            
            logger.info(f"Deleted {deleted_count} document(s) from new_questions for oldQuestionId: {old_question_id_int}")
            return deleted_count
        except ValueError:
            logger.error(f"Invalid oldQuestionId format (not a number): {old_question_id}")
            return 0
        except Exception as e:
            logger.error(f"Error deleting from new_questions for oldQuestionId {old_question_id}: {e}")
            return 0
    
    def copy_to_new_questions(self, document: dict) -> bool:
        """
        Copy the document to 'new_questions' collection.
        
        Args:
            document: The document to copy
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Remove _id to let MongoDB generate a new one
            doc_to_insert = document.copy()
            if '_id' in doc_to_insert:
                del doc_to_insert['_id']
            
            result = self.new_questions_collection.insert_one(doc_to_insert)
            logger.info(f"Copied document to new_questions with new _id: {result.inserted_id}")
            return True
        except Exception as e:
            logger.error(f"Error copying document to new_questions: {e}")
            return False
    
    def migrate_question(self, old_question_id: str) -> bool:
        """
        Migrate a single question from 'questions' to 'new_questions'.
        
        Args:
            old_question_id: The oldQuestionId to migrate
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Starting migration for oldQuestionId: {old_question_id}")
        
        # Step 1: Fetch latest version document
        document = self.get_latest_version_document(old_question_id)
        if not document:
            logger.error(f"Migration failed for oldQuestionId: {old_question_id} - Document not found")
            return False
        
        # Step 2: Delete existing documents from new_questions
        self.delete_from_new_questions(old_question_id)
        
        # Step 3: Copy document to new_questions
        success = self.copy_to_new_questions(document)
        
        if success:
            logger.info(f"Successfully migrated oldQuestionId: {old_question_id}")
        else:
            logger.error(f"Migration failed for oldQuestionId: {old_question_id}")
        
        return success
    
    def migrate_all(self, csv_file_path: str):
        """
        Migrate all questions from the CSV file.
        
        Args:
            csv_file_path: Path to the CSV file containing oldQuestionIds
        """
        logger.info("=" * 80)
        logger.info("Starting migration process")
        logger.info("=" * 80)
        
        # Read oldQuestionIds from CSV
        old_question_ids = self.read_old_question_ids_from_csv(csv_file_path)
        
        if not old_question_ids:
            logger.warning("No oldQuestionIds found in CSV file")
            return
        
        # Statistics
        total = len(old_question_ids)
        successful = 0
        failed = 0
        
        # Migrate each question
        for idx, old_question_id in enumerate(old_question_ids, 1):
            logger.info(f"Processing {idx}/{total}: {old_question_id}")
            
            if self.migrate_question(old_question_id):
                successful += 1
            else:
                failed += 1
            
            logger.info("-" * 80)
        
        # Summary
        logger.info("=" * 80)
        logger.info("Migration completed")
        logger.info(f"Total: {total}")
        logger.info(f"Successful: {successful}")
        logger.info(f"Failed: {failed}")
        logger.info("=" * 80)
    
    def close(self):
        """Close MongoDB connection."""
        self.client.close()
        logger.info("MongoDB connection closed")


def main():
    """Main function to run the migration script."""
    # Configuration - Update these values
    MONGO_URI = ""
    DATABASE_NAME = "qb"
    CSV_FILE_PATH = "oldQuestionIds.csv"
    
    try:
        # Create migrator instance
        migrator = QuestionMigrator(MONGO_URI, DATABASE_NAME)
        
        # Run migration
        migrator.migrate_all(CSV_FILE_PATH)
        
        # Close connection
        migrator.close()
        
    except Exception as e:
        logger.error(f"Migration failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

