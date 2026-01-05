#!/usr/bin/env python3
"""
Python script to call search callback API for questions and save responses to pickle file.
Modified version that calls the search callback API instead of transforming data locally.
"""

import sys
import pickle
import logging
import requests
import json
import os
import signal
from datetime import datetime
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from pymongo import MongoClient
from pymongo.errors import PyMongoError


# Constants
MONGO_URI = ""
MONGO_DB = "qb"
QUESTIONS_COLL = "questions"
SOLUTIONS_COLL = "questionSolutions"

# API Constants
API_ENDPOINT = ""
CLIENT_ID = "qb_client"
ENTITY_TYPE = "questions"

WORKERS_DEFAULT = 20
BATCH_SIZE_PER_WORKER = 20

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class BackfillConfig:
    mongo_uri: str = MONGO_URI
    mongo_db: str = MONGO_DB
    questions_col: str = QUESTIONS_COLL
    solutions_col: str = SOLUTIONS_COLL
    start_id: int = 0
    end_id: int = 0
    chunk_size: int = BATCH_SIZE_PER_WORKER
    workers: int = WORKERS_DEFAULT
    out_file: str = "opensearch_backfill_data.pkl"
    failed_ids_file: str = "failed_oldQuestionIds.txt"
    api_endpoint: str = API_ENDPOINT
    bearer_token: str = ""  # To be set when creating config


class MongoConnector:
    """MongoDB connection handler"""
    
    def __init__(self, uri: str, database: str):
        self.uri = uri
        self.database = database
        self.client = None
        self.db = None
        self.questions_collection = None
        self.solutions_collection = None
    
    def connect(self):
        """Establish MongoDB connection"""
        try:
            self.client = MongoClient(self.uri)
            self.db = self.client[self.database]
            self.questions_collection = self.db[QUESTIONS_COLL]
            self.solutions_collection = self.db[SOLUTIONS_COLL]
            logger.info("MongoDB connection established successfully")
            return True
        except PyMongoError as e:
            logger.error(f"MongoDB connection error: {e}")
            return False
    
    def disconnect(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")


class DocumentProcessor:
    """Processes questions by calling search callback API"""
    
    def __init__(self, mongo_connector: MongoConnector, api_endpoint: str, bearer_token: str, failed_ids_file: str):
        self.mongo = mongo_connector
        self.api_endpoint = api_endpoint
        self.bearer_token = bearer_token
        self.failed_ids_file = failed_ids_file
        self.results_lock = threading.Lock()
        self.failed_ids_lock = threading.Lock()
        self.all_documents = []
        self.failed_old_question_ids = set()
    
    def process_question(self, old_question_id: int) -> List[Dict[str, Any]]:
        """Process a single question by calling the search callback API"""
        try:
            # Find the latest version of the question (only fetch questionId and content)
            filter_query = {"oldQuestionId": old_question_id}
            question_doc = self.mongo.questions_collection.find_one(
                filter_query,
                sort=[("version", -1)],
                projection={"questionId": 1, "content.language": 1}
            )
            
            if not question_doc:
                logger.warning(f"No question found for oldQuestionId: {old_question_id}")
                self._log_failed_id(old_question_id, "No question document found")
                return []
            
            question_id = question_doc.get("questionId", "")
            if not question_id:
                logger.warning(f"No questionId found for oldQuestionId: {old_question_id}")
                self._log_failed_id(old_question_id, "No questionId found")
                return []
            
            # Extract all languages from content
            content = question_doc.get("content", [])
            languages = set()
            for content_item in content:
                lang = content_item.get("language")
                if lang is not None:
                    languages.add(lang)
            
            if not languages:
                logger.warning(f"No languages found for oldQuestionId: {old_question_id}")
                self._log_failed_id(old_question_id, "No languages found in content")
                return []
            
            # Make API calls for each language
            api_responses = []
            api_call_failed = False
            for language in languages:
                unique_identifier = f"{question_id}_{language}"
                response_data = self._call_search_callback_api(unique_identifier)
                if response_data:
                    # Use raw API response without adding metadata
                    api_responses.append(response_data)
                else:
                    api_call_failed = True
            
            # Log if any API calls failed
            if api_call_failed:
                self._log_failed_id(old_question_id, f"API call failed for some/all languages: {list(languages)}")
            
            logger.info(f"Processed oldQuestionId {old_question_id}: {len(api_responses)} API responses")
            return api_responses
            
        except Exception as e:
            logger.error(f"Error processing question {old_question_id}: {e}")
            self._log_failed_id(old_question_id, f"Exception: {str(e)}")
            return []
    
    def _call_search_callback_api(self, unique_identifier: str) -> Optional[Dict[str, Any]]:
        """Call the search callback API for a unique identifier"""
        try:
            headers = {
                "Authorization": f"Bearer {self.bearer_token}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "client_id": CLIENT_ID,
                "entity_type": ENTITY_TYPE,
                "unique_identifier": unique_identifier
            }
            
            logger.debug(f"Making API call for unique_identifier: {unique_identifier}")
            
            response = requests.post(
                self.api_endpoint,
                headers=headers,
                json=payload,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.debug(f"API call successful for unique_identifier: {unique_identifier}")
                return response.json()
            else:
                logger.error(f"API call failed for {unique_identifier}: {response.status_code} - {response.text}")
                return None
                
        except requests.RequestException as e:
            logger.error(f"API request error for {unique_identifier}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error calling API for {unique_identifier}: {e}")
            return None
    
    def add_documents(self, documents: List[Dict[str, Any]]):
        """Thread-safe method to add documents to the results list"""
        if documents:
            with self.results_lock:
                self.all_documents.extend(documents)
    
    def _log_failed_id(self, old_question_id: int, reason: str):
        """Thread-safe method to log failed oldQuestionId"""
        with self.failed_ids_lock:
            self.failed_old_question_ids.add((old_question_id, reason))
            logger.error(f"Failed oldQuestionId {old_question_id}: {reason}")
    
    def save_failed_ids(self):
        """Save failed oldQuestionIds to file"""
        try:
            with open(self.failed_ids_file, 'w') as f:
                f.write(f"# Failed oldQuestionIds - Generated on {datetime.now().isoformat()}\n")
                f.write("# Format: oldQuestionId,reason\n\n")
                for old_question_id, reason in sorted(self.failed_old_question_ids):
                    f.write(f"{old_question_id},{reason}\n")
            
            failed_count = len(self.failed_old_question_ids)
            if failed_count > 0:
                logger.warning(f"Saved {failed_count} failed oldQuestionIds to {self.failed_ids_file}")
            else:
                logger.info("No failed oldQuestionIds to save")
                
        except Exception as e:
            logger.error(f"Error saving failed IDs to file: {e}")


class OpenSearchBackfill:
    """Main backfill orchestrator"""
    
    def __init__(self, config: BackfillConfig):
        self.config = config
        self.mongo_connector = MongoConnector(config.mongo_uri, config.mongo_db)
        self.processor = DocumentProcessor(
            self.mongo_connector, 
            config.api_endpoint, 
            config.bearer_token,
            config.failed_ids_file
        )
        self.shutdown_requested = False
        self.processed_batches = 0
        self.total_batches = 0
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.warning(f"Received signal {signum}. Initiating graceful shutdown...")
        logger.warning("Finishing current batches and saving progress...")
        self.shutdown_requested = True
    
    def run(self):
        """Execute the backfill process"""
        logger.info(f"Starting backfill from {self.config.start_id} to {self.config.end_id}")
        logger.info(f"Batch size: {self.config.chunk_size}, Workers: {self.config.workers}")
        logger.info(f"API endpoint: {self.config.api_endpoint}")
        
        # Connect to MongoDB
        if not self.mongo_connector.connect():
            logger.error("Failed to connect to MongoDB")
            return False
        
        try:
            # Calculate total batches and distribute work
            total_questions = self.config.end_id - self.config.start_id + 1
            self.total_batches = (total_questions + self.config.chunk_size - 1) // self.config.chunk_size
            
            logger.info(f"Total questions: {total_questions}")
            logger.info(f"Total batches: {self.total_batches}")
            logger.info(f"Each batch processes {self.config.chunk_size} oldQuestionIds")
            logger.info(f"Estimated time: {self.total_batches * 2} seconds (assuming 2s per batch)")
            
            # Create batches for concurrent processing
            batches = []
            for i in range(self.config.start_id, self.config.end_id + 1, self.config.chunk_size):
                batch_start = i
                batch_end = min(i + self.config.chunk_size - 1, self.config.end_id)
                batches.append((batch_start, batch_end))
            
            logger.info(f"Created {len(batches)} batches for {self.config.workers} workers")
            
            # Process all batches concurrently
            if not self._process_all_batches_concurrent(batches):
                logger.warning("Processing was interrupted. Saving partial results...")
                return False
            
            # Save all documents to pickle file
            self._save_to_pickle()
            
            # Save failed IDs to file
            self.processor.save_failed_ids()
            
            logger.info("Data backfill completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"Error during backfill: {e}")
            return False
        finally:
            self.mongo_connector.disconnect()
    
    def _process_all_batches_concurrent(self, batches: List[tuple]) -> bool:
        """Process all batches concurrently using thread pool"""
        with ThreadPoolExecutor(max_workers=self.config.workers) as executor:
            # Submit all batch tasks
            future_to_batch = {
                executor.submit(self._process_single_batch, batch_start, batch_end): (batch_start, batch_end)
                for batch_start, batch_end in batches
            }
            
            # Collect results
            completed_batches = 0
            try:
                for future in as_completed(future_to_batch, timeout=1.0):
                    if self.shutdown_requested:
                        logger.warning("Shutdown requested. Cancelling remaining batches...")
                        # Cancel remaining futures
                        for f in future_to_batch:
                            f.cancel()
                        return False
                    
                    batch_start, batch_end = future_to_batch[future]
                    try:
                        result = future.result()
                        completed_batches += 1
                        self.processed_batches = completed_batches
                        progress = (completed_batches / len(batches)) * 100
                        logger.info(f"Completed batch {completed_batches}/{len(batches)} ({progress:.1f}%): oldQuestionIds {batch_start}-{batch_end}")
                        
                        # Save progress every 100 batches
                        if completed_batches % 100 == 0:
                            self._save_progress_checkpoint()
                            
                    except Exception as e:
                        logger.error(f"Error processing batch {batch_start}-{batch_end}: {e}")
                        # Log all IDs in the failed batch
                        for qid in range(batch_start, batch_end + 1):
                            self.processor._log_failed_id(qid, f"Batch processing failed: {str(e)}")
                            
            except KeyboardInterrupt:
                logger.warning("KeyboardInterrupt received. Initiating graceful shutdown...")
                self.shutdown_requested = True
                return False
                
        return True
    
    def _process_single_batch(self, start_id: int, end_id: int) -> bool:
        """Process a single batch of questions"""
        logger.info(f"Worker processing batch: oldQuestionIds {start_id}-{end_id}")
        
        for old_question_id in range(start_id, end_id + 1):
            try:
                documents = self.processor.process_question(old_question_id)
                self.processor.add_documents(documents)
            except Exception as e:
                logger.error(f"Error processing question {old_question_id}: {e}")
                self.processor._log_failed_id(old_question_id, f"Processing error: {str(e)}")
        
        return True
    
    def _save_progress_checkpoint(self):
        """Save progress checkpoint"""
        try:
            checkpoint_file = f"checkpoint_{self.config.start_id}_{self.config.end_id}.pkl"
            checkpoint_data = {
                'processed_batches': self.processed_batches,
                'total_batches': self.total_batches,
                'documents_count': len(self.processor.all_documents),
                'failed_ids_count': len(self.processor.failed_old_question_ids),
                'timestamp': datetime.now().isoformat()
            }
            
            with open(checkpoint_file, 'wb') as f:
                pickle.dump(checkpoint_data, f)
            
            logger.info(f"Progress checkpoint saved: {self.processed_batches}/{self.total_batches} batches completed")
            
        except Exception as e:
            logger.error(f"Error saving progress checkpoint: {e}")
    
    def _save_to_pickle(self):
        """Save all documents to pickle file"""
        try:
            with open(self.config.out_file, 'wb') as f:
                pickle.dump(self.processor.all_documents, f, protocol=pickle.HIGHEST_PROTOCOL)
            
            total_docs = len(self.processor.all_documents)
            logger.info(f"Successfully saved {total_docs} documents to {self.config.out_file}")
            
        except Exception as e:
            logger.error(f"Error saving to pickle file: {e}")
            raise


def main():
    """Main entry point"""
    if len(sys.argv) < 4:
        print("Usage: python main.py <startOldQuestionId> <endOldQuestionId> <bearerToken>")
        print(f"Note: Will use {WORKERS_DEFAULT} workers, each processing {BATCH_SIZE_PER_WORKER} oldQuestionIds")
        sys.exit(1)
    
    try:
        start_id = int(sys.argv[1])
        end_id = int(sys.argv[2])
        bearer_token = sys.argv[3]
    except (ValueError, IndexError):
        print("Error: Invalid arguments. Please provide integers for start/end IDs and a valid bearer token.")
        sys.exit(1)
    
    print(f"Start OldQuestionID Parameter 1: {start_id}")
    print(f"End OldQuestionID Parameter 2: {end_id}")
    print(f"Bearer Token provided: {'Yes' if bearer_token else 'No'}")
    print(f"Configuration: {WORKERS_DEFAULT} workers, {BATCH_SIZE_PER_WORKER} oldQuestionIds per batch")
    
    # Create configuration
    config = BackfillConfig(
        start_id=start_id,
        end_id=end_id,
        out_file=f"api_backfill_{start_id}_{end_id}.pkl",
        failed_ids_file=f"failed_oldQuestionIds_{start_id}_{end_id}.txt",
        bearer_token=bearer_token
    )
    
    # Run backfill
    backfill = OpenSearchBackfill(config)
    success = backfill.run()
    
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()