#!/usr/bin/env python3
"""
Python script to retry failed oldQuestionIds by reading them from api_call_failed_oldQuestionIds.txt.
This script processes the failed IDs in batches by calling the search callback API.
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
# MONGO_URI = "mongodb+srv://qb:1xWqW4GP2AzB6IEP@allen-staging-staging-cluster-pl-0.xklzc.mongodb.net"
MONGO_URI = "mongodb+srv://qb:EiGG1xOGtnulVkSA@learning-material-management-cluster-prod-cluster-pl-0.4dyev.mongodb.net"
MONGO_DB = "qb"
QUESTIONS_COLL = "questions"
SOLUTIONS_COLL = "questionSolutions"

# API Constants
API_ENDPOINT = "https://api.allen-live.in/question/v1/questions/search/callback"
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
    chunk_size: int = BATCH_SIZE_PER_WORKER
    workers: int = WORKERS_DEFAULT
    out_file: str = "retry_failed_ids_backfill.pkl"
    failed_ids_file: str = "retry_failed_oldQuestionIds.txt"
    api_endpoint: str = API_ENDPOINT
    bearer_token: str = ""  # To be set when creating config
    input_failed_ids_file: str = "api_call_failed_oldQuestionIds_v2.txt"


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
    """Main backfill orchestrator for processing failed IDs"""
    
    def __init__(self, config: BackfillConfig, old_question_ids: List[int]):
        self.config = config
        self.old_question_ids = old_question_ids
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
        total_ids = len(self.old_question_ids)
        logger.info(f"Starting backfill for {total_ids} failed oldQuestionIds")
        logger.info(f"Batch size: {self.config.chunk_size}, Workers: {self.config.workers}")
        logger.info(f"API endpoint: {self.config.api_endpoint}")
        
        # Connect to MongoDB
        if not self.mongo_connector.connect():
            logger.error("Failed to connect to MongoDB")
            return False
        
        try:
            # Calculate total batches
            self.total_batches = (total_ids + self.config.chunk_size - 1) // self.config.chunk_size
            
            logger.info(f"Total questions to process: {total_ids}")
            logger.info(f"Total batches: {self.total_batches}")
            logger.info(f"Each batch processes {self.config.chunk_size} oldQuestionIds")
            logger.info(f"Estimated time: {self.total_batches * 2} seconds (assuming 2s per batch)")
            
            # Create batches for concurrent processing
            batches = []
            for i in range(0, total_ids, self.config.chunk_size):
                batch_end = min(i + self.config.chunk_size, total_ids)
                batch_ids = self.old_question_ids[i:batch_end]
                batches.append(batch_ids)
            
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
    
    def _process_all_batches_concurrent(self, batches: List[List[int]]) -> bool:
        """Process all batches concurrently using thread pool"""
        with ThreadPoolExecutor(max_workers=self.config.workers) as executor:
            # Submit all batch tasks
            future_to_batch = {
                executor.submit(self._process_single_batch, batch_ids): batch_ids
                for batch_ids in batches
            }
            
            # Collect results
            completed_batches = 0
            try:
                for future in as_completed(future_to_batch):
                    if self.shutdown_requested:
                        logger.warning("Shutdown requested. Cancelling remaining batches...")
                        # Cancel remaining futures
                        for f in future_to_batch:
                            f.cancel()
                        return False
                    
                    batch_ids = future_to_batch[future]
                    try:
                        result = future.result()
                        completed_batches += 1
                        self.processed_batches = completed_batches
                        progress = (completed_batches / len(batches)) * 100
                        batch_range = f"{batch_ids[0]}-{batch_ids[-1]}" if len(batch_ids) > 1 else str(batch_ids[0])
                        logger.info(f"Completed batch {completed_batches}/{len(batches)} ({progress:.1f}%): oldQuestionIds {batch_range}")
                        
                        # Save progress every 100 batches
                        if completed_batches % 100 == 0:
                            self._save_progress_checkpoint()
                            
                    except Exception as e:
                        logger.error(f"Error processing batch: {e}")
                        # Log all IDs in the failed batch
                        for qid in batch_ids:
                            self.processor._log_failed_id(qid, f"Batch processing failed: {str(e)}")
                            
            except KeyboardInterrupt:
                logger.warning("KeyboardInterrupt received. Initiating graceful shutdown...")
                self.shutdown_requested = True
                return False
                
        return True
    
    def _process_single_batch(self, batch_ids: List[int]) -> bool:
        """Process a single batch of questions"""
        batch_range = f"{batch_ids[0]}-{batch_ids[-1]}" if len(batch_ids) > 1 else str(batch_ids[0])
        logger.info(f"Worker processing batch: oldQuestionIds {batch_range} (count: {len(batch_ids)})")
        
        for old_question_id in batch_ids:
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
            checkpoint_file = f"checkpoint_retry_failed_ids.pkl"
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


def read_failed_ids_from_file(file_path: str) -> List[int]:
    """Read oldQuestionIds from the failed IDs file"""
    old_question_ids = []
    
    try:
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if not line or line.startswith('#'):
                    continue
                
                try:
                    # Try to parse as integer (handle both plain IDs and ID,reason format)
                    if ',' in line:
                        # Format: oldQuestionId,reason
                        old_question_id = int(line.split(',')[0])
                    else:
                        # Format: plain oldQuestionId
                        old_question_id = int(line)
                    
                    old_question_ids.append(old_question_id)
                    
                except ValueError:
                    logger.warning(f"Skipping invalid line: {line}")
                    continue
        
        logger.info(f"Successfully read {len(old_question_ids)} oldQuestionIds from {file_path}")
        return old_question_ids
        
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        sys.exit(1)


def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print("Usage: python process_failed_ids.py <bearerToken> [inputFile]")
        print(f"  bearerToken: Authentication token for API calls")
        print(f"  inputFile: (Optional) Path to file containing failed oldQuestionIds")
        print(f"             Default: api_call_failed_oldQuestionIds_v2.txt")
        print(f"\nNote: Will use {WORKERS_DEFAULT} workers, each processing {BATCH_SIZE_PER_WORKER} oldQuestionIds")
        sys.exit(1)
    
    try:
        bearer_token = sys.argv[1]
        input_file = sys.argv[2] if len(sys.argv) > 2 else "api_call_failed_oldQuestionIds_v2.txt"
    except IndexError:
        print("Error: Invalid arguments.")
        sys.exit(1)
    
    print(f"Bearer Token provided: {'Yes' if bearer_token else 'No'}")
    print(f"Input file: {input_file}")
    print(f"Configuration: {WORKERS_DEFAULT} workers, {BATCH_SIZE_PER_WORKER} oldQuestionIds per batch")
    
    # Read failed IDs from file
    old_question_ids = read_failed_ids_from_file(input_file)
    
    if not old_question_ids:
        logger.error("No valid oldQuestionIds found in the input file")
        sys.exit(1)
    
    # Create configuration
    config = BackfillConfig(
        out_file="retry_failed_ids_backfill.pkl",
        failed_ids_file="retry_failed_oldQuestionIds.txt",
        bearer_token=bearer_token,
        input_failed_ids_file=input_file
    )
    
    # Run backfill
    backfill = OpenSearchBackfill(config, old_question_ids)
    success = backfill.run()
    
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()

