import os
import gc
import sys
import json
import time
import asyncio
import logging
import requests
import numpy as np
import pandas as pd

from tqdm import tqdm
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from opensearchpy import OpenSearch, RequestsHttpConnection
from opensearchpy.exceptions import ConnectionTimeout, TransportError

# Try to import psutil for memory monitoring (optional)
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

# ---------------- Configuration ---------------- #

host = 'search-opensearch.allen-live.in'
directory_path = './api_backfill_data_embeddings.parquet'  # Local file in current directory
username = ''
password = ''
index_name = 'question_bank_index'

chunk_size = 500  # Process 500 documents per batch
max_retries = 5
base_retry_delay = 2
num_threads = 2   # Reduced threads to minimize memory usage
concurrent_files = 1  # Process one file at a time

# ---------------- Logging Setup ---------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("opensearch_indexing.log"),
        logging.StreamHandler()
    ]
)

# ---------------- Global Variables ---------------- #

executor = ThreadPoolExecutor(max_workers=num_threads)
_opensearch_client = None

# ---------------- Memory Monitoring ---------------- #

def log_memory_usage(context=""):
    """Log current memory usage if psutil is available"""
    if PSUTIL_AVAILABLE:
        process = psutil.Process()
        mem_info = process.memory_info()
        mem_mb = mem_info.rss / 1024 / 1024
        logging.info(f"💾 Memory usage {context}: {mem_mb:.1f} MB")
    else:
        # Fallback to basic sys info
        import sys
        logging.debug(f"Memory monitoring not available {context}")

# ---------------- OpenSearch Setup ---------------- #

def create_opensearch_connection():
    global _opensearch_client
    if _opensearch_client is None:
        _opensearch_client = OpenSearch(
            hosts=[{"host": host, "port": 443}],
            http_auth=(username, password),
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            timeout=90,
            max_retries=max_retries,
            retry_on_timeout=True,
        )
    return _opensearch_client

# ---------------- Indexing Logic ---------------- #

def index_document(documents):
    if not documents:
        return

    bulk_data = []
    for doc in documents:
        # Use question_language_identifier as the document _id
        doc_id = doc.get("question_language_identifier")
        if doc_id:
            bulk_data.append({"index": {"_index": index_name, "_id": doc_id}})
        else:
            # Fallback to auto-generated ID if question_language_identifier is missing
            bulk_data.append({"index": {"_index": index_name}})
        bulk_data.append(doc)

    client = create_opensearch_connection()

    for attempt in range(max_retries):
        try:
            start = time.time()
            response = client.bulk(body=bulk_data, request_timeout=90)
            duration = time.time() - start

            if response.get("errors"):
                failed_count = sum(
                    1 for item in response["items"]
                    if "error" in item.get("index", {})
                )
                logging.warning(f"⚠️  {failed_count}/{len(documents)} documents failed to index in {duration:.2f}s")
            else:
                logging.info(f"✅ Successfully indexed {len(documents)} documents to OpenSearch in {duration:.2f}s")
            return

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, ConnectionTimeout, TransportError) as e:
            wait_time = base_retry_delay * (2 ** attempt) + np.random.uniform(0.1, 1.0)
            logging.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time:.1f}s...")
            time.sleep(wait_time)
        except Exception as e:
            logging.error(f"Unexpected error indexing documents: {e}")
            return

    logging.error("Max retries reached. Failed to index batch.")

# ---------------- Chunk Processor ---------------- #

def process_rows(chunk):
    # Convert all rows in the chunk to documents
    documents = []
    for _, row in chunk.iterrows():
        document = {
            key: (value.tolist() if isinstance(value, np.ndarray) else value)
            for key, value in row.dropna().to_dict().items()
        }
        documents.append(document)
    
    # Index all documents in this chunk as a single batch
    if documents:
        index_document(documents)

# ---------------- File Processor ---------------- #

async def load_and_process_file(file_path):
    try:
        import pyarrow.parquet as pq
        
        # Open parquet file and get metadata without loading data
        parquet_file = pq.ParquetFile(file_path)
        total_rows = parquet_file.metadata.num_rows
        total_batches = (total_rows + chunk_size - 1) // chunk_size
        
        logging.info(f"Parquet file contains {total_rows} rows")
        logging.info(f"Will process in {total_batches} batches of {chunk_size} rows each")
        logging.info(f"Memory-efficient streaming mode enabled")
        log_memory_usage("at start")
        
        batch_num = 0
        rows_processed = 0
        
        with tqdm(total=total_rows, desc=f"Processing {os.path.basename(file_path)}") as pbar:
            # Stream the file in batches instead of loading all at once
            for batch in parquet_file.iter_batches(batch_size=chunk_size):
                batch_num += 1
                
                # Convert arrow batch to pandas dataframe
                chunk = batch.to_pandas()
                rows_in_batch = len(chunk)
                
                logging.info(f"Processing batch {batch_num}/{total_batches} (rows {rows_processed+1} to {rows_processed+rows_in_batch})")
                await asyncio.to_thread(process_rows, chunk)
                logging.info(f"Completed batch {batch_num}/{total_batches} - {rows_in_batch} rows indexed")
                
                rows_processed += rows_in_batch
                pbar.update(rows_in_batch)
                
                # Clear the chunk from memory and force garbage collection
                del chunk
                del batch
                gc.collect()
                
                # Log memory usage every 10 batches
                if batch_num % 10 == 0:
                    log_memory_usage(f"after batch {batch_num}")
        
        logging.info(f"Completed processing {file_path} - Total: {rows_processed} rows in {batch_num} batches")
        log_memory_usage("at completion")
        
    except Exception as e:
        logging.error(f"Could not read Parquet file {file_path}: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return

# ---------------- Concurrency Limiter ---------------- #

async def bounded_process_file(semaphore, file_path):
    async with semaphore:
        logging.info(f"Starting {file_path}")
        await load_and_process_file(file_path)

# ---------------- Main ---------------- #

async def main():
    # Since directory_path now points to a specific file, process it directly
    file_path = directory_path
    
    logging.info("="*80)
    logging.info("Starting OpenSearch Bulk Indexing Process")
    logging.info("="*80)
    
    # Check if it's a local file
    if not os.path.exists(file_path):
        logging.error(f"❌ File not found: {file_path}")
        return
    
    if not file_path.endswith(".parquet"):
        logging.error(f"❌ File is not a parquet file: {file_path}")
        return

    logging.info(f"📁 Processing file: {file_path}")
    logging.info(f"🎯 Index name: {index_name}")
    logging.info(f"📦 Batch size: {chunk_size} rows")
    logging.info(f"🧵 Thread pool size: {num_threads} workers")
    if PSUTIL_AVAILABLE:
        logging.info(f"✅ Memory monitoring enabled (psutil available)")
    else:
        logging.info(f"⚠️  Memory monitoring disabled (install psutil to enable)")
    logging.info("-"*80)
    
    semaphore = asyncio.Semaphore(1)  # Only processing one file
    await bounded_process_file(semaphore, file_path)
    
    logging.info("="*80)
    logging.info("✅ File processing completed successfully!")
    logging.info("="*80)


if __name__ == "__main__":
    start = time.time()
    asyncio.run(main())
    duration = time.time() - start
    logging.info(f"⏱️  Total execution time: {duration:.2f} seconds ({duration/60:.2f} minutes)")