import os
import json
import time
import asyncio
import logging
import requests
import numpy as np
import pandas as pd

# Try to import psutil, but don't fail if it's not available
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logging.warning("psutil not available - memory monitoring disabled")

from tqdm import tqdm
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from opensearchpy import OpenSearch, RequestsHttpConnection
from opensearchpy.exceptions import ConnectionTimeout, TransportError

# ---------------- Configuration ---------------- #

host = 'doubt-service-opensearch.allen-stage.in'
directory_path = '/Users/pulkitsharma/Desktop/OpenSearch files/parquet files/api_backfill_data_embeddings.parquet'
username = 'admin'
password = 'Y5aT8gpe6P051vjeL67F'
index_name = 'qb_index'

chunk_size = 50   # Process 50 documents per batch
max_retries = 5
base_retry_delay = 2
num_threads = 2   # Two threads for parallel processing
concurrent_files = 1  # Process only one file at a time

# ---------------- Logging Setup ---------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("opensearch_indexing_v2.log"),
        logging.StreamHandler()
    ]
)

# Log system memory info
if PSUTIL_AVAILABLE:
    memory = psutil.virtual_memory()
    logging.info(f"System RAM: {memory.total / 1024**3:.1f}GB total, {memory.available / 1024**3:.1f}GB available")
else:
    logging.info("Memory monitoring disabled - psutil not available")

# ---------------- Global Variables ---------------- #

executor = ThreadPoolExecutor(max_workers=num_threads)
_opensearch_client = None

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

    # Log the first few documents for inspection (only for first batch)
    if not hasattr(index_document, '_logged_sample'):
        logging.info(f"Preparing to index {len(documents)} documents")
        for i, doc in enumerate(documents[:2]):  # Log first 2 documents only once
            logging.info(f"Sample Document {i+1} JSON: {json.dumps(doc, indent=2, default=str)}")
        
        if len(documents) > 2:
            logging.info(f"... and {len(documents) - 2} more documents in this batch")
        
        index_document._logged_sample = True
    else:
        logging.info(f"Indexing batch of {len(documents)} documents")

    bulk_data = []
    for doc in documents:
        # Use question_language_identifier as the document _id
        doc_id = doc.get("question_language_identifier")
        if doc_id:
            bulk_data.append({"index": {"_index": index_name, "_id": doc_id}})
        else:
            # Fallback to auto-generated ID if question_language_identifier is missing
            bulk_data.append({"index": {"_index": index_name}})
            logging.warning("Document missing question_language_identifier, using auto-generated ID")
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
                logging.warning(f"{failed_count}/{len(documents)} failed to index in {duration:.2f}s")
            else:
                logging.info(f"Indexed {len(documents)} docs in {duration:.2f}s")
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
    documents = []
    futures = []

    logging.info(f"Processing chunk with {len(chunk)} rows")
    
    for row_idx, (_, row) in enumerate(chunk.iterrows()):
        # Convert row to document
        document = {
            key: (value.tolist() if isinstance(value, np.ndarray) else value)
            for key, value in row.dropna().to_dict().items()
        }
        
        # Log the first document in detail for inspection (only once)
        if row_idx == 0 and not hasattr(process_rows, '_logged_sample'):
            logging.info(f"Sample row data types before conversion:")
            for key, value in row.dropna().to_dict().items():
                logging.info(f"  {key}: {type(value)} = {str(value)[:100]}...")
            
            logging.info(f"First document after conversion: {json.dumps(document, indent=2, default=str)}")
            process_rows._logged_sample = True
        
        documents.append(document)

        if len(documents) >= chunk_size:
            futures.append(executor.submit(index_document, documents))
            documents = []

    if documents:
        futures.append(executor.submit(index_document, documents))

    for future in futures:
        future.result()

# ---------------- File Processor ---------------- #

async def load_and_process_file(file_path, max_rows=100):
    try:
        # Read the parquet file efficiently with limited rows
        logging.info(f"Reading parquet file from: {file_path}")
        
        # Use pyarrow to read only the first few rows efficiently
        import pyarrow.parquet as pq
        
        # Read parquet file metadata first
        parquet_file = pq.ParquetFile(file_path)
        logging.info(f"Total rows in file: {parquet_file.metadata.num_rows}")
        logging.info(f"Number of row groups: {parquet_file.num_row_groups}")
        
        # For files with few row groups, use batch iterator to read limited rows
        if parquet_file.num_row_groups == 1:
            logging.info("Single row group detected - using batch iterator")
            
            # Use iter_batches to read small chunks
            batch_size = min(max_rows, 1000)  # Read in batches of 1000 or less
            logging.info(f"Reading in batches of {batch_size} rows")
            
            batches = []
            total_read = 0
            
            for batch in parquet_file.iter_batches(batch_size=batch_size):
                batch_df = batch.to_pandas()
                rows_to_take = min(len(batch_df), max_rows - total_read)
                
                if rows_to_take < len(batch_df):
                    batch_df = batch_df.head(rows_to_take)
                
                batches.append(batch_df)
                total_read += len(batch_df)
                
                logging.info(f"Read {total_read} rows so far")
                
                if total_read >= max_rows:
                    break
            
            df = pd.concat(batches, ignore_index=True)
            logging.info(f"Successfully loaded {len(df)} rows from {file_path}")
            
            # Log memory usage after loading
            if PSUTIL_AVAILABLE:
                memory = psutil.virtual_memory()
                logging.info(f"Memory after loading: {memory.percent}% used, {memory.available / 1024**3:.1f}GB available")
        else:
            # Read row groups incrementally for multi-group files
            rows_read = 0
            df_chunks = []
            
            for i in range(parquet_file.num_row_groups):
                if rows_read >= max_rows:
                    break
                    
                logging.info(f"Reading row group {i+1}/{parquet_file.num_row_groups}")
                row_group = parquet_file.read_row_group(i)
                chunk_df = row_group.to_pandas()
                
                # Take only what we need
                remaining_rows = max_rows - rows_read
                if len(chunk_df) > remaining_rows:
                    chunk_df = chunk_df.head(remaining_rows)
                
                df_chunks.append(chunk_df)
                rows_read += len(chunk_df)
                
                logging.info(f"Read {rows_read} rows so far")
            
            # Combine chunks
            if df_chunks:
                df = pd.concat(df_chunks, ignore_index=True)
                logging.info(f"Successfully loaded {len(df)} rows from {file_path}")
                
                # Log memory usage after loading
                if PSUTIL_AVAILABLE:
                    memory = psutil.virtual_memory()
                    logging.info(f"Memory after loading: {memory.percent}% used, {memory.available / 1024**3:.1f}GB available")
            else:
                logging.error("No data read from parquet file")
                return
        
    except Exception as e:
        logging.error(f"Could not read Parquet file {file_path}: {e}")
        return

    total_rows = len(df)
    logging.info(f"Processing {total_rows} rows from {file_path}")

    with tqdm(total=total_rows, desc=f"Processing {os.path.basename(file_path)}") as pbar:
        for start in range(0, total_rows, chunk_size):
            end = min(start + chunk_size, total_rows)
            chunk = df.iloc[start:end]
            
            # Monitor memory before processing
            if PSUTIL_AVAILABLE:
                memory = psutil.virtual_memory()
                if memory.percent > 80:  # If memory usage > 80%
                    logging.warning(f"High memory usage: {memory.percent}% used, {memory.available / 1024**3:.1f}GB available")
            
            await asyncio.to_thread(process_rows, chunk)
            pbar.update(len(chunk))
            
            # Force garbage collection after each chunk
            import gc
            gc.collect()

    logging.info(f"Completed processing {total_rows} rows from {file_path}")

# ---------------- Concurrency Limiter ---------------- #

async def bounded_process_file(semaphore, file_path, max_rows=100):
    async with semaphore:
        logging.info(f"Starting {file_path}")
        await load_and_process_file(file_path, max_rows)

# ---------------- Main ---------------- #

async def main():
    # Since directory_path now points to a specific file, process it directly
    file_path = directory_path
    max_rows_to_process = 50  # Process only 50 rows
    
    # Check if it's a local file
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        return
    
    if not file_path.endswith(".parquet"):
        logging.error(f"File is not a parquet file: {file_path}")
        return

    logging.info(f"Processing file: {file_path}")
    logging.info(f"Max rows to process: {max_rows_to_process}")
    
    semaphore = asyncio.Semaphore(1)  # Only processing one file
    await bounded_process_file(semaphore, file_path, max_rows_to_process)
    
    logging.info("File processing completed.")


if __name__ == "__main__":
    start = time.time()
    asyncio.run(main())
    logging.info(f"Finished all indexing in {time.time() - start:.2f} seconds")

