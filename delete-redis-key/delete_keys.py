#!/usr/bin/env python3
"""
Redis Key Deletion Script
Deletes keys matching pattern: qb.mapping.questions.<1-6200000>
Uses async workers with pipeline batching for optimal performance.
"""

import asyncio
import argparse
import logging
import os
import ssl
import time
from pathlib import Path

import redis.asyncio as redis
from tqdm import tqdm

# ============================================================================
# CONFIGURATION
# ============================================================================

REDIS_CONFIG = {
    "host": "",
    "port": 6379,
    "username": "",
    "password": "",
    "decode_responses": True,
}

# Key range configuration (defaults, can be overridden via CLI)
DEFAULT_START_ID = 1
DEFAULT_END_ID = 6_200_000
KEY_PREFIX = "qb.mapping.questions."

# Performance tuning
DEFAULT_BATCH_SIZE = 5000      # Keys per pipeline batch
DEFAULT_NUM_WORKERS = 10       # Concurrent workers
CHECKPOINT_FILE = "checkpoint.txt"

# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("deletion.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# CHECKPOINT MANAGEMENT
# ============================================================================

def load_checkpoint() -> int:
    """Load the last successfully processed batch from checkpoint file."""
    checkpoint_path = Path(CHECKPOINT_FILE)
    if checkpoint_path.exists():
        try:
            with open(checkpoint_path, "r") as f:
                last_id = int(f.read().strip())
                logger.info(f"Resuming from checkpoint: {last_id}")
                return last_id
        except (ValueError, IOError) as e:
            logger.warning(f"Could not read checkpoint: {e}")
    return START_ID - 1


def save_checkpoint(last_processed_id: int) -> None:
    """Save the last successfully processed ID to checkpoint file."""
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(last_processed_id))


def clear_checkpoint() -> None:
    """Remove checkpoint file after successful completion."""
    checkpoint_path = Path(CHECKPOINT_FILE)
    if checkpoint_path.exists():
        checkpoint_path.unlink()
        logger.info("Checkpoint file removed.")


# ============================================================================
# REDIS CONNECTION
# ============================================================================

def create_ssl_context() -> ssl.SSLContext:
    """Create SSL context for TLS connection to AWS ElastiCache."""
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = True
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    return ssl_context


async def create_redis_pool() -> redis.Redis:
    """Create an async Redis connection pool with TLS."""
    ssl_context = create_ssl_context()
    
    pool = redis.Redis(
        host=REDIS_CONFIG["host"],
        port=REDIS_CONFIG["port"],
        username=REDIS_CONFIG["username"],
        password=REDIS_CONFIG["password"],
        decode_responses=REDIS_CONFIG["decode_responses"],
        ssl=True,
        ssl_ca_certs=None,  # Use system CA certs
        max_connections=DEFAULT_NUM_WORKERS + 5,
    )
    
    # Test connection
    try:
        await pool.ping()
        logger.info("Successfully connected to Redis cluster")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise
    
    return pool


# ============================================================================
# DELETION LOGIC
# ============================================================================

async def delete_batch(
    redis_client: redis.Redis,
    start_id: int,
    end_id: int,
    dry_run: bool = False,
    pbar: tqdm = None
) -> int:
    """
    Delete a batch of keys using Redis pipeline.
    
    Args:
        redis_client: Async Redis client
        start_id: Starting ID (inclusive)
        end_id: Ending ID (inclusive)
        dry_run: If True, don't actually delete
        pbar: Progress bar to update
    
    Returns:
        Number of keys deleted
    """
    keys = [f"{KEY_PREFIX}{i}" for i in range(start_id, end_id + 1)]
    batch_size = len(keys)
    
    if dry_run:
        if pbar:
            pbar.update(batch_size)
        return batch_size
    
    try:
        # Use pipeline for batch deletion with UNLINK (async, non-blocking)
        async with redis_client.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.unlink(key)
            results = await pipe.execute()
        
        deleted_count = sum(1 for r in results if r == 1)
        
        if pbar:
            pbar.update(batch_size)
        
        return deleted_count
    
    except Exception as e:
        logger.error(f"Error deleting batch {start_id}-{end_id}: {e}")
        raise


async def worker(
    worker_id: int,
    queue: asyncio.Queue,
    redis_client: redis.Redis,
    dry_run: bool,
    pbar: tqdm,
    results: dict
) -> None:
    """
    Worker coroutine that processes batches from the queue.
    """
    while True:
        try:
            batch_info = await queue.get()
            if batch_info is None:  # Poison pill
                queue.task_done()
                break
            
            start_id, end_id = batch_info
            deleted = await delete_batch(redis_client, start_id, end_id, dry_run, pbar)
            
            results["deleted"] += deleted
            results["processed"] += (end_id - start_id + 1)
            
            # Save checkpoint periodically
            if results["processed"] % (DEFAULT_BATCH_SIZE * 10) == 0:
                save_checkpoint(end_id)
            
            queue.task_done()
            
        except Exception as e:
            logger.error(f"Worker {worker_id} error: {e}")
            results["errors"] += 1
            queue.task_done()


async def run_deletion(
    start_id: int,
    end_id: int,
    batch_size: int,
    num_workers: int,
    dry_run: bool,
    resume: bool
) -> None:
    """
    Main deletion coordinator.
    """
    # Determine starting point
    if resume:
        start_from = load_checkpoint() + 1
    else:
        start_from = start_id
    
    if start_from > end_id:
        logger.info("All keys already processed!")
        clear_checkpoint()
        return
    
    total_keys = end_id - start_from + 1
    logger.info(f"Starting deletion: {start_from:,} to {end_id:,} ({total_keys:,} keys)")
    logger.info(f"Batch size: {batch_size:,}, Workers: {num_workers}")
    
    if dry_run:
        logger.info("*** DRY RUN MODE - No keys will be deleted ***")
    
    # Create Redis connection
    redis_client = await create_redis_pool()
    
    # Create work queue and results tracker
    queue = asyncio.Queue(maxsize=num_workers * 2)
    results = {"deleted": 0, "processed": 0, "errors": 0}
    
    # Create progress bar
    pbar = tqdm(
        total=total_keys,
        desc="Deleting keys",
        unit="keys",
        unit_scale=True,
        dynamic_ncols=True
    )
    
    # Start workers
    workers = [
        asyncio.create_task(worker(i, queue, redis_client, dry_run, pbar, results))
        for i in range(num_workers)
    ]
    
    # Queue up batches
    start_time = time.time()
    
    try:
        current_id = start_from
        while current_id <= end_id:
            batch_end = min(current_id + batch_size - 1, end_id)
            await queue.put((current_id, batch_end))
            current_id = batch_end + 1
        
        # Send poison pills to stop workers
        for _ in range(num_workers):
            await queue.put(None)
        
        # Wait for all workers to complete
        await asyncio.gather(*workers)
        
    except KeyboardInterrupt:
        logger.info("\nInterrupted! Saving checkpoint...")
        save_checkpoint(start_from + results["processed"] - 1)
        raise
    
    finally:
        pbar.close()
        await redis_client.aclose()
    
    # Summary
    elapsed = time.time() - start_time
    keys_per_sec = results["processed"] / elapsed if elapsed > 0 else 0
    
    logger.info("=" * 60)
    logger.info("DELETION COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total keys processed: {results['processed']:,}")
    logger.info(f"Keys deleted: {results['deleted']:,}")
    logger.info(f"Errors: {results['errors']}")
    logger.info(f"Time elapsed: {elapsed:.2f} seconds")
    logger.info(f"Speed: {keys_per_sec:,.0f} keys/second")
    
    if not dry_run and results["errors"] == 0:
        clear_checkpoint()


# ============================================================================
# CLI INTERFACE
# ============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Delete Redis keys: qb.mapping.questions.<start> to qb.mapping.questions.<end>"
    )
    parser.add_argument(
        "--start",
        type=int,
        default=DEFAULT_START_ID,
        help=f"Starting oldQuestionId (default: {DEFAULT_START_ID})"
    )
    parser.add_argument(
        "--end",
        type=int,
        default=DEFAULT_END_ID,
        help=f"Ending oldQuestionId (default: {DEFAULT_END_ID})"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Number of keys per batch (default: {DEFAULT_BATCH_SIZE})"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_NUM_WORKERS,
        help=f"Number of concurrent workers (default: {DEFAULT_NUM_WORKERS})"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate deletion without actually deleting keys"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from last checkpoint"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Validate range
    if args.start > args.end:
        logger.error(f"Invalid range: --start ({args.start}) cannot be greater than --end ({args.end})")
        exit(1)
    
    logger.info("=" * 60)
    logger.info("REDIS KEY DELETION SCRIPT")
    logger.info("=" * 60)
    logger.info(f"Target: {KEY_PREFIX}{args.start:,} to {KEY_PREFIX}{args.end:,}")
    logger.info(f"Total keys: {args.end - args.start + 1:,}")
    logger.info(f"Redis: {REDIS_CONFIG['host']}:{REDIS_CONFIG['port']}")
    
    try:
        asyncio.run(run_deletion(
            start_id=args.start,
            end_id=args.end,
            batch_size=args.batch_size,
            num_workers=args.workers,
            dry_run=args.dry_run,
            resume=args.resume
        ))
    except KeyboardInterrupt:
        logger.info("\nScript interrupted by user.")
        exit(1)
    except Exception as e:
        logger.error(f"Script failed: {e}")
        exit(1)


if __name__ == "__main__":
    main()

