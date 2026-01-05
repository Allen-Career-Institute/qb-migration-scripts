import pandas as pd
from pymongo import MongoClient
import logging
import time
from datetime import datetime

# === CONFIGURATION ===
CSV_INPUT_PATH = 'custom_tags.csv'  # CSV should have columns: oldQuestionId, customTagName
MONGO_URI = ''
DB_NAME = 'qb'
COLLECTION_NAME = 'questions'
BATCH_SIZE = 20
LOG_FILE = 'custom_tag_ingestion.log'

# === LOGGING SETUP ===
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(console_handler)

# === STATISTICS ===
stats = {
    'total_records': 0,
    'successful_updates': 0,
    'documents_not_found': 0,
    'already_exists': 0,
    'errors': 0,
    'total_documents_updated': 0  # Total number of documents updated across all oldQuestionIds
}

def create_custom_tag(tag_name):
    """
    Creates a custom tag object with the required structure
    All fields are strings as per the QuestionDocument schema:
    - tag_name: string (from CSV)
    - value: string (always "yes")
    - tag_type: string (always "boolean")
    """
    return {
        "tag_name": str(tag_name),  # Ensure string type
        "value": "yes",             # String value
        "tag_type": "boolean"       # String value (not actual boolean)
    }

def process_record(collection, old_question_id, custom_tag_name):
    """
    Process a single record and update ALL MongoDB documents matching the oldQuestionId
    old_question_id: int64
    custom_tag_name: string
    Returns: (success, message, documents_updated_count)
    """
    try:
        # Ensure old_question_id is int (will be int64 from pandas)
        old_question_id = int(old_question_id)
        
        # Find ALL documents with this oldQuestionId
        docs = list(collection.find({"oldQuestionId": old_question_id}))
        
        if not docs:
            logging.warning(f"No documents found for oldQuestionId: {old_question_id}")
            return 'not_found', f"No documents found", 0
        
        # Track how many documents were updated
        docs_updated = 0
        docs_already_had_tag = 0
        
        # Create the custom tag
        custom_tag = create_custom_tag(custom_tag_name)
        
        # Process each document
        for doc in docs:
            doc_id = doc.get("_id")
            
            # Check if the custom tag already exists for this document
            existing_tags = doc.get("customTags", [])
            tag_exists = any(tag.get("tag_name") == custom_tag_name for tag in existing_tags)
            
            if tag_exists:
                logging.debug(f"Custom tag '{custom_tag_name}' already exists for doc_id: {doc_id}")
                docs_already_had_tag += 1
                continue
            
            # Update the document by adding the custom tag to the array
            # Using $addToSet to prevent duplicates
            result = collection.update_one(
                {"_id": doc_id},
                {"$addToSet": {"customTags": custom_tag}}
            )
            
            if result.modified_count > 0:
                docs_updated += 1
                logging.debug(f"Added custom tag '{custom_tag_name}' to doc_id: {doc_id}")
        
        # Log summary for this oldQuestionId
        total_docs = len(docs)
        if docs_updated > 0:
            logging.info(f"oldQuestionId: {old_question_id} - Updated {docs_updated}/{total_docs} documents with tag '{custom_tag_name}' (already had: {docs_already_had_tag})")
            return 'success', f"Updated {docs_updated}/{total_docs} documents", docs_updated
        else:
            logging.info(f"oldQuestionId: {old_question_id} - Tag '{custom_tag_name}' already exists in all {total_docs} document(s)")
            return 'already_exists', f"Tag already exists in all {total_docs} document(s)", 0
            
    except Exception as e:
        error_msg = f"Error processing oldQuestionId {old_question_id}: {str(e)}"
        logging.error(error_msg)
        return 'error', error_msg, 0

def main():
    start_time = datetime.now()
    logging.info("=" * 80)
    logging.info("CUSTOM TAG INGESTION SCRIPT STARTED")
    logging.info("=" * 80)
    
    # === CONNECT TO MONGO ===
    try:
        logging.info(f"Connecting to MongoDB: {MONGO_URI}")
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        # Test connection
        client.server_info()
        logging.info(f"Successfully connected to database: {DB_NAME}, collection: {COLLECTION_NAME}")
    except Exception as e:
        logging.error(f"MongoDB connection failed: {e}")
        print(f"❌ MongoDB connection failed: {e}")
        return
    
    # === LOAD CSV DATA ===
    try:
        logging.info(f"Loading CSV file: {CSV_INPUT_PATH}")
        df = pd.read_csv(CSV_INPUT_PATH)
        
        # Validate required columns
        required_columns = ['oldQuestionId', 'customTagName']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            error_msg = f"Missing required columns in CSV: {missing_columns}. Available columns: {list(df.columns)}"
            logging.error(error_msg)
            print(f"❌ {error_msg}")
            return
        
        # Remove rows with null values
        df = df.dropna(subset=required_columns)
        
        # Convert oldQuestionId to int64 (to match MongoDB schema)
        df['oldQuestionId'] = df['oldQuestionId'].astype('int64')
        
        # Ensure customTagName is string
        df['customTagName'] = df['customTagName'].astype(str)
        
        stats['total_records'] = len(df)
        logging.info(f"Loaded {stats['total_records']} records from CSV")
        print(f"📊 Total records to process: {stats['total_records']}")
        
    except FileNotFoundError:
        error_msg = f"CSV file not found: {CSV_INPUT_PATH}"
        logging.error(error_msg)
        print(f"❌ {error_msg}")
        return
    except Exception as e:
        error_msg = f"Error loading CSV: {e}"
        logging.error(error_msg)
        print(f"❌ {error_msg}")
        return
    
    # === PROCESS RECORDS IN BATCHES ===
    try:
        for i in range(0, len(df), BATCH_SIZE):
            batch = df.iloc[i:i + BATCH_SIZE]
            batch_number = (i // BATCH_SIZE) + 1
            total_batches = (len(df) + BATCH_SIZE - 1) // BATCH_SIZE
            
            logging.info("-" * 80)
            logging.info(f"Processing Batch {batch_number}/{total_batches} (Records {i+1} to {min(i+BATCH_SIZE, len(df))})")
            print(f"\n🔄 Processing Batch {batch_number}/{total_batches}...")
            
            batch_start_time = time.time()
            
            for idx, row in batch.iterrows():
                old_question_id = row['oldQuestionId']
                custom_tag_name = row['customTagName']
                
                status, message, docs_updated_count = process_record(collection, old_question_id, custom_tag_name)
                
                if status == 'success':
                    stats['successful_updates'] += 1
                    stats['total_documents_updated'] += docs_updated_count
                elif status == 'not_found':
                    stats['documents_not_found'] += 1
                elif status == 'already_exists':
                    stats['already_exists'] += 1
                elif status == 'error':
                    stats['errors'] += 1
            
            batch_time = time.time() - batch_start_time
            logging.info(f"Batch {batch_number} completed in {batch_time:.2f} seconds")
            
            # Progress update
            processed = min(i + BATCH_SIZE, len(df))
            progress = (processed / len(df)) * 100
            print(f"  Progress: {processed}/{len(df)} ({progress:.1f}%) - "
                  f"Success: {stats['successful_updates']}, "
                  f"Docs Updated: {stats['total_documents_updated']}, "
                  f"Already Exists: {stats['already_exists']}, "
                  f"Not Found: {stats['documents_not_found']}, "
                  f"Errors: {stats['errors']}")
            
            # Delay between batches to avoid overwhelming the database
            if i + BATCH_SIZE < len(df):
                time.sleep(1)
    
    except Exception as e:
        logging.error(f"Error during batch processing: {e}")
        print(f"❌ Error during processing: {e}")
    
    finally:
        # Close MongoDB connection
        client.close()
        logging.info("MongoDB connection closed")
    
    # === FINAL STATISTICS ===
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logging.info("=" * 80)
    logging.info("FINAL STATISTICS")
    logging.info("=" * 80)
    logging.info(f"Total oldQuestionIds processed: {stats['total_records']}")
    logging.info(f"Successful oldQuestionIds: {stats['successful_updates']}")
    logging.info(f"Total documents updated: {stats['total_documents_updated']}")
    logging.info(f"Already exists (skipped): {stats['already_exists']}")
    logging.info(f"OldQuestionIds not found: {stats['documents_not_found']}")
    logging.info(f"Errors: {stats['errors']}")
    logging.info(f"Total execution time: {duration:.2f} seconds")
    logging.info("=" * 80)
    
    print("\n" + "=" * 80)
    print("✅ CUSTOM TAG INGESTION COMPLETED")
    print("=" * 80)
    print(f"📊 Total oldQuestionIds processed: {stats['total_records']}")
    print(f"✅ Successful oldQuestionIds: {stats['successful_updates']}")
    print(f"📄 Total documents updated: {stats['total_documents_updated']}")
    print(f"ℹ️  Already exists (skipped): {stats['already_exists']}")
    print(f"⚠️  OldQuestionIds not found: {stats['documents_not_found']}")
    print(f"❌ Errors: {stats['errors']}")
    print(f"⏱️  Total execution time: {duration:.2f} seconds")
    print(f"📝 Check '{LOG_FILE}' for detailed logs")
    print("=" * 80)

if __name__ == "__main__":
    main()

