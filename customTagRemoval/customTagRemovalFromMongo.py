import pandas as pd
from pymongo import MongoClient
import logging
import time

# === CONFIGURATION ===
CSV_INPUT_PATH = 'question_ids.csv'
MONGO_URI = 'mongodb://localhost:27017'
DB_NAME = 'your_db_name'
COLLECTION_NAME = 'questions'
BATCH_SIZE = 100
LOG_FILE = 'custom_tag_removal.log'

# === LOGGING SETUP ===
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# === CONNECT TO MONGO ===
try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
except Exception as e:
    logging.error(f"MongoDB connection failed: {e}")
    raise

# === LOAD QUESTION IDs ===
df = pd.read_csv(CSV_INPUT_PATH)
question_ids = df['question_id'].astype(int).tolist()

# === PROCESS IN BATCHES ===
for i in range(0, len(question_ids), BATCH_SIZE):
    batch = question_ids[i:i + BATCH_SIZE]
    batch_number = (i // BATCH_SIZE) + 1
    logging.info(f"Processing Batch {batch_number}, Question IDs: {batch}")

    try:
        for qid in batch:
            try:
                # Optimized query to fetch only matching docs
                docs = list(collection.find({
                    "oldQuestionId": qid,
                    "customTags.tag_name": "Custom-Practice"
                }))

                if not docs:
                    logging.info(f"No documents with 'Custom-Practice' tag found for Question ID {qid}")
                    continue

                for doc in docs:
                    doc_id = doc.get("_id")

                    result = collection.update_one(
                        {"_id": doc_id},
                        {"$pull": {"customTags": {"tag_name": "Custom-Practice"}}}
                    )

                    logging.info(f"Doc ID {doc_id} (qid={qid}) updated: matched={result.matched_count}, modified={result.modified_count}")

            except Exception as doc_error:
                logging.error(f"Error processing Question ID {qid}: {doc_error}")

        time.sleep(2)

    except Exception as batch_error:
        logging.error(f"Error in Batch {batch_number}: {batch_error}")

print("✅ Processing complete. Check the log file for details.")