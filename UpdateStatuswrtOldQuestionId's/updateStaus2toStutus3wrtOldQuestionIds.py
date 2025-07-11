import pandas as pd
from pymongo import MongoClient
import logging
from datetime import datetime
import time

# ==== CONFIG ====
CSV_FILE = 'old_question_ids.csv'  # CSV should have a column "oldQuestionId"
LOG_FILE = f'update_status_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
MONGO_URI = ''  # replace with your actual URI
DB_NAME = 'qb'
COLLECTION_NAME = 'questions'
BATCH_SIZE = 200
SLEEP_TIME = 1  # seconds

# ==== LOGGING SETUP ====
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ==== READ CSV ====
df = pd.read_csv(CSV_FILE)

if 'oldQuestionId' not in df.columns:
    logging.error("CSV file must have a column named 'oldQuestionId'")
    exit("Missing 'oldQuestionId' column in CSV.")

# ==== CONNECT TO MONGO ====
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# ==== BATCH PROCESSING ====
total_ids = len(df)
print(f"Starting update for {total_ids} oldQuestionIds in batches of {BATCH_SIZE}...")

for batch_start in range(0, total_ids, BATCH_SIZE):
    batch_end = min(batch_start + BATCH_SIZE, total_ids)
    batch_df = df.iloc[batch_start:batch_end]

    print(f"\n🔄 Processing batch {batch_start + 1} to {batch_end}...")

    for idx, row in batch_df.iterrows():
        try:
            old_qid = int(row['oldQuestionId'])
            query = {
                "oldQuestionId": old_qid,
                "status": 2
            }
            update = {
                "$set": { "status": 3 }
            }
            result = collection.update_many(query, update)

            log_msg = f"[{idx+1}] oldQuestionId={old_qid} | Matched={result.matched_count}, Modified={result.modified_count}"
            logging.info(log_msg)
            print(log_msg)

        except Exception as e:
            error_msg = f"[{idx+1}] Error processing oldQuestionId={row.get('oldQuestionId')}: {str(e)}"
            logging.error(error_msg)
            print(error_msg)

    print(f"✅ Batch {batch_start + 1} to {batch_end} completed. Waiting {SLEEP_TIME} second...")
    time.sleep(SLEEP_TIME)

print(f"\n✅ All updates completed. Log saved to: {LOG_FILE}")
