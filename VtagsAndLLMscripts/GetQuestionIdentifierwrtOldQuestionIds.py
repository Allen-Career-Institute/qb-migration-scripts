import pandas as pd
from pymongo import MongoClient
import logging
import time

# -------------------------
# MongoDB connection setup
# -------------------------
mongo_uri = ""
client = MongoClient(mongo_uri)
db = client["qb"]
collection = db["questions"]

# -------------------------
# Input / Output files
# -------------------------
input_csv = "QtagsIdentifier.csv"
output_csv = "question_identifier_results.csv"
log_file = "question_identifier_fetch.log"

# -------------------------
# Logging setup
# -------------------------
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -------------------------
# Processing
# -------------------------
df = pd.read_csv(input_csv)
results = []

total = len(df)
found = 0
not_found = 0
batch_size = 200

for start in range(0, total, batch_size):
    end = min(start + batch_size, total)
    batch = df.iloc[start:end]

    logging.info(f"Processing batch {start // batch_size + 1}: rows {start + 1} to {end}")

    for idx, row in batch.iterrows():
        old_qid = int(row['oldQuestionId'])  # ensure integer

        doc = collection.find_one(
            {"oldQuestionId": old_qid},
            {"questionIdentifier": 1, "version": 1, "_id": 0},
            sort=[("version", -1)]  # latest version
        )

        if doc:
            results.append({
                "oldQuestionId": old_qid,
                "questionIdentifier": doc.get("questionIdentifier"),
                "version": doc.get("version")
            })
            logging.info(f"Found: oldQuestionId={old_qid}, questionIdentifier={doc.get('questionIdentifier')}, version={doc.get('version')}")
            found += 1
        else:
            results.append({
                "oldQuestionId": old_qid,
                "questionIdentifier": None,
                "version": None
            })
            logging.warning(f"Not found: oldQuestionId={old_qid}")
            not_found += 1

    # Pause between batches
    logging.info(f"Completed batch {start // batch_size + 1}, pausing 1 second...")
    time.sleep(1)

# Save results to CSV
output_df = pd.DataFrame(results)
output_df.to_csv(output_csv, index=False)

# -------------------------
# Final summary log
# -------------------------
logging.info("----- Summary -----")
logging.info(f"Total processed: {total}")
logging.info(f"Found: {found}")
logging.info(f"Not found: {not_found}")
logging.info("-------------------")

print(f"✅ Results written to {output_csv}")
print(f"📝 Log file created: {log_file}")
