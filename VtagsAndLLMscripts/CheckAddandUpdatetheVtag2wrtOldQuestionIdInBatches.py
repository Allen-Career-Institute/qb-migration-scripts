import pandas as pd
from pymongo import MongoClient
import time

# MongoDB setup
client = MongoClient("mongodb://localhost:27017/")  # 🔁 Update if needed
db = client["your_db_name"]                         # 🔁 Replace with your DB name
collection = db["your_collection_name"]             # 🔁 Replace with your collection name

# Load CSV
df = pd.read_csv("input.csv")

# Setup log file
log_file = open("vtag2_update_log.txt", "w")
def log(msg):
    print(msg)
    log_file.write(msg + "\n")

# Counters
added_count = 0
updated_count = 0
skipped_count = 0
error_count = 0

# Batch processing
batch_size = 100
total_rows = len(df)

for start in range(0, total_rows, batch_size):
    end = min(start + batch_size, total_rows)
    batch = df.iloc[start:end]
    log(f"\n🔄 Processing batch {start // batch_size + 1} ({start} to {end - 1})")

    for index, row in batch.iterrows():
        try:
            old_qid = int(row["oldQuestionId"])
            new_vtag2 = row["vTag2"]

            docs = list(collection.find({"oldQuestionId": old_qid}))

            if not docs:
                log(f"[SKIP] No document found for oldQuestionId: {old_qid}")
                skipped_count += 1
                continue

            for doc in docs:
                doc_id = doc["_id"]
                video_solutions = doc.get("videoSolutions", [])

                has_correct_vtag2 = any(vs.get("vTag2") == new_vtag2 for vs in video_solutions)
                if has_correct_vtag2:
                    log(f"[SKIP] vTag2 '{new_vtag2}' already exists for oldQuestionId: {old_qid}")
                    skipped_count += 1
                    continue

                found_index = -1
                for i, vs in enumerate(video_solutions):
                    if "vTag2" in vs:
                        found_index = i
                        break

                if found_index >= 0:
                    collection.update_one(
                        {"_id": doc_id},
                        {f"$set": {f"videoSolutions.{found_index}.vTag2": new_vtag2}}
                    )
                    log(f"[UPDATE] vTag2 updated to '{new_vtag2}' for oldQuestionId: {old_qid}")
                    updated_count += 1
                else:
                    collection.update_one(
                        {"_id": doc_id},
                        {"$push": {"videoSolutions": {"vTag2": new_vtag2}}}
                    )
                    log(f"[ADD] vTag2 '{new_vtag2}' added for oldQuestionId: {old_qid}")
                    added_count += 1

        except Exception as e:
            log(f"[ERROR] Failed oldQuestionId: {row.get('oldQuestionId')}, Error: {str(e)}")
            error_count += 1

    # Wait 2 seconds after each batch
    time.sleep(2)

# Final report
log("\n=== FINAL REPORT ===")
log(f"✅ Added:   {added_count}")
log(f"🔄 Updated: {updated_count}")
log(f"⏭️ Skipped:  {skipped_count}")
log(f"❌ Errors:   {error_count}")
log_file.close()
