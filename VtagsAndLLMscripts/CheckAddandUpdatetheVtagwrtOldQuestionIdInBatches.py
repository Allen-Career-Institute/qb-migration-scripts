import pandas as pd
import time
from pymongo import MongoClient

# === MongoDB Setup ===
mongo_uri = ""
client = MongoClient(mongo_uri)
db = client["qb"]
collection = db["questionSolutions"]

# === Load CSV ===
df = pd.read_csv("Finaladityatuploadjeeneetpncf.csv")  # Columns: oldQuestionId,vTag

# === Log Files ===
log_file = open("vtag_update_log.txt", "w")
processed_log = open("vtag_processed.csv", "w")
processed_log.write("oldQuestionId,vTag\n")

# === Counters ===
added_count = 0
updated_count = 0
skipped_count = 0
error_count = 0

def log(msg):
    print(msg)
    log_file.write(msg + "\n")

# === Batch Processing ===
batch_size = 100

for batch_start in range(0, len(df), batch_size):
    batch = df.iloc[batch_start:batch_start+batch_size]
    log(f"\n🔄 Processing batch {batch_start // batch_size + 1} ({batch_start} to {batch_start + len(batch) - 1})")

    for _, row in batch.iterrows():
        try:
            old_qid = int(row["oldQuestionId"])
            new_vtag = str(row["vTag"]).strip()

            docs = list(collection.find({"oldQuestionId": old_qid}))
            if not docs:
                log(f"[SKIP] No document found for oldQuestionId: {old_qid}")
                skipped_count += 1
                processed_log.write(f"{old_qid},{new_vtag}\n")
                continue

            for doc in docs:
                doc_id = doc["_id"]
                video_solutions = doc.get("videoSolutions", [])

                # --- Check if vTag exists
                found_index = next((i for i, vs in enumerate(video_solutions) if "vTag" in vs), -1)
                current_vtag = video_solutions[found_index]["vTag"] if found_index != -1 else None

                if current_vtag == new_vtag:
                    log(f"[SKIP] vTag '{new_vtag}' already exists for oldQuestionId: {old_qid}")
                    skipped_count += 1
                    processed_log.write(f"{old_qid},{new_vtag}\n")
                    continue

                if found_index != -1:
                    # UPDATE existing vTag
                    collection.update_one(
                        {"_id": doc_id},
                        {"$set": {f"videoSolutions.{found_index}.vTag": new_vtag}}
                    )
                    log(f"[UPDATE] old vTag: '{current_vtag}' → new vTag: '{new_vtag}' for oldQuestionId: {old_qid}")
                    updated_count += 1
                    processed_log.write(f"{old_qid},{new_vtag}\n")

                else:
                    # Check for empty object inside videoSolutions
                    empty_index = next((i for i, vs in enumerate(video_solutions) if vs == {}), -1)

                    if empty_index >= 0:
                        collection.update_one(
                            {"_id": doc_id},
                            {"$set": {f"videoSolutions.{empty_index}": {"vTag": new_vtag}}}
                        )
                        log(f"[UPDATE] Replaced empty object with vTag '{new_vtag}' for oldQuestionId: {old_qid}")
                        updated_count += 1
                        processed_log.write(f"{old_qid},{new_vtag}\n")

                    elif not video_solutions:
                        # If videoSolutions key missing or empty
                        collection.update_one(
                            {"_id": doc_id},
                            {"$set": {"videoSolutions": [{"vTag": new_vtag}]}}
                        )
                        log(f"[ADD] vTag '{new_vtag}' added for oldQuestionId: {old_qid}")
                        added_count += 1
                        processed_log.write(f"{old_qid},{new_vtag}\n")

                    else:
                        # Append new vTag to videoSolutions
                        collection.update_one(
                            {"_id": doc_id},
                            {"$push": {"videoSolutions": {"vTag": new_vtag}}}
                        )
                        log(f"[ADD] vTag '{new_vtag}' added for oldQuestionId: {old_qid}")
                        added_count += 1
                        processed_log.write(f"{old_qid},{new_vtag}\n")

        except Exception as e:
            log(f"[ERROR] Failed oldQuestionId: {row.get('oldQuestionId')} Error: {e}")
            error_count += 1
            processed_log.write(f"{row.get('oldQuestionId')},{row.get('vTag')}\n")

    time.sleep(2)  # Pause between batches

# === Final Report ===
log("\n=== FINAL REPORT ===")
log(f"✅ Added:   {added_count}")
log(f"🔄 Updated: {updated_count}")
log(f"⏭️ Skipped: {skipped_count}")
log(f"❌ Errors:   {error_count}")

# === Close Logs ===
log_file.close()
processed_log.close()
