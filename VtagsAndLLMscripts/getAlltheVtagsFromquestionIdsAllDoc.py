from pymongo import MongoClient
import pandas as pd
import logging
from math import ceil

# Logging setup
logging.basicConfig(
    filename='video_tag_extraction.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# MongoDB setup
MONGO_URI = ""
DATABASE_NAME = "qb"
COLLECTION_NAME = "questionSolutions"

client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

# Load and clean oldQuestionIds
df_ids = pd.read_csv("vTagDataOldQue.csv")
old_ids = df_ids["oldQuestionId"].dropna().astype(int).tolist()

# Batch settings
BATCH_SIZE = 100
num_batches = ceil(len(old_ids) / BATCH_SIZE)

results = []

for i in range(num_batches):
    batch = old_ids[i * BATCH_SIZE: (i + 1) * BATCH_SIZE]
    logging.info(f"Processing batch {i+1}/{num_batches} - Size: {len(batch)}")

    for oid in batch:
        try:
            docs = list(collection.find({"oldQuestionId": oid}))

            if not docs:
                logging.warning(f"No documents found for oldQuestionId: {oid}")
                continue

            vtags = set()
            vtag2s = set()
            question_ids = set()

            for doc in docs:
                question_id = doc.get("questionId")
                if question_id:
                    question_ids.add(str(question_id))

                video_solutions = doc.get("videoSolutions", [])
                for vs in video_solutions:
                    vtags.add(vs.get("vTag", "NA"))
                    vtag2s.add(vs.get("vTag2", "NA"))

            # Clean "NA" if real tags exist
            if len(vtags) > 1:
                vtags.discard("NA")
            if len(vtag2s) > 1:
                vtag2s.discard("NA")

            results.append({
                "oldQuestionId": oid,
                "questionIds": ", ".join(question_ids) if question_ids else "NA",
                "vTags": ", ".join(vtags) if vtags else "NA",
                "vTag2s": ", ".join(vtag2s) if vtag2s else "NA"
            })

        except Exception as e:
            logging.error(f"Error processing oldQuestionId {oid}: {e}")

# Save final results
df_result = pd.DataFrame(results)
output_csv = "merged_video_tags.csv"
df_result.to_csv(output_csv, index=False)

logging.info(f"Completed processing {len(old_ids)} oldQuestionIds")
logging.info(f"Saved {len(df_result)} records to {output_csv}")

print(f"Saved {len(df_result)} records to {output_csv}")
