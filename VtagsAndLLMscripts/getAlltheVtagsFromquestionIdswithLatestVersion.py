from pymongo import MongoClient
import pandas as pd

# MongoDB setup
MONGO_URI = ""
DATABASE_NAME = "qb"
COLLECTION_NAME = "questionSolutions"

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

# Load oldQuestionIds from CSV
df_ids = pd.read_csv("vTagDataOldQue.csv")
old_ids = df_ids["oldQuestionId"].dropna().astype(int).tolist()

# Output list
results = []

# Loop through each oldQuestionId
for oid in old_ids:
    # Find all docs for this oldQuestionId
    docs = list(collection.find({"oldQuestionId": oid}))
    
    if not docs:
        continue

    # Pick the doc with highest versionId (default to 0 if missing)
    latest_doc = max(docs, key=lambda d: d.get("versionId", 0))
    
    question_id = latest_doc.get("questionId", "NA")
    video_solutions = latest_doc.get("videoSolutions", [])
    
    # Handle empty or missing videoSolutions
    if not video_solutions:
        results.append({
            "oldQuestionId": oid,
            "questionId": question_id,
            "vTag": "NA",
            "vTag2": "NA"
        })
    else:
        for vs in video_solutions:
            results.append({
                "oldQuestionId": oid,
                "questionId": question_id,
                "vTag": vs.get("vTag", "NA"),
                "vTag2": vs.get("vTag2", "NA")
            })

# Convert to DataFrame and save to CSV
df_result = pd.DataFrame(results)
output_csv = "latest_video_tags.csv"
df_result.to_csv(output_csv, index=False)

print(f"Saved {len(df_result)} records to {output_csv}")
