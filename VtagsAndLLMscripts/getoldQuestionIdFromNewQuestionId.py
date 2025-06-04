import pandas as pd
from pymongo import MongoClient


client = MongoClient("mongodb+srv://qb:EiGG1xOGtnulVkSA@learning-material-management-cluster-prod-cluster-pl-0.4dyev.mongodb.net")  # Adjust as needed
db = client["qb"]  # Replace with your actual database name
collection = db["questionSolutions"]

# File paths
input_csv_path = "questionIdtoFetchOld.csv"
output_csv_path = "question_ids_with_old.csv"


# Read the input CSV
df = pd.read_csv(input_csv_path)

# Function to fetch oldQuestionId
def get_old_question_id(qid):
    doc = collection.find_one({"questionId": qid}, {"oldQuestionId": 1})
    if doc and "oldQuestionId" in doc:
        return doc["oldQuestionId"]
    return "NA"

# Add column to DataFrame
df["oldQuestionId"] = df["questionId"].apply(get_old_question_id)

# Save to output CSV
df.to_csv(output_csv_path, index=False)
print(f"✅ Output saved to: {output_csv_path}")
