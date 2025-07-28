import pandas as pd
import requests
import json
import time
import os

# Config
API_URL = 'https://bff.allen-stage.in/question/v1/ingest/feedback'
BEARER_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9'
CSV_PATH = 'question_ids.csv'  # CSV file must have a column: questionId
BATCH_SIZE = 100
SUCCESS_LOG = 'success_log.csv'
ERROR_LOG = 'error_log.csv'

# Ensure logs are created fresh
for file in [SUCCESS_LOG, ERROR_LOG]:
    if os.path.exists(file):
        os.remove(file)

# Load question IDs from CSV
df = pd.read_csv(CSV_PATH)
if 'questionId' not in df.columns:
    raise Exception("CSV must contain a column named 'questionId'")
question_ids = df['questionId'].dropna().astype(int).tolist()

# Batch into chunks
batches = [question_ids[i:i + BATCH_SIZE] for i in range(0, len(question_ids), BATCH_SIZE)]

# Send one batch
def send_to_api(batch):
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {BEARER_TOKEN}',
        'Content-Type': 'application/json',
    }
    payload = {"old_questionIds": batch}
    try:
        response = requests.post(API_URL, headers=headers, json=payload)
        response.raise_for_status()
        return {"success": True, "response": response.text}
    except requests.exceptions.RequestException as e:
        return {"success": False, "error": str(e)}

# Process all batches
for index, batch in enumerate(batches):
    print(f"Processing batch {index + 1}/{len(batches)} with {len(batch)} IDs...")
    result = send_to_api(batch)

    log_entry = {
        "batch_index": index + 1,
        "question_ids": json.dumps(batch),
        "timestamp": pd.Timestamp.now().isoformat(),
    }

    if result["success"]:
        log_entry["response"] = result["response"]
        pd.DataFrame([log_entry]).to_csv(SUCCESS_LOG, mode='a', header=not os.path.exists(SUCCESS_LOG), index=False)
    else:
        log_entry["error"] = result["error"]
        pd.DataFrame([log_entry]).to_csv(ERROR_LOG, mode='a', header=not os.path.exists(ERROR_LOG), index=False)

    time.sleep(1)

print("✅ All batches processed. Check success_log.csv and error_log.csv.")
