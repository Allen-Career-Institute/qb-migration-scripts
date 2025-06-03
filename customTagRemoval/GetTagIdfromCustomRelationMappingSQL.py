import pandas as pd
import mysql.connector
import time

# CONFIGURE
CSV_INPUT_PATH = 'question_ids.csv'
CSV_OUTPUT_PATH = 'question_tags_output.csv'

MYSQL_HOST = 'your-mysql-host'
MYSQL_USER = 'your-username'
MYSQL_PASSWORD = 'your-password'
MYSQL_DATABASE = 'your-database'

BATCH_SIZE = 50
SLEEP_INTERVAL = 2

# Connect to MySQL
conn = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE
)
cursor = conn.cursor(dictionary=True)

# Read question IDs
df = pd.read_csv(CSV_INPUT_PATH)
question_ids = df['question_id'].tolist()

results = []

# Process in batches
for i in range(0, len(question_ids), BATCH_SIZE):
    batch = question_ids[i:i + BATCH_SIZE]
    placeholders = ','.join(['%s'] * len(batch))

    query = f"""
    SELECT question_id, tag_id 
    FROM custom_tag_relation 
    WHERE question_id IN ({placeholders})
    """
    cursor.execute(query, batch)
    rows = cursor.fetchall()

    # Create a lookup dictionary
    found = {}
    for row in rows:
        qid = row['question_id']
        tag = row['tag_id']
        if qid in found:
            found[qid].append(str(tag))
        else:
            found[qid] = [str(tag)]

    for qid in batch:
        tag_list = found.get(qid, ['NA'])
        results.append({
            'question_id': qid,
            'tag_id': ','.join(tag_list)
        })

    print(f"Processed batch {(i // BATCH_SIZE) + 1}")
    time.sleep(SLEEP_INTERVAL)

# Save to CSV
output_df = pd.DataFrame(results)
output_df.to_csv(CSV_OUTPUT_PATH, index=False)

print(f"Done! Output saved to: {CSV_OUTPUT_PATH}")
