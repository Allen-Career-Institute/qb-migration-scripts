import pandas as pd
import psycopg2
import logging

# ======================
# CONFIG
# ======================
CSV_FILE = "data.csv"   # your CSV file containing oldQuestionId,vTag
LOG_FILE = "vtag_update.log"
CSV_LOG_FILE = "update_results.csv"

DB_CONFIG = {
    "host": "your_host",
    "port": "5439",  # Redshift default
    "dbname": "your_db",
    "user": "your_user",
    "password": "your_password"
}

# ======================
# SETUP LOGGER
# ======================
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ======================
# DB CONNECTION
# ======================
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# ======================
# READ CSV
# ======================
df = pd.read_csv(CSV_FILE)

# ======================
# COUNTERS & RESULTS
# ======================
total = 0
added = 0
skipped = 0
not_found = 0
results = []

# ======================
# PROCESS ROWS
# ======================
for _, row in df.iterrows():
    total += 1
    old_question_id = row["oldQuestionId"]
    new_vtag = row["vTag"]

    # First, check existing vtag
    cursor.execute(
        "SELECT vtag FROM question_pool.questions WHERE id = %s",
        (old_question_id,)
    )
    result = cursor.fetchone()

    if not result:
        not_found += 1
        logging.warning(f"ID {old_question_id} not found in DB.")
        results.append([old_question_id, "NOT_FOUND", new_vtag, None])
        continue

    current_vtag = result[0]

    if current_vtag is None or current_vtag.strip() == "":
        # Perform update
        cursor.execute(
            """
            UPDATE question_pool.questions
            SET vtag = %s
            WHERE id = %s AND (vtag IS NULL OR vtag = '')
            """,
            (new_vtag, old_question_id)
        )
        conn.commit()
        added += 1
        logging.info(f"ADDED vTag '{new_vtag}' for oldQuestionId {old_question_id}")
        results.append([old_question_id, "ADDED", new_vtag, None])
    else:
        skipped += 1
        logging.info(
            f"SKIPPED oldQuestionId {old_question_id} - already has vTag '{current_vtag}'"
        )
        results.append([old_question_id, "SKIPPED", new_vtag, current_vtag])

# ======================
# CLEANUP
# ======================
cursor.close()
conn.close()

# ======================
# SAVE CSV RESULTS
# ======================
results_df = pd.DataFrame(results, columns=["oldQuestionId", "action", "new_vTag", "existing_vTag"])
results_df.to_csv(CSV_LOG_FILE, index=False)

# ======================
# FINAL SUMMARY
# ======================
summary = (
    f"PROCESS COMPLETED\n"
    f"Total rows processed: {total}\n"
    f"Added: {added}\n"
    f"Skipped: {skipped}\n"
    f"Not Found: {not_found}\n"
    f"CSV log saved to {CSV_LOG_FILE}\n"
)
print(summary)
logging.info(summary)
