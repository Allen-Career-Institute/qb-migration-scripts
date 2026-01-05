# Question Migration Script - Passive to Active

This script migrates questions from the `questions` collection to the `new_questions` collection based on oldQuestionIds provided in a CSV file.

## Features

- Reads oldQuestionIds from a CSV file (supports comma-separated values)
- Fetches the latest version document from `questions` collection for each oldQuestionId
- Deletes all existing documents with the same oldQuestionId from `new_questions` collection
- Copies the exact document to `new_questions` collection
- Comprehensive logging with both file and console output
- Error handling and migration statistics

## Prerequisites

- Python 3.6+
- MongoDB instance accessible
- Access to both `questions` and `new_questions` collections

## Installation

1. Install required dependencies:

```bash
pip install -r requirements.txt
```

## CSV File Format

The script expects a CSV file with oldQuestionIds. The file can have:
- A header row (will be skipped)
- One or more oldQuestionIds per row
- Comma-separated values in a single cell or across multiple cells

Example CSV format:

```csv
oldQuestionId
Q12345
Q12346,Q12347
Q12348
```

Or:

```csv
oldQuestionId1,oldQuestionId2,oldQuestionId3
Q12345,Q12346,Q12347
Q12348,Q12349,Q12350
```

## Configuration

Before running the script, update the following variables in `migrate_questions.py`:

```python
MONGO_URI = "mongodb://localhost:27017/"  # Your MongoDB connection URI
DATABASE_NAME = "your_database_name"       # Your database name
CSV_FILE_PATH = "oldQuestionIds.csv"       # Path to your CSV file
```

## Usage

### Method 1: Update configuration in script and run

```bash
python migrate_questions.py
```

### Method 2: Pass arguments via command line

```bash
python migrate_questions.py <csv_file_path> [mongo_uri] [database_name]
```

Examples:

```bash
# Using default MongoDB URI and database name
python migrate_questions.py oldQuestionIds.csv

# Specifying all parameters
python migrate_questions.py oldQuestionIds.csv "mongodb://localhost:27017/" "my_database"
```

## Logging

The script creates a timestamped log file in the format `migration_YYYYMMDD_HHMMSS.log` and also outputs to console.

Log levels:
- INFO: Normal operation messages
- WARNING: Non-critical issues (e.g., document not found)
- ERROR: Critical errors during migration

## How It Works

For each oldQuestionId in the CSV file:

1. **Fetch Latest Version**: Queries the `questions` collection for documents with the given oldQuestionId and retrieves the one with the highest version number
2. **Delete Existing**: Removes all documents with the same oldQuestionId from the `new_questions` collection
3. **Copy Document**: Inserts the fetched document into the `new_questions` collection (with a new MongoDB _id)

## Output

At the end of execution, the script provides a summary:
- Total number of oldQuestionIds processed
- Number of successful migrations
- Number of failed migrations

## Error Handling

The script handles various error scenarios:
- CSV file not found
- MongoDB connection failures
- Document not found in source collection
- Insertion failures

All errors are logged with details for troubleshooting.

## Notes

- The script removes the `_id` field from the source document before inserting into `new_questions` to allow MongoDB to generate a new unique _id
- If multiple versions exist for an oldQuestionId, only the latest version (highest version number) is migrated
- All existing documents with the same oldQuestionId in `new_questions` are deleted before the new copy is inserted






