# Custom Tag Ingestion Script

This script ingests custom tags into MongoDB QuestionDocuments based on oldQuestionId mappings from a CSV file.

## Overview

The script reads a CSV file containing `oldQuestionId` and `customTagName` columns, then updates the corresponding MongoDB documents by appending the custom tag to the `customTags` array in the QuestionDocument.

## CustomTag Structure

Each custom tag added to the document has the following structure:
```json
{
  "tag_name": "<customTagName from CSV>",
  "value": "yes",
  "tag_type": "boolean"
}
```

## Prerequisites

1. Python 3.7 or higher
2. MongoDB connection access
3. CSV file with required columns

## Installation

1. Install required dependencies:
```bash
pip install -r requirements.txt
```

## CSV Format

Your CSV file should have the following columns:

| oldQuestionId | customTagName |
|---------------|---------------|
| 12345         | CustomTag1    |
| 67890         | CustomTag2    |
| ...           | ...           |

**Example CSV (`custom_tags.csv`):**
```csv
oldQuestionId,customTagName
12345,Premium-Content
67890,Featured-Question
11111,Special-Practice
```

## Configuration

Before running the script, update the following configuration variables in `custom_tag_ingestion.py`:

```python
CSV_INPUT_PATH = 'custom_tags.csv'  # Path to your CSV file
MONGO_URI = 'mongodb://localhost:27017'  # Your MongoDB connection string
DB_NAME = 'your_db_name'  # Your database name
COLLECTION_NAME = 'questions'  # Your collection name
BATCH_SIZE = 100  # Number of records to process per batch
```

## Usage

1. Place your CSV file in the same directory as the script (or update `CSV_INPUT_PATH`)
2. Update the MongoDB connection string and database details
3. Run the script:

```bash
python custom_tag_ingestion.py
```

## Features

- ✅ **Batch Processing**: Processes records in configurable batches for better performance
- ✅ **Duplicate Prevention**: Uses `$addToSet` to prevent duplicate tags
- ✅ **Comprehensive Logging**: Logs all operations to `custom_tag_ingestion.log`
- ✅ **Error Handling**: Graceful error handling with detailed error messages
- ✅ **Progress Tracking**: Real-time progress updates during execution
- ✅ **Statistics**: Detailed statistics at the end of execution
- ✅ **Validation**: Validates CSV structure and MongoDB connection before processing

## Output

### Console Output
The script provides real-time progress updates:
```
📊 Total records to process: 1000
🔄 Processing Batch 1/10...
  Progress: 100/1000 (10.0%) - Success: 95, Already Exists: 3, Not Found: 2, Errors: 0
...
```

### Final Statistics
```
✅ CUSTOM TAG INGESTION COMPLETED
================================================================================
📊 Total records processed: 1000
✅ Successful updates: 950
ℹ️  Already exists (skipped): 30
⚠️  Documents not found: 15
❌ Errors: 5
⏱️  Total execution time: 45.23 seconds
📝 Check 'custom_tag_ingestion.log' for detailed logs
```

### Log File
Detailed logs are saved to `custom_tag_ingestion.log` including:
- Timestamp for each operation
- Success/failure status for each record
- Error messages with stack traces
- Batch processing information
- Final statistics

## MongoDB Operations

The script performs the following MongoDB operation for each record:

```python
db.questions.update_one(
    {"_id": doc_id},
    {"$addToSet": {"customTags": {
        "tag_name": "CustomTagName",
        "value": "yes",
        "tag_type": "boolean"
    }}}
)
```

**Note**: `$addToSet` ensures that duplicate tags are not added to the array.

## Error Handling

The script handles the following scenarios:
- Missing CSV file
- Invalid CSV format or missing columns
- MongoDB connection failures
- Documents not found for given oldQuestionId
- Duplicate tags (skipped automatically)
- Database update errors

## Performance Considerations

- Default batch size is 100 records
- Small delay (0.5 seconds) between batches to avoid overwhelming the database
- Can process thousands of records efficiently
- Adjust `BATCH_SIZE` based on your database capacity

## Troubleshooting

### "CSV file not found"
- Ensure the CSV file path is correct
- Check if the file exists in the specified location

### "MongoDB connection failed"
- Verify the MongoDB connection string
- Check if MongoDB is running and accessible
- Verify database and collection names

### "Missing required columns"
- Ensure your CSV has both `oldQuestionId` and `customTagName` columns
- Check for typos in column names (case-sensitive)

### "Documents not found"
- Verify that the oldQuestionId values exist in your database
- Check the log file for specific oldQuestionIds that were not found

## Safety Features

1. **Non-destructive**: Only adds tags, doesn't remove or modify existing data
2. **Duplicate prevention**: Won't add the same tag twice
3. **Dry-run capable**: Can be modified to include a dry-run mode
4. **Transaction safety**: Each update is atomic

## Related Scripts

- `customTagRemoval/customTagRemovalFromMongo.py` - Remove custom tags
- `VtagsAndLLMscripts/` - Various tag-related scripts

## Support

For issues or questions, check the log file for detailed error messages and stack traces.

