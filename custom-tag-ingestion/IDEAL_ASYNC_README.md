# Ideal Async Release - Complete Migration Script

This script performs a comprehensive migration for the "Ideal Async Release" that updates taxonomy data, adds custom tags, and locks question documents in MongoDB.

## 📋 Overview

The script processes **210 oldQuestionIds** and performs the following operations on **ALL versions** of each question:

1. **Update/Replace TaxonomyData** - Updates or adds taxonomy entries to `taxonomyData` array
2. **Add Custom Tags** - Adds custom tags to `customTags` array for ALL versions
3. **Lock Latest Version** - Sets `questionQualityStatus = 2` (LOCKED) for the latest version only
4. **Create Backups** - Automatically backs up data before making changes

## 📁 Files

- `ideal_async_complete.py` - **Main script** (complete and ready to run)
- `tax_data.py` - TAX_DATA dictionary with 210 entries
- `custom_tag_data.csv` - Source CSV file with taxonomy and custom tag data
- `generate_tax_data.py` - Script to regenerate TAX_DATA from CSV

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install pymongo
```

### 2. Run in Dry-Run Mode (Test First!)

```bash
python3 ideal_async_complete.py --dry-run
```

This will:
- Show what changes **would** be made
- **NOT modify** the database
- Display statistics and logs

### 3. Run Actual Migration

```bash
python3 ideal_async_complete.py
```

⚠️ **Warning**: This will modify the PRODUCTION database!
- 5-second countdown before execution
- Press Ctrl+C to cancel

### 4. Resume from Specific Index

If the script fails or is interrupted, resume from where it stopped:

```bash
python3 ideal_async_complete.py --resume 50
```

This will skip the first 50 entries and start from index 50.

## 📊 TAX_DATA Structure

Each entry in TAX_DATA contains:

```python
"4600965": {
    "customTags": "IA_Concept",      # Custom tag name to add
    "taxonomyId": "1734438058Qn",    # Target taxonomy ID
    "classId": "1347",               # Class ID
    "subjectId": "2390",             # Subject ID
    "topicId": "2392",               # Topic ID
    "subtopicId": "2404",            # Subtopic ID
    "conceptId": "12234"             # Concept ID
}
```

## 🔄 What the Script Does

### For Each oldQuestionId:

1. **Find ALL versions** of the question document
   ```
   Example: oldQuestionId 12345 might have v1, v2, v3
   ```

2. **Categorize documents**:
   - Documents **with** taxonomyId `1734438058Qn` → **REPLACE** taxonomy entry
   - Documents **without** taxonomyId → **ADD** new taxonomy entry

3. **Update TaxonomyData**:
   - **Replace**: Updates existing taxonomy entry in `taxonomyData` array
   - **Add**: Pushes new taxonomy entry to `taxonomyData` array

4. **Add Custom Tags** (ALL versions):
   - Checks if tag already exists (skips if yes)
   - Appends to `customTags` array
   - Structure: `{"tag_name": "IA_Concept", "value": "yes", "tag_type": "boolean"}`

5. **Lock Latest Version** (only the highest version):
   - Finds document with highest `version` number
   - Sets `questionQualityStatus = 2` (LOCKED status)

## 📝 Logging

### Dry-Run Mode
- Console output only
- No log files created

### Actual Run
- Creates `log/ideal_async_release_YYYYMMDD_HHMMSS.log`
- Both console and file logging
- Detailed operation logs for each document

## 💾 Backup

**Automatic backup is created before any changes:**
- Filename: `taxonomy_backup_YYYYMMDD_HHMMSS.json`
- Contains: Latest version data for each oldQuestionId
- Fields backed up: `_id`, `version`, `taxonomyData`, `customTags`

## 📊 Statistics

The script tracks and displays:

```
SUMMARY
======================================================================
Mode: ACTUAL RUN
Total documents found: 350               ← Total docs across all versions
Taxonomy entries replaced: 120           ← How many were replaced
Taxonomy entries added: 230              ← How many were added
Custom tags added: 340                   ← Tags added to documents
Custom tags skipped (already exists): 10 ← Tags that already existed
Questions locked (latest version): 210   ← Latest versions locked
Time elapsed: 120.50 seconds
```

## 🔍 MongoDB Operations

### 1. Replace Taxonomy (Bulk Update)
```python
collection.update_many(
    {"_id": {"$in": doc_ids}},
    {"$set": {"taxonomyData.$[elem]": taxonomy_entry}},
    array_filters=[{"elem.taxonomyId": TARGET_TAXONOMY_ID}]
)
```

### 2. Add Taxonomy (Individual Updates)
```python
# If taxonomyData is null
collection.update_one(
    {"_id": doc_id},
    {"$set": {"taxonomyData": [taxonomy_entry]}}
)

# If taxonomyData exists
collection.update_one(
    {"_id": doc_id},
    {"$push": {"taxonomyData": taxonomy_entry}}
)
```

### 3. Add Custom Tags
```python
collection.update_one(
    {"_id": doc_id},
    {"$set": {"customTags": updated_tags}}
)
```

### 4. Lock Latest Version
```python
collection.update_one(
    {"_id": latest_doc_id},
    {"$set": {"questionQualityStatus": 2}}
)
```

## ⚙️ Configuration

Edit these constants in `ideal_async_complete.py`:

```python
# MongoDB Configuration
MONGO_URI = 'your_mongodb_connection_string'
DB_NAME = 'qb'
COLLECTION_NAME = 'questions'

# Script Configuration
TARGET_TAXONOMY_ID = "1734438058Qn"      # Taxonomy ID to update/replace
BATCH_SLEEP_SECONDS = 0.5                # Delay between each oldQuestionId
CUSTOM_TAG_VALUE = "yes"                 # Default value for custom tags
CUSTOM_TAG_TYPE = "boolean"              # Default type for custom tags
LOG_DIR = "log"                          # Directory for log files
```

## 🛡️ Safety Features

1. **Dry-Run Mode**: Test without making changes
2. **Automatic Backups**: Created before any modifications
3. **Resume Support**: Continue from where you left off
4. **5-Second Warning**: Cancel before actual run
5. **Comprehensive Logging**: Track all operations
6. **Error Handling**: Continues processing even if individual updates fail

## 📈 Example Execution Flow

```bash
$ python3 ideal_async_complete.py

======================================================================
Ideal Async Release - Taxonomy, CustomTags & Lock Status Update Script
======================================================================
Mode: ACTUAL RUN
Total oldQuestionIds to process: 210
======================================================================

⚠️  WARNING: This will modify the PRODUCTION database!
Starting in 5 seconds... Press Ctrl+C to cancel

Connecting to MongoDB...
✓ Connected to MongoDB successfully

Step 1: Creating backup of latest versions...
  Backed up oldQuestionId: 4600965, version: 3
  Backed up oldQuestionId: 3652652, version: 2
  ...
✓ Backup saved to: taxonomy_backup_20241229_153045.json

Step 2: Processing updates...

[0] Processing oldQuestionId: 4600965
  Found 3 version(s)
  Will REPLACE taxonomy in 2 doc(s)
  Will ADD taxonomy to 1 doc(s)
  ✓ Replaced taxonomy in 2 doc(s)
  ✓ Added taxonomy to 1 doc(s)
  ✓ Added customTag 'IA_Concept' to 3 doc(s), skipped 0 (already exists)
  ✓ Set questionQualityStatus to LOCKED for latest version (v3)

[1] Processing oldQuestionId: 3652652
  ...

======================================================================
SUMMARY
======================================================================
Mode: ACTUAL RUN
Total documents found: 350
Taxonomy entries replaced: 120
Taxonomy entries added: 230
Custom tags added: 340
Custom tags skipped (already exists): 10
Questions locked (latest version): 210
Time elapsed: 120.50 seconds
======================================================================

✓ Updates completed successfully!
MongoDB connection closed
```

## ❌ Error Handling

If errors occur:
- Individual failures are logged but don't stop the script
- Use `--resume` to continue from the last successful point
- Check log files for detailed error messages

## 🔄 Regenerating TAX_DATA

If you need to update the `custom_tag_data.csv` and regenerate TAX_DATA:

```bash
python3 generate_tax_data.py
```

This will:
- Read `custom_tag_data.csv`
- Generate new `tax_data.py`
- Display statistics

## 📚 Related Scripts

- `custom_tag_ingestion.py` - Simpler script for only adding custom tags (no taxonomy updates)
- `ideal_async_custom_tag_ingestion.py` - Original incomplete template

## ⚠️ Important Notes

1. **ALL versions are updated** with custom tags and taxonomy data
2. **Only the latest version** is locked (questionQualityStatus = 2)
3. **Backups only save latest version** data (not all versions)
4. Use `--dry-run` first to verify changes
5. Production database - be careful!

## 🐛 Troubleshooting

### "ModuleNotFoundError: No module named 'tax_data'"
- Ensure `tax_data.py` is in the same directory
- Or regenerate using `python3 generate_tax_data.py`

### "Connection timeout"
- Check MongoDB connection string
- Verify network connectivity
- Check firewall settings

### Script stops unexpectedly
- Check the log file for errors
- Use `--resume X` to continue from index X
- Verify MongoDB is accessible

## 📞 Support

For issues or questions:
1. Check the log files in `log/` directory
2. Review backup files for data recovery
3. Test with `--dry-run` before actual execution

---

**Last Updated**: December 29, 2024
**Script Version**: 1.0
**Total Questions**: 210

