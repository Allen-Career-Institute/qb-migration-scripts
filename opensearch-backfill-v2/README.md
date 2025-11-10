# OpenSearch Backfill Python Script

This Python script is a conversion of the original Go opensearch-backfill script. Instead of directly indexing data to OpenSearch, this script processes questions and solutions from MongoDB and saves all the documents to a pickle file.

## Features

- **MongoDB Integration**: Connects to MongoDB and fetches questions and solutions
- **Concurrent Processing**: Uses ThreadPoolExecutor for parallel processing of questions
- **Pickle Output**: Saves all processed documents to a pickle file instead of indexing to OpenSearch
- **Data Transformation**: Converts MongoDB documents to OpenSearch-compatible format
- **Taxonomy Processing**: Handles complex taxonomy data structure conversion
- **Solution Integration**: Merges question and solution data appropriately

## Requirements

- Python 3.7+
- MongoDB access
- Dependencies listed in requirements.txt

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

## Usage

```bash
python main.py <startOldQuestionId> <endOldQuestionId> <bearerToken>
```

### Parameters

- `startOldQuestionId`: Starting old question ID (integer)
- `endOldQuestionId`: Ending old question ID (integer)
- `bearerToken`: Bearer token for API authentication (string)

### Example

```bash
python main.py 1 100 "your_bearer_token_here"
```

This will process questions with old question IDs from 1 to 100 using:
- **10 concurrent workers** (fixed)
- **10 oldQuestionIds per batch** (fixed)
- **API calls** for each questionId_language combination

## Output

The script generates a pickle file named `opensearch_backfill_{start_id}_{end_id}.pkl` containing all processed documents.

### Loading the pickle file

```python
import pickle

# Load the documents
with open('opensearch_backfill_1_1000.pkl', 'rb') as f:
    documents = pickle.load(f)

print(f"Total documents: {len(documents)}")
```

## Key Differences from Go Version

1. **Output Format**: Saves to pickle file instead of indexing to OpenSearch
2. **Language**: Python instead of Go
3. **Concurrency**: Uses ThreadPoolExecutor instead of goroutines
4. **Data Structures**: Uses Python dataclasses instead of Go structs
5. **Error Handling**: Python-style exception handling

## Document Structure

Each document in the pickle file contains the following fields:

- `_id`: Unique document identifier
- `old_question_id`: Original question ID
- `question_id`: New question ID
- `question`: Question text
- `options`: Answer options
- `answer`: Correct answer
- `language`: Content language
- `status`: Question status
- `difficulty_level`: Question difficulty
- `has_text_solution`: Boolean flag for text solutions
- `has_video_solution`: Boolean flag for video solutions
- `has_vtag`: Boolean flag for video tags
- `has_bot_solution`: Boolean flag for bot solutions
- Various taxonomy and metadata fields

## Configuration

The script uses the following MongoDB configuration (hardcoded):

- URI: `mongodb+srv://qb:1xWqW4GP2AzB6IEP@allen-staging-staging-cluster-pl-0.xklzc.mongodb.net`
- Database: `qb`
- Collections: `questions`, `questionSolutions`

To change these values, modify the constants at the top of `main.py`.

## Logging

The script uses Python's logging module to provide detailed information about the processing progress. Logs include:

- Connection status
- Batch processing progress
- Error messages
- Final statistics

## Testing

The conversion includes a comprehensive test suite to validate the functionality:

```bash
python3 test_conversion.py
```

This test script validates:
- Document structure conversion
- Taxonomy processing logic
- Tag formatting (custom tags and hash tags)
- Data type consistency
- Required field presence

## Utility Scripts

### Inspect Pickle File

Use the inspection utility to examine the contents of generated pickle files:

```bash
python3 inspect_pickle.py opensearch_backfill_1_1000.pkl
```

This will show:
- Total document count
- Document structure
- Language distribution
- Sample document content

## Memory Considerations

Since all documents are stored in memory before being written to the pickle file, ensure you have sufficient RAM for large datasets. For very large datasets, consider processing in smaller chunks or modifying the script to write incrementally.
