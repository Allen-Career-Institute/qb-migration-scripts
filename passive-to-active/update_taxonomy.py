#!/usr/bin/env python3
"""
Script to update taxonomyData for questions with taxonomyId "1734438058Qn"
- Replaces existing entry if found
- Adds new entry if not found
"""

import pymongo
import json
from datetime import datetime
from bson import json_util

# MongoDB connection details
MONGO_URI = ""
DB_NAME = "qb"
COLLECTION_NAME = "questions"

# Taxonomy data to update (all IDs as strings)
TAX_DATA = {
  "4514544": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12090"
  },
  "4514809": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12090"
  },
  "5130703": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12090"
  },
  "3591742": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12090"
  },
  "3592719": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12090"
  },
  "3611418": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12090"
  },
  "3720725": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12090"
  },
  "4513571": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12090"
  },
  "4513614": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12090"
  },
  "4513636": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12090"
  },
  "4513760": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12090"
  },
  "1301125": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12090"
  },
  "2124065": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12090"
  },
  "2173561": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12090"
  },
  "2684765": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12090"
  },
  "1362364": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12091"
  },
  "2755505": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12091"
  },
  "3749749": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12091"
  },
  "3795432": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12091"
  },
  "1250846": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12091"
  },
  "1311683": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12091"
  },
  "1250802": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12091"
  },
  "2665728": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12091"
  },
  "1250241": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12091"
  },
  "2755357": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12091"
  },
  "2767406": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12091"
  },
  "1250706": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12092"
  },
  "3757109": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12092"
  },
  "3795512": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12092"
  },
  "1966775": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12092"
  },
  "1250153": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12092"
  },
  "1250783": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12092"
  },
  "1311568": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12092"
  },
  "5087066": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12092"
  },
  "3795417": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12092"
  },
  "3631256": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12092"
  },
  "455404": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12095"
  },
  "455565": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12095"
  },
  "1373873": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12095"
  },
  "455554": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12095"
  },
  "397730": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12095"
  },
  "5250769": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12095"
  },
  "2196919": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553",
    "conceptId": "12095"
  },
  "1313062": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555",
    "conceptId": "12097"
  },
  "2755346": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555",
    "conceptId": "12097"
  },
  "2907223": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555",
    "conceptId": "12097"
  },
  "4598421": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555",
    "conceptId": "12097"
  },
  "1220678": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555",
    "conceptId": "12097"
  },
  "1966783": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555",
    "conceptId": "12097"
  },
  "1220674": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555",
    "conceptId": "12097"
  },
  "4598840": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555",
    "conceptId": "12097"
  },
  "2066632": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555",
    "conceptId": "12097"
  },
  "2196925": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555",
    "conceptId": "12097"
  },
  "397387": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555",
    "conceptId": "12099"
  },
  "397558": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555",
    "conceptId": "12099"
  },
  "777429": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555",
    "conceptId": "12099"
  },
  "785380": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555",
    "conceptId": "12099"
  },
  "901700": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555",
    "conceptId": "12099"
  },
  "937525": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555",
    "conceptId": "12099"
  },
  "938106": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555",
    "conceptId": "12099"
  },
  "4599008": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555",
    "conceptId": "12099"
  },
  "4646327": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555",
    "conceptId": "12099"
  },
  "1258951": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555",
    "conceptId": "12099"
  },
  "2155839": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555",
    "conceptId": "12099"
  },
  "2678507": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555",
    "conceptId": "12099"
  },
  "4599067": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553"
  },
  "4599334": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553"
  },
  "4599357": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553"
  },
  "4599369": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553"
  },
  "4602725": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553"
  },
  "4604116": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553"
  },
  "4604146": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553"
  },
  "4935078": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553"
  },
  "397414": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553"
  },
  "397564": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553"
  },
  "1900625": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553"
  },
  "1900705": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553"
  },
  "4676212": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553"
  },
  "1317612": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553"
  },
  "3274832": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553"
  },
  "4598980": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1553"
  },
  "1464375": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555"
  },
  "1402440": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555"
  },
  "5135184": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555"
  },
  "4599511": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555"
  },
  "4601475": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555"
  },
  "4601839": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555"
  },
  "4601875": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555"
  },
  "4601880": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555"
  },
  "4601897": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555"
  },
  "4601902": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555"
  },
  "4601946": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555"
  },
  "4601952": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555"
  },
  "1383081": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555"
  },
  "1502529": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555"
  },
  "1502533": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555"
  },
  "772040": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555"
  },
  "3577137": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555"
  },
  "4149822": {
    "taxonomyId": "1734438058Qn",
    "classId": "1347",
    "subjectId": "1532",
    "topicId": "1541",
    "subtopicId": "1555"
  }
}

TARGET_TAXONOMY_ID = "1734438058Qn"
DRY_RUN = False  # Set to False to actually update

def main():
    print(f"{'='*80}")
    print(f"Taxonomy Update Script - {'DRY RUN MODE' if DRY_RUN else 'LIVE MODE'}")
    print(f"{'='*80}\n")
    
    # Connect to MongoDB
    print("Connecting to MongoDB...")
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    print("Connected successfully!\n")
    
    # Step 1: Take backup of latest version for each question
    print("Step 1: Creating backup of latest version taxonomyData...")
    backup_data = []
    
    for old_question_id in TAX_DATA.keys():
        # Find latest version for this oldQuestionId
        latest_doc = collection.find_one(
            {"oldQuestionId": int(old_question_id)},
            sort=[("version", pymongo.DESCENDING)]
        )
        
        if latest_doc:
            backup_entry = {
                "_id": str(latest_doc["_id"]),
                "oldQuestionId": old_question_id,
                "version": latest_doc.get("version"),
                "taxonomyData": latest_doc.get("taxonomyData", [])
            }
            backup_data.append(backup_entry)
            print(f"  Backed up oldQuestionId: {old_question_id}, version: {latest_doc.get('version')}")
    
    # Save backup to file
    backup_filename = f"taxonomy_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(backup_filename, 'w') as f:
        json.dump(backup_data, f, indent=2, default=json_util.default)
    print(f"\n✓ Backup saved to: {backup_filename}\n")
    
    # Step 2: Update documents
    print("Step 2: Processing updates...")
    total_documents_found = 0
    total_documents_replaced = 0
    total_documents_added = 0
    
    for old_question_id, new_tax_data in TAX_DATA.items():
        print(f"\n--- Processing oldQuestionId: {old_question_id} ---")
        
        # Find ALL versions with this oldQuestionId
        all_docs = list(collection.find({"oldQuestionId": int(old_question_id)}))
        total_documents_found += len(all_docs)
        
        if not all_docs:
            print(f"  No documents found")
            continue
            
        print(f"Found {len(all_docs)} version(s)")
        
        # Separate documents into those with and without the target taxonomyId
        docs_with_taxonomy = []
        docs_without_taxonomy = []
        
        for doc in all_docs:
            taxonomy_data = doc.get("taxonomyData") or []
            has_taxonomy = any(tax.get("taxonomyId") == TARGET_TAXONOMY_ID for tax in taxonomy_data)
            
            if has_taxonomy:
                docs_with_taxonomy.append(doc["_id"])
            else:
                docs_without_taxonomy.append(doc["_id"])
        
        # Show sample of what will be updated/added
        if docs_with_taxonomy:
            sample_doc = collection.find_one({"_id": docs_with_taxonomy[0]})
            taxonomy_data = sample_doc.get("taxonomyData") or []
            old_value = next((tax for tax in taxonomy_data if tax.get("taxonomyId") == TARGET_TAXONOMY_ID), None)
            
            print(f"\n  REPLACE in {len(docs_with_taxonomy)} document(s):")
            print(f"    Sample - Document ID: {docs_with_taxonomy[0]}")
            print(f"    OLD: {old_value}")
            print(f"    NEW: {new_tax_data}")
        
        if docs_without_taxonomy:
            print(f"\n  ADD to {len(docs_without_taxonomy)} document(s):")
            print(f"    Sample - Document ID: {docs_without_taxonomy[0]}")
            print(f"    NEW: {new_tax_data}")
        
        if not DRY_RUN:
            # Replace existing entries using array filters
            if docs_with_taxonomy:
                result = collection.update_many(
                    {"_id": {"$in": docs_with_taxonomy}},
                    {"$set": {"taxonomyData.$[elem]": new_tax_data}},
                    array_filters=[{"elem.taxonomyId": TARGET_TAXONOMY_ID}]
                )
                print(f"\n  ✓ Replaced in {result.modified_count} document(s)")
                total_documents_replaced += result.modified_count
            
            # Add to documents that don't have it
            if docs_without_taxonomy:
                # First, handle documents with null taxonomyData by initializing as array
                for doc_id in docs_without_taxonomy:
                    doc = collection.find_one({"_id": doc_id})
                    if doc.get("taxonomyData") is None:
                        # Initialize taxonomyData as array with new entry
                        collection.update_one(
                            {"_id": doc_id},
                            {"$set": {"taxonomyData": [new_tax_data]}}
                        )
                    else:
                        # Push to existing array
                        collection.update_one(
                            {"_id": doc_id},
                            {"$push": {"taxonomyData": new_tax_data}}
                        )
                print(f"  ✓ Added to {len(docs_without_taxonomy)} document(s)")
                total_documents_added += len(docs_without_taxonomy)
        else:
            print(f"\n  [DRY RUN] Would replace in {len(docs_with_taxonomy)} and add to {len(docs_without_taxonomy)} document(s)")
            total_documents_replaced += len(docs_with_taxonomy)
            total_documents_added += len(docs_without_taxonomy)
    
    # Summary
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    print(f"Total documents found: {total_documents_found}")
    print(f"Documents {'that would be' if DRY_RUN else ''} replaced: {total_documents_replaced}")
    print(f"Documents {'that would have' if DRY_RUN else ''} added: {total_documents_added}")
    print(f"Mode: {'DRY RUN (no actual changes)' if DRY_RUN else 'LIVE (changes applied)'}")
    print(f"{'='*80}\n")
    
    if DRY_RUN:
        print("⚠️  This was a DRY RUN. Set DRY_RUN = False to apply changes.")
    else:
        print("✓ Updates completed successfully!")
    
    client.close()

if __name__ == "__main__":
    main()
