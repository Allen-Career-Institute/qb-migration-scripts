"""
Script to update taxonomyData, add customTags, and lock questions based on oldQuestionId.
- Updates taxonomyData: Replaces existing entry if found, adds new entry if not found (ALL versions)
- Adds customTags: Adds a custom tag entry to the customTags array (ALL versions)
- Locks question: Sets questionQualityStatus to 2 (LOCKED) for LATEST version only

Usage:
    python ideal_async_release.py --dry-run [--resume INDEX]
    python ideal_async_release.py [--resume INDEX]
"""

import sys
import json
import time
import argparse
import logging
import os
from datetime import datetime
from pymongo import MongoClient, DESCENDING
from bson import json_util
from typing import Dict, List, Any

# MongoDB Connection
# prod
MONGO_URI = ""


DB_NAME = "qb"
COLLECTION_NAME = "questions"

# Taxonomy data to update (all IDs as strings)
# Key: oldQuestionId (as string)
# Value: Object with taxonomyData fields and customTags field (tag name to add)
TAX_DATA = {
    "6092053": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2408"
    },
    "6091989": {
        "customTags": "IA_Concept",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2404"
    },
    "1087341": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2403"
    },
    "1283230": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2403"
    },
    "1322881": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2405"
    },
    "1377752": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2408"
    },
    "1377873": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2403"
    },
    "1410383": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2403"
    },
    "1561994": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2408"
    },
    "1571246": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2404"
    },
    "1836165": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2408"
    },
    "1836177": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2408"
    },
    "1836186": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2408"
    },
    "1909955": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2408"
    },
    "1961951": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2408"
    },
    "2499903": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2404"
    },
    "2499906": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2404"
    },
    "2646616": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2410"
    },
    "3164548": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2410"
    },
    "3164622": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2405"
    },
    "3193895": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2408"
    },
    "3263265": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2403"
    },
    "3263356": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2403"
    },
    "3266326": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2404"
    },
    "3268565": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2405"
    },
    "3394321": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2403"
    },
    "3394518": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2410"
    },
    "3399104": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2403"
    },
    "3606833": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2408"
    },
    "3614125": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2405"
    },
    "3643509": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2403"
    },
    "3673781": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2403"
    },
    "3779492": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2403"
    },
    "3779549": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2403"
    },
    "3848062": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2403"
    },
    "4179368": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2403"
    },
    "4179379": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2403"
    },
    "4182550": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2408"
    },
    "4339163": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2403"
    },
    "4339497": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2404"
    },
    "4365518": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2403"
    },
    "4368659": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2404"
    },
    "4392047": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2404"
    },
    "4444259": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2405"
    },
    "4444638": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2405"
    },
    "4462902": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2404"
    },
    "4472235": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2404"
    },
    "4487045": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2405"
    },
    "4487058": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2403"
    },
    "4490275": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2404"
    },
    "4520712": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2405"
    },
    "4569887": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2405"
    },
    "4578427": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2404"
    },
    "4660877": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2408"
    },
    "4890287": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2404"
    },
    "4891864": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2410"
    },
    "5096328": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2404"
    },
    "5217528": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2405"
    },
    "5399368": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2404"
    },
    "5530182": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2404"
    },
    "5563789": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2408"
    },
    "6011716": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2404"
    },
    "6046002": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2404"
    },
    "6058081": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2403"
    },
    "6058482": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2404"
    },
    "6059745": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2404"
    },
    "6061536": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2404"
    },
    "6065584": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2405"
    },
    "6067863": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2408"
    },
    "6067884": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2408"
    },
    "6068399": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2408"
    },
    "6068435": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2408"
    },
    "6068552": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2408"
    },
    "6068586": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2408"
    },
    "6070601": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2410"
    },
    "6070613": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2410"
    },
    "6070617": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2410"
    },
    "6070692": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2410"
    },
    "6070707": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2410"
    },
    "6070730": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2410"
    },
    "6070743": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2410"
    },
    "6070759": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2410"
    },
    "6071372": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2405"
    },
    "6071433": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2405"
    },
    "6071488": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2405"
    },
    "6071568": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2405"
    },
    "6071606": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2405"
    },
    "6071648": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2405"
    },
    "6071664": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2405"
    },
    "6071679": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2405"
    },
    "6071741": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2408"
    },
    "6071769": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2408"
    },
    "6071929": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2410"
    },
    "6071936": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2410"
    },
    "6072223": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2410"
    },
    "6072228": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2410"
    },
    "6072239": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2410"
    },
    "6072245": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2410"
    },
    "6072410": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2410"
    },
    "6072821": {
        "customTags": "IA_Subtopic",
        "taxonomyId": "1734438058Qn",
        "classId": "1347",
        "subjectId": "2390",
        "topicId": "2392",
        "subtopicId": "2410"
    }
}


TARGET_TAXONOMY_ID = "1734438058Qn"

# Configuration
BATCH_SLEEP_SECONDS = 0.5  # Sleep between processing each oldQuestionId
CUSTOM_TAG_VALUE = "yes"
CUSTOM_TAG_TYPE = "boolean"
LOG_DIR = "log"


def setup_logging(dry_run: bool):
    """Setup logging configuration"""
    log_format = '%(asctime)s - %(levelname)s - %(message)s'

    if dry_run:
        # For dry-run, only console output
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[logging.StreamHandler()]
        )
    else:
        # For actual run, both file and console
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR)
        log_file = os.path.join(LOG_DIR, f"ideal_async_release_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        logging.info(f"Logging to file: {log_file}")


def tag_exists(custom_tags: List[Dict], tag_name: str) -> bool:
    """Check if a tag already exists in customTags"""
    if not custom_tags:
        return False
    return any(tag.get("tag_name") == tag_name for tag in custom_tags)


def create_backup(collection, old_question_ids: List[str]) -> str:
    """Create backup of latest version taxonomyData for each question"""
    backup_data = []

    for old_question_id in old_question_ids:
        try:
            latest_doc = collection.find_one(
                {"oldQuestionId": int(old_question_id)},
                sort=[("version", DESCENDING)]
            )

            if latest_doc:
                backup_entry = {
                    "_id": str(latest_doc["_id"]),
                    "oldQuestionId": old_question_id,
                    "version": latest_doc.get("version"),
                    "taxonomyData": latest_doc.get("taxonomyData", []),
                    "customTags": latest_doc.get("customTags", [])
                }
                backup_data.append(backup_entry)
                logging.info(f"  Backed up oldQuestionId: {old_question_id}, version: {latest_doc.get('version')}")
        except Exception as e:
            logging.error(f"  Error backing up oldQuestionId {old_question_id}: {e}")

    backup_filename = f"taxonomy_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(backup_filename, 'w') as f:
        json.dump(backup_data, f, indent=2, default=json_util.default)

    return backup_filename


def process_question(collection, old_question_id: str, tax_data: Dict[str, Any], dry_run: bool) -> Dict[str, int]:
    """
    Process a single oldQuestionId - update all its versions.
    Returns counts of operations performed.
    """
    stats = {
        "docs_found": 0,
        "taxonomy_replaced": 0,
        "taxonomy_added": 0,
        "custom_tags_added": 0,
        "custom_tags_skipped": 0,
        "quality_status_updated": 0
    }

    # Extract customTags name from tax_data
    custom_tag_name = tax_data.get("customTags")

    # Create taxonomy data without the customTags field (for taxonomyData array)
    taxonomy_entry = {k: v for k, v in tax_data.items() if k != "customTags"}

    # Find ALL versions with this oldQuestionId
    try:
        all_docs = list(collection.find({"oldQuestionId": int(old_question_id)}))
    except Exception as e:
        logging.error(f"  Error finding documents: {e}")
        return stats

    stats["docs_found"] = len(all_docs)

    if not all_docs:
        logging.warning(f"  No documents found")
        return stats

    logging.info(f"  Found {len(all_docs)} version(s)")

    # Separate documents based on whether they have the target taxonomyId
    docs_with_taxonomy = []
    docs_without_taxonomy = []

    for doc in all_docs:
        taxonomy_data = doc.get("taxonomyData") or []
        has_taxonomy = any(tax.get("taxonomyId") == TARGET_TAXONOMY_ID for tax in taxonomy_data)

        if has_taxonomy:
            docs_with_taxonomy.append(doc)
        else:
            docs_without_taxonomy.append(doc)

    # Log what will be done
    if docs_with_taxonomy:
        logging.info(f"  Will REPLACE taxonomy in {len(docs_with_taxonomy)} doc(s)")
    if docs_without_taxonomy:
        logging.info(f"  Will ADD taxonomy to {len(docs_without_taxonomy)} doc(s)")

    if dry_run:
        # Dry run - just count what would happen
        stats["taxonomy_replaced"] = len(docs_with_taxonomy)
        stats["taxonomy_added"] = len(docs_without_taxonomy)

        # Count custom tags that would be added
        for doc in all_docs:
            existing_tags = doc.get("customTags") or []
            if custom_tag_name and not tag_exists(existing_tags, custom_tag_name):
                stats["custom_tags_added"] += 1
            else:
                stats["custom_tags_skipped"] += 1

        # Latest version would be locked
        stats["quality_status_updated"] = 1

        logging.info(f"  [DRY-RUN] Would add customTag '{custom_tag_name}' to {stats['custom_tags_added']} doc(s)")
        logging.info(f"  [DRY-RUN] Would set questionQualityStatus to LOCKED for latest version")
        return stats

    # Actual updates
    try:
        # 1. Replace taxonomy entries in docs that already have them
        if docs_with_taxonomy:
            doc_ids = [doc["_id"] for doc in docs_with_taxonomy]
            result = collection.update_many(
                {"_id": {"$in": doc_ids}},
                {"$set": {"taxonomyData.$[elem]": taxonomy_entry}},
                array_filters=[{"elem.taxonomyId": TARGET_TAXONOMY_ID}]
            )
            stats["taxonomy_replaced"] = result.modified_count
            logging.info(f"  ✓ Replaced taxonomy in {result.modified_count} doc(s)")

        # 2. Add taxonomy entries to docs that don't have them
        if docs_without_taxonomy:
            for doc in docs_without_taxonomy:
                doc_id = doc["_id"]
                if doc.get("taxonomyData") is None:
                    # Initialize taxonomyData as array with new entry
                    collection.update_one(
                        {"_id": doc_id},
                        {"$set": {"taxonomyData": [taxonomy_entry]}}
                    )
                else:
                    # Push to existing array
                    collection.update_one(
                        {"_id": doc_id},
                        {"$push": {"taxonomyData": taxonomy_entry}}
                    )
            stats["taxonomy_added"] = len(docs_without_taxonomy)
            logging.info(f"  ✓ Added taxonomy to {len(docs_without_taxonomy)} doc(s)")

        # 3. Add customTag to ALL versions
        if custom_tag_name:
            new_tag = {
                "tag_name": custom_tag_name,
                "value": CUSTOM_TAG_VALUE,
                "tag_type": CUSTOM_TAG_TYPE
            }

            for doc in all_docs:
                doc_id = doc["_id"]
                existing_tags = doc.get("customTags") or []

                if tag_exists(existing_tags, custom_tag_name):
                    stats["custom_tags_skipped"] += 1
                    continue

                updated_tags = existing_tags.copy()
                updated_tags.append(new_tag)

                result = collection.update_one(
                    {"_id": doc_id},
                    {"$set": {"customTags": updated_tags}}
                )

                if result.modified_count > 0:
                    stats["custom_tags_added"] += 1
                else:
                    logging.warning(f"  ! Failed to add customTag to doc {doc_id}")

            logging.info(f"  ✓ Added customTag '{custom_tag_name}' to {stats['custom_tags_added']} doc(s), skipped {stats['custom_tags_skipped']} (already exists)")

        # 4. Update questionQualityStatus to LOCKED (2) for LATEST version only
        latest_doc = collection.find_one(
            {"oldQuestionId": int(old_question_id)},
            sort=[("version", DESCENDING)]
        )

        if latest_doc:
            latest_doc_id = latest_doc["_id"]
            latest_version = latest_doc.get("version")

            result = collection.update_one(
                {"_id": latest_doc_id},
                {"$set": {"questionQualityStatus": 2}}  # 2 = LockStatus
            )

            if result.modified_count > 0:
                stats["quality_status_updated"] = 1
                logging.info(f"  ✓ Set questionQualityStatus to LOCKED for latest version (v{latest_version})")
            else:
                logging.warning(f"  ! Failed to update questionQualityStatus for latest version (v{latest_version})")

    except Exception as e:
        logging.error(f"  Error during update: {e}")

    return stats


def main():
    parser = argparse.ArgumentParser(
        description='Update taxonomyData, add customTags, and lock questions based on oldQuestionId'
    )
    parser.add_argument('--dry-run', action='store_true', help='Run in dry-run mode (no actual updates)')
    parser.add_argument('--resume', type=int, default=0, help='Resume from index (0-based)')

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.dry_run)

    logging.info("=" * 70)
    logging.info("Ideal Async Release - Taxonomy, CustomTags & Lock Status Update Script")
    logging.info("=" * 70)
    logging.info(f"Mode: {'DRY-RUN' if args.dry_run else 'ACTUAL RUN'}")
    logging.info(f"Total oldQuestionIds to process: {len(TAX_DATA)}")
    if args.resume > 0:
        logging.info(f"Resuming from index: {args.resume}")
    logging.info("=" * 70)

    # Warning for actual run
    if not args.dry_run:
        logging.warning("\n⚠️  WARNING: This will modify the PRODUCTION database!")
        logging.warning("Starting in 5 seconds... Press Ctrl+C to cancel\n")
        try:
            time.sleep(5)
        except KeyboardInterrupt:
            logging.info("\nOperation cancelled by user")
            sys.exit(0)

    try:
        # Connect to MongoDB
        logging.info("Connecting to MongoDB...")
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        # Test connection
        collection.find_one()
        logging.info("✓ Connected to MongoDB successfully\n")

        # Step 1: Create backup (only for items we'll process)
        items = list(TAX_DATA.items())
        items_to_process = items[args.resume:]
        old_ids_to_backup = [item[0] for item in items_to_process]

        if not args.dry_run:
            logging.info("Step 1: Creating backup of latest versions...")
            backup_filename = create_backup(collection, old_ids_to_backup)
            logging.info(f"✓ Backup saved to: {backup_filename}\n")
        else:
            logging.info("Step 1: [DRY-RUN] Skipping backup\n")

        # Step 2: Process updates
        logging.info("Step 2: Processing updates...")
        start_time = time.time()

        total_stats = {
            "docs_found": 0,
            "taxonomy_replaced": 0,
            "taxonomy_added": 0,
            "custom_tags_added": 0,
            "custom_tags_skipped": 0,
            "quality_status_updated": 0
        }

        for idx, (old_question_id, tax_data) in enumerate(items):
            if idx < args.resume:
                logging.info(f"[{idx}] Skipping oldQuestionId {old_question_id} (before resume point)")
                continue

            logging.info(f"\n[{idx}] Processing oldQuestionId: {old_question_id}")

            stats = process_question(collection, old_question_id, tax_data, args.dry_run)

            # Accumulate stats
            for key in total_stats:
                total_stats[key] += stats[key]

            # Sleep to avoid stressing the database
            time.sleep(BATCH_SLEEP_SECONDS)

        elapsed_time = time.time() - start_time

        # Summary
        logging.info("\n" + "=" * 70)
        logging.info("SUMMARY")
        logging.info("=" * 70)
        logging.info(f"Mode: {'DRY-RUN' if args.dry_run else 'ACTUAL RUN'}")
        logging.info(f"Total documents found: {total_stats['docs_found']}")
        logging.info(f"Taxonomy entries replaced: {total_stats['taxonomy_replaced']}")
        logging.info(f"Taxonomy entries added: {total_stats['taxonomy_added']}")
        logging.info(f"Custom tags added: {total_stats['custom_tags_added']}")
        logging.info(f"Custom tags skipped (already exists): {total_stats['custom_tags_skipped']}")
        logging.info(f"Questions locked (latest version): {total_stats['quality_status_updated']}")
        logging.info(f"Time elapsed: {elapsed_time:.2f} seconds")
        logging.info("=" * 70)

        if args.dry_run:
            logging.info("\n✓ Dry-run completed successfully. No changes were made to the database.")
            logging.info("To perform actual updates, run without --dry-run flag")
        else:
            logging.info("\n✓ Updates completed successfully!")

    except KeyboardInterrupt:
        logging.info("\n\nOperation interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"\n\nFatal error: {e}")
        import traceback
        logging.error(traceback.format_exc())
        sys.exit(1)
    finally:
        try:
            client.close()
            logging.info("MongoDB connection closed")
        except:
            pass


if __name__ == "__main__":
    main()