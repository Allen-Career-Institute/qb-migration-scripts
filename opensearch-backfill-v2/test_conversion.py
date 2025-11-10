#!/usr/bin/env python3
"""
Test script to validate the conversion logic and data structures
"""

import sys
from main import (
    QuestionDoc, QuestionContent, QuestionStem, QuestionOption,
    TaxonomyData, Tag, CustomTag, HashTags,
    DocumentProcessor, MongoConnector
)


def create_sample_question() -> QuestionDoc:
    """Create a sample question for testing"""
    
    # Create question content
    stem = QuestionStem(text="What is the capital of France?")
    options = [
        QuestionOption(text="London"),
        QuestionOption(text="Berlin"),
        QuestionOption(text="Paris"),
        QuestionOption(text="Madrid")
    ]
    
    content = QuestionContent(
        language=1,
        question_nature=1,
        has_text_solution=True,
        question_stem=stem,
        options=options,
        answer="Paris"
    )
    
    # Create taxonomy data
    taxonomy = TaxonomyData(
        taxonomy_id="tax123",
        class_id="class456",
        subject_id="subject789",
        topic_id="topic012",
        subtopic_id="subtopic345"
    )
    
    # Create tags
    old_tags = [
        Tag(name="streamName", value="NEET"),
        Tag(name="className", value="Class 12"),
        Tag(name="subjectName", value="Geography")
    ]
    
    custom_tags = [
        CustomTag(tag_name="difficulty", value="medium", tag_type="level")
    ]
    
    hash_tags = [
        HashTags(hash_tag_id="hash123", description="Geography basics")
    ]
    
    # Create question document
    question = QuestionDoc(
        question_id="q123456",
        old_question_id=12345,
        version=1,
        status=3,
        type=1,
        qns_level=2,
        session=2024,
        source=1,
        source_center="ALLEN",
        unique_identifier="ALLEN_12345",
        has_video_solution=True,
        content=[content],
        taxonomy_data=[taxonomy],
        old_tags=old_tags,
        custom_tags=custom_tags,
        hash_tags=hash_tags,
        created_at=1640995200,  # 2022-01-01
        updated_at=1640995200
    )
    
    return question


def test_document_conversion():
    """Test the document conversion logic"""
    print("Testing document conversion logic...")
    
    # Create a mock MongoDB connector (we won't actually connect)
    class MockMongoConnector:
        def __init__(self):
            pass
    
    mock_mongo = MockMongoConnector()
    processor = DocumentProcessor(mock_mongo)
    
    # Create sample question
    question = create_sample_question()
    solution = None  # No solution for this test
    
    # Build OpenSearch documents
    docs = processor._build_opensearch_docs(question, solution)
    
    print(f"Generated {len(docs)} documents")
    
    if docs:
        doc = docs[0]
        print("\nSample document structure:")
        for key, value in doc.items():
            print(f"  {key}: {type(value).__name__} = {value}")
        
        # Validate required fields
        required_fields = [
            '_id', 'old_question_id', 'question_id', 'question', 'options',
            'answer', 'language', 'status', 'difficulty_level', 'question_type'
        ]
        
        missing_fields = [field for field in required_fields if field not in doc]
        if missing_fields:
            print(f"\nMissing required fields: {missing_fields}")
            return False
        else:
            print(f"\nAll required fields present: ✓")
        
        # Validate data types
        validations = [
            ('_id', str),
            ('old_question_id', int),
            ('question', str),
            ('options', list),
            ('language', int),
            ('status', int)
        ]
        
        for field, expected_type in validations:
            if not isinstance(doc[field], expected_type):
                print(f"Field '{field}' should be {expected_type.__name__}, got {type(doc[field]).__name__}")
                return False
        
        print("Data type validation: ✓")
        
        # Test taxonomy processing
        if 'class_tax' in doc:
            print(f"Taxonomy class_tax: {doc['class_tax']}")
        if 'subject_tax' in doc:
            print(f"Taxonomy subject_tax: {doc['subject_tax']}")
        
        # Test custom tags formatting
        if 'custom_tags' in doc:
            print(f"Custom tags: {doc['custom_tags']}")
        
        # Test hash tags formatting
        if 'hashTags' in doc:
            print(f"Hash tags: {doc['hashTags']}")
        
        print("\nDocument conversion test: PASSED ✓")
        return True
    else:
        print("No documents generated")
        return False


def test_taxonomy_processing():
    """Test taxonomy ID extraction and formatting"""
    print("\nTesting taxonomy processing...")
    
    mock_mongo = type('MockMongo', (), {})()
    processor = DocumentProcessor(mock_mongo)
    
    # Create test taxonomy data
    taxonomy_data = [
        TaxonomyData(
            taxonomy_id="tax1",
            class_id="cls1",
            subject_id="subj1",
            topic_id="topic1"
        ),
        TaxonomyData(
            taxonomy_id="tax2",
            class_id="cls2",
            subject_id="subj1",  # Same subject, different taxonomy
            concept_id="concept1"
        )
    ]
    
    # Extract taxonomy IDs
    tax_ids = processor._extract_taxonomy_ids(taxonomy_data)
    
    print(f"Class taxonomy: {tax_ids.class_tax}")
    print(f"Subject taxonomy: {tax_ids.subject_tax}")
    print(f"Topic taxonomy: {tax_ids.topic_tax}")
    print(f"Concept taxonomy: {tax_ids.concept_tax}")
    
    # Validate expected combinations
    expected_class = {'tax1_cls1', 'tax2_cls2'}
    expected_subject = {'tax1_subj1', 'tax2_subj1'}
    expected_topic = {'tax1_topic1'}
    expected_concept = {'tax2_concept1'}
    
    if set(tax_ids.class_tax) == expected_class:
        print("Class taxonomy extraction: ✓")
    else:
        print(f"Class taxonomy mismatch: expected {expected_class}, got {set(tax_ids.class_tax)}")
        return False
    
    if set(tax_ids.subject_tax) == expected_subject:
        print("Subject taxonomy extraction: ✓")
    else:
        print(f"Subject taxonomy mismatch: expected {expected_subject}, got {set(tax_ids.subject_tax)}")
        return False
    
    print("Taxonomy processing test: PASSED ✓")
    return True


def test_tag_formatting():
    """Test custom tags and hash tags formatting"""
    print("\nTesting tag formatting...")
    
    mock_mongo = type('MockMongo', (), {})()
    processor = DocumentProcessor(mock_mongo)
    
    # Test custom tags
    custom_tags = [
        CustomTag(tag_name="difficulty", value="hard", tag_type="level"),
        CustomTag(tag_name="source", value="textbook", tag_type="origin"),
        CustomTag(tag_name="", value="empty_name", tag_type="test"),  # Should be filtered out
        CustomTag(tag_name="empty_value", value="", tag_type="test")   # Should be filtered out
    ]
    
    formatted_custom = processor._format_custom_tags(custom_tags)
    print(f"Formatted custom tags: {formatted_custom}")
    
    expected_custom = ["difficulty|hard", "source|textbook"]
    if formatted_custom == expected_custom:
        print("Custom tags formatting: ✓")
    else:
        print(f"Custom tags mismatch: expected {expected_custom}, got {formatted_custom}")
        return False
    
    # Test hash tags
    hash_tags = [
        HashTags(hash_tag_id="hash1", description="Description 1"),
        HashTags(hash_tag_id="hash2", description="Description 2"),
        HashTags(hash_tag_id="", description="Empty ID"),      # Should be filtered out
        HashTags(hash_tag_id="hash3", description="")          # Should be filtered out
    ]
    
    formatted_hash = processor._format_hash_tags(hash_tags)
    print(f"Formatted hash tags: {formatted_hash}")
    
    expected_hash = ["hash1|Description 1", "hash2|Description 2"]
    if formatted_hash == expected_hash:
        print("Hash tags formatting: ✓")
    else:
        print(f"Hash tags mismatch: expected {expected_hash}, got {formatted_hash}")
        return False
    
    print("Tag formatting test: PASSED ✓")
    return True


def main():
    """Run all tests"""
    print("=== Python Conversion Validation Tests ===\n")
    
    tests = [
        test_document_conversion,
        test_taxonomy_processing,
        test_tag_formatting
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"Test failed with exception: {e}")
            failed += 1
        print()
    
    print("=== Test Results ===")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Total: {passed + failed}")
    
    if failed == 0:
        print("\n🎉 All tests passed! The Python conversion is working correctly.")
        return 0
    else:
        print(f"\n❌ {failed} test(s) failed. Please review the implementation.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
