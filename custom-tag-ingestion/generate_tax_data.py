"""
Script to generate TAX_DATA dictionary from custom_tag_data.csv
"""

import csv

def generate_tax_data(input_csv='custom_tag_data.csv', output_file='tax_data.py'):
    """
    Read CSV and generate TAX_DATA dictionary
    
    CSV Format (tab-separated):
    taxonomyId	classId	subjectId	TopicID	SubtopicID	QId	conceptId	customTags
    """
    
    tax_data = {}
    
    with open(input_csv, 'r', encoding='utf-8') as f:
        # Read tab-separated values
        for line_num, line in enumerate(f, 1):
            try:
                # Split by tab
                parts = line.strip().split('\t')
                
                if len(parts) != 8:
                    print(f"Warning: Line {line_num} has {len(parts)} columns (expected 8)")
                    continue
                
                taxonomy_id, class_id, subject_id, topic_id, subtopic_id, qid, concept_id, custom_tags = parts
                
                # Create entry for this QId
                tax_data[qid] = {
                    "customTags": custom_tags,
                    "taxonomyId": taxonomy_id,
                    "classId": class_id,
                    "subjectId": subject_id,
                    "topicId": topic_id,
                    "subtopicId": subtopic_id,
                    "conceptId": concept_id
                }
                
            except Exception as e:
                print(f"Error processing line {line_num}: {e}")
                continue
    
    # Write to Python file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('"""Auto-generated TAX_DATA from custom_tag_data.csv"""\n\n')
        f.write('TAX_DATA = {\n')
        
        # Sort by QId for better readability
        sorted_items = sorted(tax_data.items(), key=lambda x: int(x[0]))
        
        for idx, (qid, data) in enumerate(sorted_items):
            f.write(f'    "{qid}": {{\n')
            f.write(f'        "customTags": "{data["customTags"]}",\n')
            f.write(f'        "taxonomyId": "{data["taxonomyId"]}",\n')
            f.write(f'        "classId": "{data["classId"]}",\n')
            f.write(f'        "subjectId": "{data["subjectId"]}",\n')
            f.write(f'        "topicId": "{data["topicId"]}",\n')
            f.write(f'        "subtopicId": "{data["subtopicId"]}",\n')
            f.write(f'        "conceptId": "{data["conceptId"]}"\n')
            
            # Add comma except for last item
            if idx < len(sorted_items) - 1:
                f.write('    },\n')
            else:
                f.write('    }\n')
        
        f.write('}\n')
    
    print(f"✅ Generated TAX_DATA with {len(tax_data)} entries")
    print(f"📄 Output saved to: {output_file}")
    
    # Display first 3 entries as sample
    print(f"\n📋 Sample entries (first 3):")
    for idx, (qid, data) in enumerate(sorted_items[:3]):
        print(f'  "{qid}": {data}')
    
    return tax_data


if __name__ == "__main__":
    generate_tax_data()

