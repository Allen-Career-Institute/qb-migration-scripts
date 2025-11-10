#!/usr/bin/env python3
"""
Script to extract oldQuestionIds where the reason starts with 
"API call failed for some/all languages" from all failed_oldQuestionIds_*.txt files
"""

import os
import glob
from datetime import datetime

def extract_api_failed_ids(directory_path, output_file):
    """
    Extract oldQuestionIds where reason starts with "API call failed for some/all languages"
    
    Args:
        directory_path: Path to the directory containing the .txt files
        output_file: Path to the output file
    """
    # Find all .txt files matching the pattern
    pattern = os.path.join(directory_path, "failed_oldQuestionIds_*.txt")
    txt_files = sorted(glob.glob(pattern))
    
    if not txt_files:
        print(f"No files found matching pattern: {pattern}")
        return
    
    print(f"Found {len(txt_files)} files to process")
    
    api_failed_ids = []
    total_lines_processed = 0
    
    # Process each file
    for file_path in txt_files:
        filename = os.path.basename(file_path)
        print(f"Processing: {filename}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                
                # Skip first 2 header lines and process from line 3 onwards
                for line_num, line in enumerate(lines[2:], start=3):
                    line = line.strip()
                    if not line:
                        continue
                    
                    # Split by comma (only split on first comma to handle commas in reason)
                    parts = line.split(',', 1)
                    if len(parts) == 2:
                        old_question_id = parts[0].strip()
                        reason = parts[1].strip()
                        
                        # Check if reason starts with the target string
                        if reason.startswith("API call failed for some/all languages"):
                            api_failed_ids.append(old_question_id)
                    
                    total_lines_processed += 1
                    
        except Exception as e:
            print(f"Error processing {filename}: {e}")
            continue
    
    # Write results to output file
    print(f"\nTotal lines processed: {total_lines_processed}")
    print(f"Total IDs matching criteria: {len(api_failed_ids)}")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"# oldQuestionIds where API call failed for some/all languages\n")
        f.write(f"# Generated on: {datetime.now().isoformat()}\n")
        f.write(f"# Total count: {len(api_failed_ids)}\n")
        f.write("#\n")
        for question_id in api_failed_ids:
            f.write(f"{question_id}\n")
    
    print(f"\nOutput written to: {output_file}")
    print("Done!")

if __name__ == "__main__":
    # Set the directory path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Output file path
    output_file = os.path.join(script_dir, "api_call_failed_oldQuestionIds_v2.txt")
    
    # Run the extraction
    extract_api_failed_ids(script_dir, output_file)

