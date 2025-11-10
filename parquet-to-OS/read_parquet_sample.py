#!/usr/bin/env python3
"""
Simple script to read and display the first 10 rows from a large parquet file.
Optimized for large files (6GB+) by reading only what's needed.
"""

import pandas as pd
import sys
import os

def read_parquet_sample(file_path, num_rows=10, save_to_csv=True):
    """
    Read and display the first N rows from a parquet file, optionally save to CSV.
    
    Args:
        file_path (str): Path to the parquet file
        num_rows (int): Number of rows to display (default: 10)
        save_to_csv (bool): Whether to save the sample to CSV (default: True)
    """
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found.")
        return
    
    # Get file size for information
    file_size_gb = os.path.getsize(file_path) / (1024**3)
    print(f"File: {file_path}")
    print(f"Size: {file_size_gb:.2f} GB")
    print("-" * 50)
    
    try:
        import pyarrow.parquet as pq
        
        # First, get metadata without loading data
        print("Getting file metadata...")
        parquet_file = pq.ParquetFile(file_path)
        
        # Display basic info about the dataset from metadata
        schema = parquet_file.schema
        print(f"Total columns: {len(schema)}")
        print(f"Total rows: {parquet_file.metadata.num_rows}")
        print(f"Number of row groups: {parquet_file.num_row_groups}")
        print("-" * 50)
        
        # Display column names and types from schema
        print("Columns:")
        for i, field in enumerate(schema):
            print(f"  {i+1}. {field.name} ({field.type})")
        print("-" * 50)
        
        # Read only the first few rows efficiently
        print(f"Reading first {num_rows} rows...")
        
        # Read just the first row group or enough to get num_rows
        first_batch = parquet_file.read_row_group(0)
        df_sample = first_batch.to_pandas()
        
        # Take only the number of rows we need
        sample_df = df_sample.head(num_rows)
        
        print(f"Sample memory usage: {sample_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        print("-" * 50)
        
        # Print with better formatting
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 50)
        
        print(f"First {len(sample_df)} rows:")
        print(sample_df.to_string(index=True))
        
        # Save to CSV if requested
        if save_to_csv:
            csv_filename = f"parquet_sample_{num_rows}_rows.csv"
            csv_path = os.path.join(os.path.dirname(file_path), csv_filename)
            sample_df.to_csv(csv_path, index=True)
            print(f"\nSaved {len(sample_df)} rows to: {csv_path}")
        
        # Show data types and non-null counts for the sample
        print("\n" + "-" * 50)
        print("Sample Data Info:")
        print(sample_df.info())
        
    except Exception as e:
        print(f"Error reading parquet file: {e}")
        
        # Try alternative method - read with pandas but limit rows
        try:
            print("\nTrying alternative method...")
            # Use pandas to read just a small sample
            df_sample = pd.read_parquet(file_path, engine='pyarrow')
            
            # Get basic info
            print(f"Total columns: {len(df_sample.columns)}")
            print(f"Total rows: {len(df_sample)}")
            print("-" * 50)
            
            # Show columns
            print("Columns:")
            for i, col in enumerate(df_sample.columns):
                print(f"  {i+1}. {col} ({df_sample[col].dtype})")
            print("-" * 50)
            
            # Show sample
            sample = df_sample.head(num_rows)
            print(f"First {len(sample)} rows:")
            print(sample.to_string(index=True))
            
            # Save to CSV if requested
            if save_to_csv:
                csv_filename = f"parquet_sample_{num_rows}_rows.csv"
                csv_path = os.path.join(os.path.dirname(file_path), csv_filename)
                sample.to_csv(csv_path, index=True)
                print(f"\nSaved {len(sample)} rows to: {csv_path}")
            
        except Exception as e2:
            print(f"Alternative method also failed: {e2}")
            print("The file might be corrupted or in an unsupported format.")

def main():
    # Your specific file path
    default_file = "/Users/pulkitsharma/Desktop/OpenSearch files/parquet files/api_backfill_data_embeddings.parquet"
    
    # Get file path from command line argument or use your specific file
    if len(sys.argv) > 1 and sys.argv[1].strip():
        file_path = sys.argv[1]
    else:
        # Use your specific file path
        file_path = default_file
        print(f"Using default file: {file_path}")
    
    # Get number of rows (optional)
    num_rows = 10
    if len(sys.argv) > 2:
        try:
            num_rows = int(sys.argv[2])
        except ValueError:
            print("Invalid number of rows. Using default: 10")
    
    # Read and display the parquet file
    read_parquet_sample(file_path, num_rows)

if __name__ == "__main__":
    main()