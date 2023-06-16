import argparse
import pyarrow.parquet as pq

def convert_parquet_to_csv(parquet_file, csv_file, chunk_size):
    # Read Parquet file in chunks and append to CSV file
    parquet_reader = pq.ParquetFile(parquet_file)
    total_rows = parquet_reader.num_row_groups
    num_chunks = (total_rows // chunk_size) + 1

    for i in range(num_chunks):
        # Calculate start and end row for each chunk
        start_row = i * chunk_size
        end_row = min((i + 1) * chunk_size, total_rows)

        # Read the chunk of rows from Parquet into a Pandas DataFrame
        df = parquet_reader.read_row_group(start_row, end_row - start_row).to_pandas()

        # Convert the DataFrame to CSV and write to file
        if i == 0:
            mode = 'w'  # Write mode for the first chunk
        else:
            mode = 'a'  # Append mode for subsequent chunks
        df.to_csv(csv_file, mode=mode, index=False, header=(i == 0))

    print(f"Conversion complete. CSV files generated: {num_chunks}")

# Parse command line arguments
parser = argparse.ArgumentParser(description='Convert Parquet file to CSV.')
parser.add_argument('parquet_file', type=str, help='Path to the input Parquet file')
parser.add_argument('csv_file', type=str, help='Path to the output CSV file')
parser.add_argument('chunk_size', type=int, help='Number of rows per CSV file')

args = parser.parse_args()

# Call the conversion function with the provided arguments
convert_parquet_to_csv(args.parquet_file, args.csv_file, args.chunk_size)
