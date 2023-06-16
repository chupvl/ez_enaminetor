import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pc
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import time

from rdkit import Chem
from rdkit import RDLogger
from rdkit.Chem import Descriptors

import dask.dataframe as dd

def smiles2props(s):
    mol = Chem.MolFromSmiles(str(s))
    if mol is not None:
        hba = Descriptors.NumHAcceptors(mol)
        hbd = Descriptors.NumHDonors(mol)
        mw = round(Descriptors.MolWt(mol), 4)
        num_heavy_atoms = Descriptors.HeavyAtomCount(mol)
        return [mw, hba, hbd, num_heavy_atoms]
    else:
        return [None, None, None, None]

def convert_csv_to_parquet(in_path, out_path, delimiter='\t'):

    options = pc.ReadOptions()
    options.use_threads = True
    parse_options = pc.ParseOptions()
    parse_options.delimiter = delimiter

    writer = None
    start_time = time.time()

    # Set the CSV parse options
    if parse_options is None:
        parse_options = pa.csv.ParseOptions()

    with pa.csv.open_csv(in_path, parse_options=parse_options) as reader:
        for next_chunk in reader:
            if next_chunk is None:
                break
            if writer is None:
                next_chunk_wprops = process_pa_chunk(next_chunk)
                writer = pq.ParquetWriter(out_path, next_chunk_wprops.schema)
            next_chunk_wprops = process_pa_chunk(next_chunk)
            next_table = pa.Table.from_batches([next_chunk_wprops])
            writer.write_table(next_table)

    writer.close()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Conversion completed in {elapsed_time:.2f} seconds.")
def process_pa_chunk(chunk):
    smiles_list = chunk['smiles'].to_pylist()
    smiles_bag = db.from_sequence(smiles_list)
    prop_bag = smiles_bag.map(smiles2props)
    props = prop_bag.compute()

    # Convert the chunk to a pandas DataFrame
    df = chunk.to_pandas()

    # Add the new properties as columns to the DataFrame: one by one for clarity
    #df['smiles_original'] = smiles_list
    df['MW'] = [prop[0] for prop in props]
    df['HBA'] = [prop[1] for prop in props]
    df['HBD'] = [prop[2] for prop in props]
    df['HeavyAtoms'] = [prop[3] for prop in props]

    # Convert the DataFrame back to a pyarrow Table
    combined_table = pa.Table.from_pandas(df, preserve_index=False)

    # Get the list of record batches in the table
    batches = combined_table.to_batches()

    # Return the first record batch
    return batches[0]





def convert_csv_to_parquet(in_path, out_path, read_options=None, parse_options=None):
    writer = None
    start_time = time.time()

    with pa.csv.open_csv(in_path, read_options=read_options, parse_options=parse_options) as reader:
        for next_chunk in reader:
            if next_chunk is None:
                break
            if writer is None:
                next_chunk_wprops = process_pa_chunk(next_chunk)
                writer = pq.ParquetWriter(out_path, next_chunk_wprops.schema)
            next_chunk_wprops = process_pa_chunk(next_chunk)
            next_table = pa.Table.from_batches([next_chunk_wprops])
            writer.write_table(next_table)

    writer.close()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Conversion completed in {elapsed_time:.2f} seconds.")

def convert_parquet_to_csv(parquet_file, csv_file, chunk_size):
    # Read Parquet file in chunks
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

# Usage example
parquet_file = 'path/to/input.parquet'
csv_file = 'path/to/output.csv'
chunk_size = 10000  # Define the desired number of rows per CSV file
#