import time

import pandas as pd
import dask.bag as db

import pyarrow as pa
import pyarrow.csv as pc
import pyarrow.parquet as pq

from rdkit import Chem
from rdkit.Chem import Descriptors
import argparse


def smiles2props(s):
    s = str(s)
    mol = Chem.MolFromSmiles(s)
    if mol is not None:
        hba = Descriptors.NumHAcceptors(mol)
        hbd = Descriptors.NumHDonors(mol)
        mw = round(Descriptors.MolWt(mol), 4)
        num_heavy_atoms = Descriptors.HeavyAtomCount(mol)
        return [mw, hba, hbd, num_heavy_atoms]
    else:
        return [None, None, None, None]


def process_pa_chunk(chunk, prop_function, prop_names):

    smiles_list = chunk['smiles'].to_pylist()
    smiles_bag = db.from_sequence(smiles_list)
    prop_bag = smiles_bag.map(prop_function)
    props = prop_bag.compute()

    df = chunk.to_pandas()
    df_props = pd.DataFrame(props, columns=prop_names)
    df = pd.concat([df, df_props], axis=1)
    combined_table = pa.Table.from_pandas(df, preserve_index=False)
    batches = combined_table.to_batches()

    return batches[0]


def convert_csv_to_parquet_withProps(in_path, out_path, delimiter='\t'):
    # Define your default fill value
    options = pc.ReadOptions()
    options.use_threads = True
    parse_options = pc.ParseOptions()
    parse_options.delimiter = delimiter

    writer = None

    with pa.csv.open_csv(in_path, read_options=options, parse_options=parse_options) as reader:
        prop_names = ['mw', 'hba', 'hbd', 'num_heavy_atoms']
        prop_func = smiles2props
        for next_chunk in reader:
            if next_chunk is None:
                break
            if writer is None:
                next_chunk_wprops = process_pa_chunk(next_chunk, prop_func, prop_names)
                writer = pq.ParquetWriter(out_path, next_chunk_wprops.schema)
            next_chunk_wprops = process_pa_chunk(next_chunk, prop_func, prop_names)
            next_table = pa.Table.from_batches([next_chunk_wprops])
            writer.write_table(next_table)

    writer.close()


def main():
    parser = argparse.ArgumentParser(description='Convert CSV to Parquet with additional properties')
    parser.add_argument('--input', required=True, help='Input CSV file path')
    parser.add_argument('--output', required=True, help='Output Parquet file path')
    parser.add_argument('--delimiter', default='\t', help='CSV delimiter. Default is tab.')
    parser.add_argument('--print_time', action='store_true', help='Print elapsed time. Default is False.')

    args = parser.parse_args()

    start_time = time.time()
    convert_csv_to_parquet_withProps(args.input, args.output, args.delimiter)
    end_time = time.time()
    elapsed_time = end_time - start_time

    if args.print_time:
        print(f"Conversion completed in {elapsed_time:.2f} seconds.")


if __name__ == '__main__':
    main()

# todo: add IPC format
