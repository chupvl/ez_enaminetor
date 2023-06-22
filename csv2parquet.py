import argparse
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pc


def convert_csv_to_parquet(in_path, out_path, delimiter='\t'):
    options = pc.ReadOptions()
    options.use_threads = True
    parse_options = pc.ParseOptions()
    parse_options.delimiter = delimiter

    writer = None
    with pa.csv.open_csv(in_path, read_options=options, parse_options=parse_options) as reader:
        for next_chunk in reader:
            if next_chunk is None:
                break
            if writer is None:
                writer = pq.ParquetWriter(out_path, next_chunk.schema)
            next_table = pa.Table.from_batches([next_chunk])
            writer.write_table(next_table)
    writer.close()


def main():
    parser = argparse.ArgumentParser(description='Convert CSV to Parquet')
    parser.add_argument('input', help='Input CSV file path')
    parser.add_argument('output', help='Output Parquet file path')
    parser.add_argument('--delimiter', default='\t', help='Delimiter for the CSV file (default: tab)')

    args = parser.parse_args()

    convert_csv_to_parquet(args.input, args.output, args.delimiter)


if __name__ == '__main__':
    main()

# todo: add IPC format