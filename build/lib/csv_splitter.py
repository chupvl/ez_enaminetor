#!/usr/bin/env python3
import argparse
def main(args):
    chunk_size = args.c

    def write_chunk(part, lines, name_pattern):
        filename = f'{name_pattern}_{part}.csv'
        with open(filename, 'w') as f_out:
            f_out.write(header)
            f_out.writelines(lines)

    with open(args.input_file, 'r') as f:
        count = 0
        header = f.readline()
        lines = []
        for line in f:
            count += 1
            lines.append(line)
            if count % chunk_size == 0:
                write_chunk(count // chunk_size, lines, args.n)
                lines = []
        # write remainder
        if len(lines) > 0:
            write_chunk((count // chunk_size) + 1, lines, args.name_pattern)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Split a CSV file into chunks.')
    parser.add_argument('input_file', help='Path to the input CSV file')
    parser.add_argument('-c', type=int, default=10000, help='Number of lines per chunk')
    parser.add_argument('-n', type=str, default='data_part', help='Prefix for output file names')
    args = parser.parse_args()
    main(args)