from argparse import ArgumentParser
from bz2 import BZ2File
from random import random

parser = ArgumentParser()
parser.add_argument('input_filename')
parser.add_argument('output_filename')
parser.add_argument('--rate', '-r', type=float, default=0.01)

def main():
    args = parser.parse_args()
    input_file = BZ2File(args.input_filename, 'r')
    output_file = BZ2File(args.output_filename, 'w')
    output_file.write(input_file.readline())

    num_sampled = 0
    num_processed = 0
    for line in input_file:
        if random() < args.rate:
            output_file.write(line)
            num_sampled += 1
        num_processed += 1

    input_file.close()
    output_file.close()
    print "Sampled %d of %d records." % (num_sampled, num_processed)

if __name__ == '__main__':
    main()
