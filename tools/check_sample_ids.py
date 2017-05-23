from argparse import ArgumentParser
from bz2 import BZ2File
from random import random

parser = ArgumentParser()
parser.add_argument('input')
parser.add_argument('output')

def main():
    args = parser.parse_args()
    input = BZ2File(args.input, 'r')
    output = open(args.output, 'r')
    input.readline()
    output.readline()
    for input_line, output_line in zip(input, output):
        if input_line.split(',')[0] != output_line.split(',')[0]:
            print "Sample ID's do not match."
            break
    print "Sample ID's match"

if __name__ == '__main__':
    main()
