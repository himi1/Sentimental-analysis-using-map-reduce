from argparse import ArgumentParser
from bz2 import BZ2File
import re

parser = ArgumentParser()
parser.add_argument('input_filename')
parser.add_argument('output_filename')

def main():
    args = parser.parse_args()
    input_file = BZ2File(args.input_filename, 'r')
    output_file = open(args.output_filename, 'w')
    seen = set()

    output_file.write('package ebird.data\n')
    output_file.write('\n')
    output_file.write('object Features {\n') 
    
    features = input_file.readline().strip().split(',')
    for feature, name in enumerate(features):
        name = name.upper().replace(' ', '_')
        while name in seen:
            match = re.match(r'([A-Z_]+)(\d+)', name)
            if match is not None:
                name = match.group(1) + str(int(match.group(2)) + 1)
            else:
                name = name + '1'
        output_file.write('    val %s = %d\n' % (name, feature))
        seen.add(name)
    
    output_file.write('}')
    input_file.close()
    output_file.close()

if __name__ == '__main__':
    main()
