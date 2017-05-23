from argparse import ArgumentParser
from matplotlib import pyplot
import re

parser = ArgumentParser()
parser.add_argument('input')
parser.add_argument('pattern')
parser.add_argument('--name')
parser.add_argument('--x_label')
parser.add_argument('--y_label')

def main():
    args = parser.parse_args()
    input = open(args.input, 'r')
    pattern = re.compile(args.pattern)
    x = []
    y = []
    
    for line in input:
        match = pattern.match(line)
        if match is not None:
            x.append(float(match.group(1)))
            y.append(float(match.group(2)))


    pyplot.figure
    pyplot.xlim(min(x), max(x))
    pyplot.ylim(0,1)
    pyplot.xlabel(args.x_label)
    pyplot.ylabel(args.y_label)
    pyplot.plot(x,y)
    pyplot.savefig('%s.png' % args.name)

if __name__ == '__main__':
    main()
