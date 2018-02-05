"""
Generate Accelerated Thrift bindings
"""
import os
import argparse
import re
import shutil
import subprocess
import sys

xpr_hints = re.compile(".*completion_hints.*")


def parse_args(args=None):
    parser = argparse.ArgumentParser(description='Run some benchmarks')
    parser.add_argument('infile', nargs='?', type=argparse.FileType('r'),
                        default=sys.stdin, help="mapd.thrift file")
    parser.add_argument('outfile', nargs='?', default="mapd.thrift",
                        help="Patched mapd.thrift file")
    return parser.parse_args(args)


def thrift_gen(spec):
    subprocess.check_output(['thrift', '-gen', 'py', '-r', spec])


def main(args=None):
    args = parse_args(args)
    thrift = args.infile.readlines()
    new = [x for x in thrift if not xpr_hints.match(x)]
    with open(args.outfile, 'wt') as f:
        f.write(''.join(new))

    try:
        thrift_gen(args.outfile)
        shutil.rmtree("mapd", ignore_errors=True)
        shutil.copytree(os.path.join("gen-py", "mapd"), "mapd")
    finally:
        os.remove(args.outfile)
        shutil.rmtree("gen-py")


if __name__ == '__main__':
    sys.exit(main(None))
