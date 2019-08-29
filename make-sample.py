""" Sample random lines from a HXL dataset """

import hxl, io, random, sys

if len(sys.argv) != 2:
    print("Usage: {} <sample-rate>".format(sys.argv[0]), file=sys.stderr)
    sys.exit(2)

sample_rate = float(sys.argv[1])

random.seed()

class SampleFilter(hxl.filters.AbstractStreamingFilter):

    def __init__(self, source):
        super(hxl.filters.AbstractStreamingFilter, self).__init__(source)

    def filter_row(self, row):
        if random.random() >= sample_rate:
            return None # remove from output
        else:
            return row

hxl.write_hxl(sys.stdout, SampleFilter(hxl.data(sys.stdin.buffer)))
