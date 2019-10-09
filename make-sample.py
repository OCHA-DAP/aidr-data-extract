""" Sample random lines from a HXL dataset """

import hxl, io, math, random, sys

class SampleFilter(hxl.filters.AbstractStreamingFilter):

    def __init__(self, source, sample_rate):
        super(hxl.filters.AbstractStreamingFilter, self).__init__(source)
        self.sample_rate = sample_rate
        random.seed()

    def filter_row(self, row):
        if random.random() >= self.sample_rate:
            return None # remove from output
        else:
            return row

if len(sys.argv) != 2:
    print("Usage: {} <sample-size>".format(sys.argv[0]), file=sys.stderr)
    sys.exit(2)

sample_size = int(sys.argv[1])

data = hxl.data(sys.stdin.buffer).cache()

n_rows = 0
for row in data:
    n_rows = n_rows + 1

sample_rate = float(sample_size) / float(n_rows)

hxl.write_hxl(sys.stdout, SampleFilter(data, sample_rate))
