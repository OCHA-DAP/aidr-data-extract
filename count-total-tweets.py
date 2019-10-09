""" Count the total tweets processed by AIDR. """

import sys

line_count = 0

for filename in sys.argv:
    with open(filename, 'r') as input:
        for line in input:
            line_count += 1
            if (line_count % 100000) == 0:
                print("{:,}...".format(line_count))

print("Total lines/tweets: {:,}".format(line_count))

