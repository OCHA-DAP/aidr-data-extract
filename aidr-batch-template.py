""" Template for batch-processing AIDR data in Python 3
Typical usage would be

    $ cat *.json | python3 aidr-batch-template.py > my-output-data.csv

This code is in the Public Domain. Use or abuse it as you wish.
"""

import csv, json, sys

# Create a CSV writer
output = csv.writer(sys.stdout)

# write the CSV header row
output.writerow([
    'Date',
    'Language'
])

# read one line at a time from standard input
# each line is a JSON object
for line in sys.stdin:

    # parse the JSON
    record = json.loads(line)

    # extract some fields from the JSON
    date = record.get('created_at', '')
    lang = record.get('lang', '')

    # write a data row out to the CSV
    output.writerow([date, lang])
