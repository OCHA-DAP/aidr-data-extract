AIDR data parser
================

Extract tweet-analysis data from AIDR's JSON output format into a spreadsheet.

The AIDR data is arranged with one JSON object on each line. This library reads the input line by line, parses each JSON object, writes the result as a CSV row, then continues to the next line. Since it does not need to hold all of the data in memory at once, it can handle very large datasets.

# Requirements

* Python 3
* python-dateutil library

# Installation

    $ pip install -r requirements.txt

# Usage

    $ python3 extract-aidr-data.py < source.txt.json > output.csv

