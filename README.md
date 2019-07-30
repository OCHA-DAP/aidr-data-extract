AIDR data tools
===============

Extract tweet-analysis data from AIDR's JSON output format into a spreadsheet.

The AIDR data is arranged with one JSON object on each line. This library reads the input line by line, parses each JSON object, writes the result as a CSV row, then continues to the next line. Since it does not need to hold all of the data in memory at once, it can handle very large datasets.

# Requirements

This package requires Python3 along with the following Python packages:

* geocoder
* hxl
* python-dateutil

# Installation

    $ pip install -r requirements.txt

# Usage

## Extract data to a HXL-hashtagged CSV file

    $ python3 extract-aidr-data.py < source.txt.json > output.csv

## Geocode extracted tweets

    $ # TODO

## Create a report

    $ # TODO

# License

This package is in the Public Domain and comes with NO WARRANTY.

