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

    $ python3 geocode-aidr-data.py --cache-in=old-cache.csv --cache-out=new-cache.csv --output=tweets-geocoded.csv tweets-raw.csv

## Create reports (using libhxl)

Total tweets by day and language:

    $ hxlcount -t date+posted,meta+lang tweets.csv > report.csv

Total tweets by week and language:

    $ hxlcount -t date+week_start,meta+lang tweets.csv > report.csv

Total tweets by week, language, and country:

    $ hxlcount -t date+week_start,meta+lang,country+code tweets.csv > report.csv

# License

This package is in the Public Domain and comes with NO WARRANTY.

