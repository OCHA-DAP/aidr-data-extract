AIDR data tools
===============

Extract tweet-analysis data from AIDR's JSON output format into a spreadsheet.

The AIDR data is arranged with one JSON object on each line. This library reads the input line by line, parses each JSON object, writes the result as a CSV row, then continues to the next line. Since it does not need to hold all of the data in memory at once, it can handle very large datasets.

# Requirements

This package requires Python3 along with the following Python packages:

* ggeocode
* hxl
* python-dateutil

# Installation

    $ pip install -r requirements.txt

## One-time preparation

If you want to geocode (you probably do), then you'll need to prepare a name map file first:

1. Download the GeoNames ``allCountries.zip`` data file from http://download.geonames.org/export/dump/allCountries.zip
2. Unzip the archive to get the file ``allCountries.txt`` (approximately 1.5 GB)
3. Compile a local JSON mapping table using the command below (you may pick a different filename or path).

Compilation command:

    $ python -m ggeocode.parser allCountries.txt > name-map.lines.json

# Usage

## Extract data to a HXL-hashtagged CSV file (with geocoding):

    $ python extract-aidr-data.py -n name-map.lines.json < source.txt.json > output.csv

## Create reports (using libhxl)

Total tweets by day and language:

    $ hxlcount -t date+posted,meta+lang tweets.csv > report.csv

Total tweets by week and language:

    $ hxlcount -t date+week_start,meta+lang tweets.csv > report.csv

Total tweets by week, language, and country:

    $ hxlcount -t date+week_start,meta+lang,country+code tweets.csv > report.csv


# License

This package is in the Public Domain and comes with NO WARRANTY. See the file UNLICENSE for details.

