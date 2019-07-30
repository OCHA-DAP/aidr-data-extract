""" Extract data from an AIDR JSON file.

Usage:

    $ cat *.json | python3 extract-aidr-data.py > output.csv

Started 2019-07-25 by David Megginson
"""

import csv, dateutil.parser, dateutil.relativedelta, json, logging, re, sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("extract-aidr-data")

#
# Utility functions
#

def normalise_whitespace(s):
    """Clean whitespace in a string, and handle None
    """
    if s is None:
        s = ""
    else:
        # strip leading/trailing whitespace, and normalise the rest
        s = s.strip()
        s = re.sub(r'\s+', ' ', s, flags=re.MULTILINE)
    return s

def format_date (date_object):
    """Normalise a date to YYYY-MM-DD (ISO 8601)
    """
    return date_object.strftime("%Y-%m-%d")

def get_week_start (date_object):
    """Parse a raw date string and return the start of the week it occurs in
    """
    # when was the previous (or current) Sunday?
    return date_object + dateutil.relativedelta.relativedelta(
        date_object,
        weekday = dateutil.relativedelta.SU(-1)
    )


#
# Set up variables for batch processing
#

output = csv.writer(sys.stdout)
skipped_count = 0
total_count = 0
tweet_ids_seen = set()

#
# write the CSV header rows (text headers and HXL hashtags)
#

output.writerow([
    'Tweet date',
    'Week starting',
    'Language',
    'Location string',
    'Country code',
])

output.writerow([
    "#date+posted",
    "#date+week_start",
    "#meta+lang",
    "#loc+name",
    "#country+code+v_iso2",
])

#
# Iterate over the data (one JSON object on each line)
#

for line in sys.stdin:

    # progress info in terminal
    if total_count > 0 and (total_count % 10000) == 0:
        logger.info("Read %d tweets (%d skipped)...", total_count, skipped_count)

    # parse the JSON
    record = json.loads(line)
    total_count += 1

    # check that we haven't see this already (in this run)
    if record['id'] in tweet_ids_seen:
        skipped_count += 1
        continue
    else:
        tweet_ids_seen.add(record['id'])

    # If no label info, skip
    if 'aidr' not in record or 'nominal_labels' not in record['aidr']:
        skipped_count += 1
        continue
    
    label = record['aidr']['nominal_labels'][0]['label_code']

    # if wrong label
    if label != 'related_to_education_insecurity':
        skipped_count += 1
        continue

    # if we get to here, we have a relevant tweet; grab some fields
    language_code = record['lang']
    date_object = dateutil.parser.parse(record['created_at'])
    location_string = normalise_whitespace(record['user']['location'])

    # see if we already have a country code
    country_code = ''
    place_object = record.get('place')
    if place_object is not None:
        country_code = place_object.get('country_code')

    # write to CSV
    output.writerow([
        format_date(date_object),
        format_date(get_week_start(date_object)),
        language_code,
        location_string,
        country_code,
    ])

logger.info("Read %d total tweets", total_count)
if skipped_count > 0:
    logger.warn("Skipped %d tweets with no label information", skipped_count)
