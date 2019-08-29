""" Extract data from an AIDR JSON file.

Usage:

    $ python3 extract-aidr-data.py -o output.csv -t 0.9 *.json

Started 2019-07-25 by David Megginson
"""

import argparse, csv, dateutil.parser, dateutil.relativedelta, json, logging, re, sys

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
# JSON processing
#

def process_file (input_stream, csv_out, status):
    """ Process one input JSON-formatted AIDR file
    @param input_stream: a bytewise input stream to read from
    @param csv_out: a CSV writer for output
    @param status: a simple object with persistent status variables
    """

    # each line contains a JSON object
    for line in input_stream:

        # progress info in terminal
        if status.total_count > 0 and (status.total_count % 10000) == 0:
            logger.info("Read %d tweets (%d skipped)...", status.total_count, status.skipped_count)

        # parse the JSON
        try:
            record = json.loads(line)
        except:
            logger.warning("Failed to parse JSON record (possibly incomplete at end of file)")

        status.total_count += 1

        # check that we haven't see this already (in this run)
        if record['id'] in status.tweet_ids_seen:
            status.skipped_count += 1
            continue
        else:
            status.tweet_ids_seen.add(record['id'])

        # If no label info, skip
        if 'aidr' not in record or 'nominal_labels' not in record['aidr']:
            status.skipped_count += 1
            continue

        label = record['aidr']['nominal_labels'][0]['label_code']
        confidence = record['aidr']['nominal_labels'][0]['confidence']

        # if wrong label or not confident
        if label != status.classifier or confidence < status.threshold:
            status.skipped_count += 1
            continue

        # if we get to here, we have a relevant tweet; grab some fields
        if status.include_text:
            if 'extended_tweet' in record:
                tweet_text = record['extended_tweet']['full_text']
            else:
                tweet_text = record['text']
        else:
            tweet_text = ""
        language_code = record['lang']
        date_object = dateutil.parser.parse(record['created_at'])
        location_string = normalise_whitespace(record['user']['location'])

        # see if we already have a country code
        country_code = ''
        place_object = record.get('place')
        if place_object is not None:
            country_code = place_object.get('country_code')

        # write to CSV
        csv_out.writerow([
            format_date(date_object),
            format_date(get_week_start(date_object)),
            language_code,
            confidence,
            tweet_text,
            location_string,
            country_code,
        ])


def process_tweets (input_files=None, output_file=None, classifier='related_to_education_insecurity', threshold=0.9, include_text=False):
    """ Process the JSON twitter data and produce HXL-hashtagged CSV output.
    @param input_files: a list of input filenames to read (if None, use sys.stdin)
    @param output: the output filename (if None, default to sys.stdout)
    @param classifer: the classifier string from AIDR (defaults to "related_to_education_insecurity")
    @param threshold: the confidence threshold for relevance (defaults to 0.9, 90%).
    @param include_text: if True, include the full tweet text (defaults to False).
    """

    # set up a simple class to hold status variables
    class Struct(object):
        pass
    status = Struct()
    status.classifier = classifier
    status.threshold = threshold
    status.include_text = include_text
    status.total_count = 0
    status.skipped_count = 0
    status.tweet_ids_seen = set()

    # output file stays open for the whole run
    with (open(output_file, "w") if output_file else sys.stdout) as output:

        csv_out = csv.writer(output)

        # write the CSV header rows (text headers and HXL hashtags)
        csv_out.writerow([
            'Tweet date',
            'Week starting',
            'Language',
            'Confidence',
            'Text',
            'Location string',
            'Country code',
        ])

        csv_out.writerow([
            "#date+posted",
            "#date+week_start",
            "#meta+lang",
            "#indicator+confidence+num",
            "#description+tweet",
            "#loc+name",
            "#country+code+v_iso2",
        ])

        # Loop through the input files
        if input_files:
            # if we have input filenames, walk through the list
            for filename in input_files:
                with open(filename, "r") as input:
                    process_file(input, csv_out, status)
        else:
            # otherwise, read from standard input
            process_file(sys.stdin, csv_out, status)


        # log an end message
        logger.info("Read %d total tweets", status.total_count)
        if status.skipped_count > 0:
            logger.warning("Skipped %d tweets with no label information or low confidence", status.skipped_count)



#
# If called as a command-line script ...
#
if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description="Extract AIDR tweet data")
    arg_parser.add_argument("-i", "--include-text", action="store_true", help="Include the full tweet text in the output")
    arg_parser.add_argument("-t", "--threshold", type=float, default=0.9, help="Minimum confidence threshold (0.0-1.0)")
    arg_parser.add_argument("-o", "--output", required=False, help="name of the output file (defaults to standard output)")
    arg_parser.add_argument("json_file", nargs="*")

    args = arg_parser.parse_args()

    process_tweets(
        input_files = args.json_file,
        output_file = args.output,
        threshold = args.threshold,
        include_text=args.include_text
    )

# end
