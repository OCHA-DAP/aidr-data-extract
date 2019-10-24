import argparse, csv, dateutil.parser, ggeocode.coder, json, logging, re, sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("search-tweets")

def write_result(record, csvout, tweet_text, args):

    # country code
    iso2 = ''
    if args.name_map:
        result = ggeocode.coder.code(tweet_text, 5)
        if result['status'] and len(result['countries']) == 1:
            iso2 = result['countries'][0]

    # classifier
    classifier = ''
    confidence = ''
    try:
        classifier = record['aidr']['nominal_labels'][0]['label_code']
        confidence = record['aidr']['nominal_labels'][0]['confidence']
    except:
        pass

    # tweet date
    date_object = dateutil.parser.parse(record['created_at'])
    
    csvout.writerow([
        date_object.strftime('%Y-%m-%d'),
        '@' + record['user']['screen_name'],
        iso2,
        classifier,
        confidence,
        tweet_text,
    ])

def process_record (record, csvout, args):
    if 'extended_tweet' in record:
        tweet_text = record['extended_tweet']['full_text']
    elif 'retweeted_status' in record and 'extended_tweet' in record['retweeted_status']:
        tweet_text = record['retweeted_status']['extended_tweet']['full_text']
    else:
        tweet_text = record['text']

    for query in args.query:
        logger.debug("Trying %s", query)
        if not re.search(query, tweet_text, re.IGNORECASE):
            return False

    write_result(record, csvout, tweet_text, args)
    return True


def process_input_stream (input_stream, csvout, args):
    for line in input_stream:
        process_record(json.loads(line), csvout, args)

def process (output_stream, args):
    csvout = csv.writer(output_stream)
    csvout.writerow([
        'Date',
        'User',
        'Country',
        'Classifier',
        'Confidence',
        'Tweet',
    ])
    if args.json_file:
        for file in args.json_file:
            with open(file, 'r') as input_stream:
                process_input_stream(input_stream, csvout, args)
    else:
        process_input_stream(sys.stdin, csvout, args)



#
# If called as a command-line script ...
#
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description="Search AIDR tweet data")

    arg_parser.add_argument("-q", "--query", action="append", required=True, help="Search query for tweet text (regular expression)")
    arg_parser.add_argument("-n", "--name-map", required=False, help="Filename of the compiled ggeocode JSON name map (if not included, the script will not geocode locations")
    arg_parser.add_argument("-o", "--output", required=False, help="name of the output file (defaults to standard output)")
    arg_parser.add_argument("json_file", nargs="*")

    args = arg_parser.parse_args()

    # If the caller provided a JSON name map, load it and enable geocoding
    if args.name_map is not None:
        ggeocode.coder.load_name_map(args.name_map)

    # Output to a file or to stdout if none specified
    if args.output:
        with open(args.output, 'w') as output_stream:
            process(output_stream, args)
    else:
        process(sys.stdout, args)

# end
