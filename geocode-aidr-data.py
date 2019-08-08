""" Geocode location strings from Twitter profiles (via AIDR)

This script expects the input data to be in a HXL-hashtagged CSV file, with
columns matching the hashtag patterns #loc and #country+code+v_iso2

"""

import argparse, csv, geocoder, hxl, json, logging, re, sys, time

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

WHITESPACE_PATTERN = re.compile('\s+', re.MULTILINE)
"""Regular-expression pattern for multi-line whitespace."""

ACCURACY_THRESHOLD = 0.5


def normalise_string(s):
    """Normalise whitespace in a string"""
    if s is None:
        return None
    else:
        s = s.strip().replace("\n", " ")
        return re.sub(WHITESPACE_PATTERN, ' ', s)

    
def read_location_cache(filename):
    """Read a comma-delimited cache of previously-geocoded countries"""
    i = 0
    cache = {}
    with open(filename, 'r') as fd:
        input = csv.reader(fd)
        for row in input:
            cache[normalise_string(row[0])] = row[1]
            i += 1
    logger.info("Read %d cached entries", i)
    return cache


def write_location_cache(filename, cache):
    """Write a comma-delimited cache of previously-geocoded countries"""
    i = 0
    with open(filename, "w") as fd:
        output = csv.writer(fd)
        for key in cache:
            output.writerow([key, cache[key]])
            i += 1
    logger.info("Wrote %d cached entries", i)

    
def geocode_location_string(s):
    """Attempt to geocode a location string"""
    time.sleep(1)
    try:
        g = geocoder.osm(s)
        if g.accuracy is not None and g.accuracy >= ACCURACY_THRESHOLD and g.country_code:
            return g.country_code
    except Exception as (e):
        logger.exception(e)

    return '' # insufficient accuracy or exception

def do_geocode(input_stream, output_stream, location_cache):

    # open the HXL dataset
    dataset = hxl.data(hxl.make_input(input_stream.buffer))

    # start the CSV output
    csv_out = csv.writer(output_stream)

    # precache indices for faster processing
    location_index = hxl.TagPattern.parse("#loc").find_column_index(dataset.columns)
    if location_index is None:
        raise Exception("Dataset does not contain #loc column")

    country_index = hxl.TagPattern.parse("#country+code+v_iso2").find_column_index(dataset.columns)
    if country_index is None:
        raise Exception("Dataset does not contain #country+code+v_iso2 column")

    csv_out.writerow(dataset.headers)
    csv_out.writerow(dataset.display_tags)

    # go through the dataset row by row
    for row in dataset:
        
        country_code = row.values[country_index]

        # if we already have a country code, we're done
        if not country_code:

            # do we have a location string?
            location_string = normalise_string(row.values[location_index])
            if location_string:
                if not location_string:
                    country_code = ''
                elif location_string in location_cache:
                    country_code = location_cache[location_string]
                else:
                    country_code = geocode_location_string(location_string)
                    location_cache[location_string] = country_code # cache empty values so we don't repeat the query

        # update the country code
        row.values[country_index] = country_code
        csv_out.writerow(row.values)

    # return the updated cache
    return location_cache


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description="Geocode AIDR tweet data")
    arg_parser.add_argument("--cache-in", required=False, help="name of the cache file to read before processing")
    arg_parser.add_argument("--cache-out", required=False, help="name of the cache file to write after processing")
    arg_parser.add_argument("--output", "-o", required=False, help="name of output file (defaults to standard output)")
    arg_parser.add_argument("input", nargs="?")
    
    args = arg_parser.parse_args()

    if args.cache_in:
        location_cache = read_location_cache(args.cache_in)
    else:
        location_cache = {}

    try:
        if args.input:
            input_stream = open(args.input, "r")
        else:
            input_stream = sys.stdin
        if args.output:
            output_stream = open(args.output, "w")
        else:
            output_stream = sys.stdout
        location_cache = do_geocode(input_stream, output_stream, location_cache)
    finally:
        if input_stream != sys.stdin:
            input_stream.close()
        if output_stream != sys.stdout:
            output_stream.close()

    if args.cache_out:
        write_location_cache(args.cache_out, location_cache)

