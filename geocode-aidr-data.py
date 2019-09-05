""" Geocode location strings from Twitter profiles (via AIDR)

This script expects the input data to be in a HXL-hashtagged CSV file, with
columns matching the hashtag patterns #loc and #country+code+v_iso2

Usage:

    python geocode-aidr-data --names=name-map.lines.json --output=tweets-geocoded.csv tweets-raw.csv

"""

import argparse, csv, ggeocode.coder, hxl, json, logging, re, sys, time

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def do_geocode(input_stream, output_stream):

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
    row_count = 0
    for row in dataset:
        
        location_string = row.values[location_index]
        country_code = row.values[country_index]

        # if we already have a country code, we're done
        if not country_code and location_string:
            result = ggeocode.coder.code(location_string, 3)
            if len(result) == 1:
                country_code = result[0]

        row.values[country_index] = country_code
        csv_out.writerow(row.values)

        row_count += 1
        if (row_count % 100000) == 0:
            logger.info("Processed %d rows", row_count)


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description="Geocode AIDR tweet data")
    arg_parser.add_argument("--name-map", "-n", required=True, help="Filename of the compiled names data.")
    arg_parser.add_argument("--output", "-o", required=False, help="name of output file (defaults to standard output)")
    arg_parser.add_argument("input", nargs="?")
    
    args = arg_parser.parse_args()

    # Load the name map
    ggeocode.coder.load_name_map(args.name_map)

    try:
        if args.input:
            input_stream = open(args.input, "r")
        else:
            input_stream = sys.stdin
        if args.output:
            output_stream = open(args.output, "w")
        else:
            output_stream = sys.stdout
        do_geocode(input_stream, output_stream)
    finally:
        if input_stream != sys.stdin:
            input_stream.close()
        if output_stream != sys.stdout:
            output_stream.close()

