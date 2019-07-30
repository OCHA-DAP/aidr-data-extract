""" Geocode location strings from Twitter profiles (via AIDR)
"""

import csv, geocoder, json, logging, re, sys, time

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

WHITESPACE_PATTERN = re.compile('\s+', re.MULTILINE)
"""Regular-expression pattern for multi-line whitespace."""

ACCURACY_THRESHOLD = 0.5

cache = {}

def load_cache(filename):
    i = 0
    with open(filename, 'r') as fd:
        input = csv.reader(fd)
        for row in input:
            cache[normalise_string(row[0])] = row[1]
            i += 1
    logger.info("Read %d cached entries", i)

def load_location_strings(filename):
    locations = []
    with open(filename, 'r') as input:
        raw_data = json.load(input)
        for obj in raw_data:
            if obj is not None:
                for location in obj.get('locations'):
                    if location is not None:
                        locations.append(location)
    return locations

def normalise_string(s):
    if s is None:
        return None
    else:
        s = s.strip().replace("\n", " ")
        return re.sub(WHITESPACE_PATTERN, ' ', s)
            
def geocode_location_string(s):
    s = normalise_string(s)
    if s is None or s is '':
        return None
    if s in cache:
        logger.debug("Found %s in cache", s)
        return cache[s]
    logger.debug("Did not find \"%s\" in cache; calling geocoder", s)
    g = geocoder.osm(s)
    time.sleep(1)
    if g.accuracy is not None and g.accuracy >= ACCURACY_THRESHOLD and g.country_code:
        cache[s] = g.country_code
        return g.country_code
    else:
        cache[s] = None # so that we don't repeat the query
        return None

def do_geocode(cache_filename, location_strings_filename):
    load_cache(cache_filename)
    output = csv.writer(sys.stdout)
    location_strings = load_location_strings(location_strings_filename)
    i = 0
    for location_string in location_strings:
        s = normalise_string(location_string)
        iso2 = geocode_location_string(s)
        output.writerow([s, iso2])
        i += 1
        if (1 % 10) == 0:
            logger.info("Processed %d locations", i)

if __name__ == '__main__':
    cache_filename = sys.argv[1]
    location_strings_filename = sys.argv[2]
    do_geocode(cache_filename, location_strings_filename)
