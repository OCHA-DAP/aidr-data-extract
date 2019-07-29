import csv, dateutil.parser, dateutil.relativedelta, json, logging, re, sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("extract-aidr-data")

#
# Utility functions
#

def clean(s):
    """Clean whitespace in a string, and handle None"""
    if s is None:
        s = ""
    else:
        # strip leading/trailing whitespace, and normalise the rest
        s = s.strip()
        s = re.sub(r'\s+', ' ', s, flags=re.MULTILINE)
    return s

def get_week_start (date_string):
    """Parse a raw date string and return the start of the week it occurs in"""
    if not date_string:
        return ""
    date = dateutil.parser.parse(date_string)
    # when was the previous (or current) Sunday?
    week_start_date = date + dateutil.relativedelta.relativedelta(
        date,
        weekday = dateutil.relativedelta.SU(-1)
    )
    return week_start_date.strftime("%Y-%m-%d")
    

#
# Main processing loop
#
output = csv.writer(sys.stdout)
language_string = 'en' # need to change for other languages
skipped_count = 0
total_count = 0

# write the CSV header row
output.writerow([
    'Week starting',
    'Language',
    'Location string'
])

# each line is a JSON record
for line in sys.stdin:
    if total_count > 0 and (total_count % 10000) == 0:
        logger.info("Read %d tweets (%d skipped)...", total_count, skipped_count)
    
    record = json.loads(line)
    total_count += 1

    # If no label info, skip
    if 'aidr' not in record or 'nominal_labels' not in record['aidr']:
        skipped_count += 1
        continue

    label = record['aidr']['nominal_labels'][0]['label_code']
    if label == 'related_to_education_insecurity':
        date_string = clean(record['created_at'])
        location_string = clean(record['user']['location'])
        output.writerow([
            get_week_start(date_string),
            language_string,
            location_string
        ])

logger.info("Read %d total tweets", total_count)
if skipped_count > 0:
    logger.warn("Skipped %d tweets with no label information", skipped_count)
