#!/bin/bash
########################################################################
# Generate reports from the AIDR JSON files.
#
# JSON files must be in the following subdirectories:
#
# ./aidr-data/ar/
# ./aidr-data/en/
# ./aidr-data/fr/
#
# Expecting a Python virtual environment in $HOME/.virtualenvs/aidr/
#
# Expecting a ggeocode names database in
# ./inputs/name-country-map.lines.json
#
# Expecting a list of spambot accounts (one per line) in
# ./inputs/spambots.txt
########################################################################

# Activate a Python virtualenv (change or remove if desired)
. $HOME/.virtualenvs/aidr/bin/activate

# Today's date (to be prefixed to generated data)
today=$(date +%Y%m%d)

# Since this week is possibly incomplete, we want only tweets from
# weeks starting before the most-recent Sunday.
this_week_start=$(date +%Y-%m-%d -d 'last Sunday')

# Minimum AIDR tweet-classification certainty (0.00-1.00)
threshold=0.8

# Precompiled ggeocode database for geocoding
names=./inputs/name-country-map.lines.json

# Spambot account list
bots=./inputs/spambots.txt

# Allowed countries list
countries=./inputs/countries.txt

# Extract the data on individual tweets in each language
for lang in ar en fr; do
    tweets="output/$today-tweets-$lang.csv"
    global_report="reports/$today-report-global-$lang.csv"
    country_report="reports/$today-report-countries-$lang.csv"
    echo Generating $tweets ... \
        && python extract-aidr-data.py -D -R -n "$names" -b "$bots" -C "$countries" -t "$threshold" -o "$tweets" aidr-data/$lang/*.json \
        && echo Generating $global_report ... \
        && hxlcount -t date+week_start,date+posted,meta+lang $tweets > $global_report \
        && echo Generating $country_report ... \
        && hxlcount -t date+week_start,date+posted,meta+lang,country+code $tweets > $country_report
done

# Generate the global tweet counts by date
report="reports/$today-report-global-all.csv"

echo Generating $report
hxlappend -a reports/$today-report-global-ar.csv -a reports/$today-report-global-fr.csv reports/$today-report-global-en.csv  \
    | hxlcount -q "date+week_start<$this_week_start" -t date+week_start,meta+lang -a 'sum(#meta+count) as Tweets#indicator+tweets' > $report

# Generate the country tweet counts by date
report="reports/$today-report-countries-all.csv"

echo Generating $report
hxlappend -a reports/$today-report-countries-ar.csv -a reports/$today-report-countries-fr.csv reports/$today-report-countries-en.csv  \
    | hxlcount -q "date+week_start<$this_week_start" -t date+week_start,meta+lang,country+code -a 'sum(#meta+count) as Tweets#indicator+tweets' > $report

exit 0
# end
