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
# ./geodata/name-country-map.lines.json
########################################################################

today=$(date +%Y%m%d)

threshold=0.9

names=./geodata/name-country-map.lines.json

. $HOME/.virtualenvs/aidr/bin/activate

for lang in ar en fr; do
    tweets="output/$today-tweets-$lang.csv"
    global_report="reports/$today-report-global-$lang.csv"
    country_report="reports/$today-report-countries-$lang.csv"
    echo Generating $tweets ... \
        && python extract-aidr-data.py -n "$names" -t "$threshold" -o "$tweets" aidr-data/$lang/*.json \
        && echo Generating $global_report ... \
        && hxlcount -t date+week_start,date+posted,meta+lang $tweets > $global_report \
        && echo Generating $country_report ... \
        && hxlcount -t date+week_start,date+posted,meta+lang,country+code $tweets > $country_report
done

report="reports/$today-report-global-all.csv"

echo Generating $report
hxlappend -a reports/$today-report-global-ar.csv -a reports/$today-report-global-fr.csv reports/$today-report-global-en.csv  \
    | hxlcount -t date+week_start,meta+lang -a 'sum(#meta+count) as #indicator+tweets' > $report

report="reports/$today-report-countries-all.csv"

echo Generating $report
hxlappend -a reports/$today-report-countries-ar.csv -a reports/$today-countries-global-fr.csv reports/$today-report-countries-en.csv  \
    | hxlcount -t date+week_start,meta+lang,country+code -a 'sum(#meta+count) as #indicator+tweets' > $report
