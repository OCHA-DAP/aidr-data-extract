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
########################################################################

today=$(date +%Y%m%d)

threshold=0.9

. $HOME/.virtualenvs/aidr/bin/activate

for lang in ar en fr; do
    tweets="output/$today-tweets-$lang.csv"
    report="reports/$today-report-$lang.csv"
    echo Generating $tweets ... \
        && python extract-aidr-data.py -t "$threshold" -o "$tweets" aidr-data/$lang/*.json \
        && echo Generating $report ... \
        && hxlcount -t date+week_start,date+posted,meta+lang $tweets > $report
done

report="reports/$today-report-all.csv"

echo Generating $report
hxlappend -a reports/$today-report-ar.csv -a reports/$today-report-fr.csv reports/$today-report-en.csv  \
    | hxlcount -t date+week_start,meta+lang -a 'sum(#meta+count) as #indicator+tweets' > $report
