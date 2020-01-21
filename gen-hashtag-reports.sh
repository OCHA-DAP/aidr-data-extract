#!/bin/bash
########################################################################
# Generate reports from the AIDR JSON files for hashtag campaigns.
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

# Ditto for this month
this_month=$(date +%Y-%m)

# Minimum AIDR tweet-classification certainty (0.00-1.00)
threshold=0.8

# Precompiled ggeocode database for geocoding
names=./inputs/name-country-map.lines.json

# Spambot account list
bots=./inputs/spambots.txt

# Segment replacement table
segments=./inputs/segment-replacement-table.csv

# Extract the data 
FILES=$(ls aidr-data/hashtag-campaigns/all-languages/*.json)

# Extract
tweets="output/$today-tweets-hashtags.csv"
echo Generating $tweets ... \
    && python extract-aidr-data.py -i -D -R -n "$names" -b "$bots" -t "$threshold" -o "$tweets" $FILES

# Make reports

function make_report {
    report="reports/$today-hashtags-$2-$3-$4.csv"
    if [ "X$4" = "Xweekly" ]; then
        filter="date+week_start<$this_week_start"
    else
        filter="date+month<$this_month"
    fi
    echo "Generating $report..."
    hxlcount -q "$filter" -t "$1" "$tweets" \
        | hxladd -s 'Segment#indicator+count_segment={{#meta+count}}' \
        | hxlreplace -m "$segments" \
                     > $report
}

echo "Generating monthly reports..."
make_report date+month global all monthly
make_report date+month,country+code countries all monthly
make_report date+month,country+code,meta+lang countries languages monthly

echo "Generating weekly reports..."
make_report date+week_start global all weekly
make_report date+week_start,country+code countries all weekly
make_report date+week_start,country+code,meta+lang countries languages weekly


exit 0
# end
