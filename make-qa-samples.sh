#!/bin/sh
########################################################################
# Generate QA samples from the last week for each language.
#
# The sample sizes will be approximately 100 tweets each, but it's
# not exact.
#
# Results will be in reports/TODAY-qa-sample-LANG.csv
#
# Started 2019-10-09
########################################################################

# Activate a Python virtualenv (change or remove if desired)
. $HOME/.virtualenvs/aidr/bin/activate

# Today's date (to be prefixed to generated data)
today=$(date +%Y%m%d)

# Minimum AIDR tweet-classification certainty (0.00-1.00)
threshold=0.8

# Approximate sample size
sample_size=500

# Precompiled ggeocode database for geocoding (not used right now)
# names=./inputs/name-country-map.lines.json

# Spambot account list
bots=./inputs/spambots.txt

# Precompiled ggeocode database for geocoding
names=./inputs/name-country-map.lines.json

# Allowed countries list
countries=./inputs/countries.txt

# Target week start
target_week=$(date +%Y-%m-%d -d "$(date -d 'last Sunday') -1 week")

# Working directory
dir=$(mktemp -d)

echo "Making samples for week starting $target_week" >&2
for lang in ar en fr; do
    output=reports/$today-qa-sample-$lang.csv
    echo Generating $lang in $output ... >&2
    python extract-aidr-data.py -D -R -i -n "$names" -b "$bots" -C "$countries" -t "$threshold" aidr-data/$lang/*.json \
        | hxlcut -i description+tweet,loc+name,country+code,meta+location_source \
        | python make-sample.py "$sample_size" > "$output"
done
