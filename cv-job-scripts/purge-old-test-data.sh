#!/bin/bash

function remove_superfluous_log_files () {
  # Remove all directories as this data won't be used
  # This includes PCAPs and cbcollects
  rm -rf "${1:?}"/*/
  # Remove log files
  rm -f "$1"/*.log
  # Remove all the remaining Jepsen produced log files apart from results.edn which is needed for the web server
  rm -rf "$1/history.txt" "$1/history.edn" "$1/timeline.html"
}

# Purge all Job data in the store older than 30 days
find ./store -type d -depth 1 -mtime +30 -exec rm -rf {} \;
# Purge the logs for any tests that crash more than a week ago
REMAING_TEST_LOGS=$(find ./store -type d -depth 2 -mtime +7)
for d in $REMAING_TEST_LOGS; do
  if [[ $(grep -c -E ':valid\? (:unknown|true)}' $d/results.edn) -gt 0 ]]; then
    remove_superfluous_log_files $d
  fi
done