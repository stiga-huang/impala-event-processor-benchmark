#!/bin/bash

set -e

#HIVE_EXEC="beeline -e"
#IMPALA_EXEC="impala-shell -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -B --quiet -q"

# Clients in Impala Dev Env
HIVE_EXEC="beeline -u jdbc:hive2://localhost:11050 -e"
IMPALA_EXEC="impala-shell.sh -B --quiet -q"

cleanup() {
  : # No-op by default
}

source $1

get_ts() {
  date '+%Y-%m-%d %H:%M:%S'
}

procuder

start_time=$(date +%s.%3N)
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Procuder done $start_time"

while ! consumer_verified; do
  :
done

end_time=$(date +%s.%3N)
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Synced $end_time"
duration=$(echo "$end_time - $start_time" | bc)
echo
echo "$1>>>>>>>>>>$duration"
echo

cleanup
