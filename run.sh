#!/bin/bash

set -e

#HIVE_EXEC="beeline -e"
#IMPALA_EXEC="impala-shell -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem -B --quiet -q"
#CURL="curl -s --cacert /var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem --negotiate -u : "
#CATALOG_URL="https://vb1404.halxg.cloudera.com:25020"

# Clients in Impala Dev Env
HIVE_EXEC="beeline -u jdbc:hive2://localhost:11050 -e"
IMPALA_EXEC="impala-shell.sh -B --quiet -q"
CURL="curl -s"
CATALOG_URL="http://localhost:25020"

DB=scale_4k_500cols_db
MANUAL_REFRESH=${MANUAL_REFRESH:-false}

# Column list used in statements
COLS="col0"
for i in {1..499}; do
  COLS="$COLS,col$i"
done

cleanup() {
  : # No-op by default
}

source $1
TEST_CASE=$1
shift

get_ts() {
  date '+%Y-%m-%d %H:%M:%S'
}

procuder $@

start_time=$(date +%s.%3N)
echo "$(get_ts) Procuder done $start_time"

if [[ "$MANUAL_REFRESH" == "true" ]]; then
  manual_refresh $@
  echo "$(get_ts) Manual refresh finished"
fi

while ! consumer_verified $@; do
  :
done

end_time=$(date +%s.%3N)
echo "$(get_ts) Synced $end_time"
duration=$(echo "$end_time - $start_time" | bc)
echo
echo "$TEST_CASE>>>>>>>>>>$duration"
echo

cleanup $@
