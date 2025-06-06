#!/bin/bash

export URL_PATH="/catalog_object?json&object_type=DATABASE&object_name="
export DB_PREFIX="hive_drop_db_$(uuidgen | cut -c 1-8)"
export NUM_DBS=1

procuder() {
  SQL=""
  for i in `seq $NUM_DBS`; do
    SQL="$SQL create database if not exists ${DB_PREFIX}${i};"
  done
  $IMPALA_EXEC "$SQL"
  SQL=""
  for i in `seq $NUM_DBS`; do
    SQL="$SQL drop database ${DB_PREFIX}${i};"
  done
  $HIVE_EXEC "$SQL"
}

consumer_verified_old() {
  dbs=$($IMPALA_EXEC "show databases")
  for i in `seq $NUM_DBS`; do
    if grep -q "^${DB_PREFIX}$i"$'\t' <<< "$dbs"; then
      echo "$(get_ts) db$i still exists"
      return 1
    fi
    echo "$(get_ts) Removed db$i"
  done
  return 0
}

manual_refresh() {
  # Impala doesn't support invalidating a db. Since the db has been dropped, we can only
  # use global invalidate.
  $IMPALA_EXEC "invalidate metadata"
}

consumer_verified() {
  for i in `seq $NUM_DBS`; do
    # Old versions of Impala have key 'thrift_json'. Newer versions have 'json_string'.
    set -x
    if $CURL "${CATALOG_URL}${URL_PATH}${DB_PREFIX}${i}" | jq -e ".json_string // .thrift_string"; then
      set +x
      echo "$(get_ts) ${DB_PREFIX}${i} still exists"
      sleep 0.01
      return 1
    fi
    set +x
    echo "$(get_ts) Removed ${DB_PREFIX}${i}"
  done
}
