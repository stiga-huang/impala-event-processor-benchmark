#!/bin/bash

export URL_PATH="/catalog_object?json&object_type=DATABASE&object_name="
export DB_PREFIX="hive_created_db_$(uuidgen | cut -c 1-8)"
export NUM_DBS=1

procuder() {
  SQL=""
  for i in `seq $NUM_DBS`; do
    SQL="$SQL drop database if exists ${DB_PREFIX}${i};"
  done
  $IMPALA_EXEC "$SQL"
  SQL=""
  for i in `seq $NUM_DBS`; do
    SQL="$SQL create database ${DB_PREFIX}${i};"
  done
  $HIVE_EXEC "$SQL"
}

consumer_verified_old() {
  dbs=$($IMPALA_EXEC "show databases")
  for i in `seq $NUM_DBS`; do
    if ! grep -q "^${DB_PREFIX}${i}"$'\t' <<< "$dbs"; then
      echo "$(get_ts) ${DB_PREFIX}${i} not found"
      return 1
    fi
    echo "Found db$i"
  done
  return 0
}

consumer_verified() {
  for i in `seq $NUM_DBS`; do
    # Old versions of Impala have key 'thrift_json'. Newer versions have 'json_string'.
    set -x
    if ! $CURL "${CATALOG_URL}${URL_PATH}${DB_PREFIX}${i}" | jq -e ".json_string // .thrift_string"; then
      set +x
      echo "$(get_ts) ${DB_PREFIX}${i} not found"
      sleep 0.01
      return 1
    fi
    set +x
    echo "$(get_ts) Found ${DB_PREFIX}${i}"
  done
}

manual_refresh() {
  # Impala doesn't support invalidating a db. We have to create a table under it and
  # invalidate the table instead. That will bring the db up.
  for i in `seq $NUM_DBS`; do
    $HIVE_EXEC "create table ${DB_PREFIX}${i}.tbl (i int)"
    $IMPALA_EXEC "invalidate metadata ${DB_PREFIX}${i}.tbl"
  done
}

cleanup() {
  SQL=""
  for i in `seq $NUM_DBS`; do
    SQL="$SQL drop database if exists ${DB_PREFIX}${i} cascade;"
  done
  $HIVE_EXEC "$SQL"
}
