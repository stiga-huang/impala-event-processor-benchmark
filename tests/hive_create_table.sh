#!/bin/bash

export CATALOG_URL="http://localhost:25020/catalog_object?json&object_type=TABLE&object_name=default."
export CURL="curl -s"
export TBL_NAME_PREFIX="hive_created_tbl"

procuder() {
  SQL=""
  for i in {1..3}; do
    SQL="$SQL; drop table if exists ${TBL_NAME_PREFIX}_${i}"
  done
  $IMPALA_EXEC "$SQL"
  SQL=""
  for i in {1..3}; do
    SQL="$SQL; create table ${TBL_NAME_PREFIX}_${i} (i int)"
  done
  $HIVE_EXEC "$SQL"
}

consumer_verified_old() {
  tables=$($IMPALA_EXEC "show tables")
  for i in {1..3}; do
    if ! grep -q "^${TBL_NAME_PREFIX}_$i$" <<< "$tables"; then
      echo "$(get_ts) ${TBL_NAME_PREFIX}_$i not found"
      return 1
    fi
    echo "$(get_ts) Found ${TBL_NAME_PREFIX}_$i"
  done
  return 0
}

consumer_verified() {
  for i in {1..3}; do
    TBL_NAME="${TBL_NAME_PREFIX}_${i}"
    if ! $CURL "${CATALOG_URL}${TBL_NAME}" | jq -e ".json_string" > /dev/null; then
      echo "$(get_ts) ${TBL_NAME} not found"
      return 1
    fi
    echo "$(get_ts) Found ${TBL_NAME}"
  done
}
