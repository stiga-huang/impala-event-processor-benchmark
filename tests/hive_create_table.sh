#!/bin/bash

export URL_PATH="/catalog_object?json&object_type=TABLE&object_name=default."
export TBL_NAME_PREFIX="hive_created_tbl_$(uuidgen | cut -c 1-8)"
export NUM_TABLES=1

procuder() {
  SQL=""
  for i in `seq $NUM_TABLES`; do
    SQL="$SQL; drop table if exists ${TBL_NAME_PREFIX}_${i}"
  done
  $IMPALA_EXEC "$SQL"
  SQL=""
  for i in `seq $NUM_TABLES`; do
    SQL="$SQL; create table ${TBL_NAME_PREFIX}_${i} (i int)"
  done
  $HIVE_EXEC "$SQL"
}

consumer_verified_old() {
  tables=$($IMPALA_EXEC "show tables")
  for i in `seq $NUM_TABLES`; do
    if ! grep -q "^${TBL_NAME_PREFIX}_$i$" <<< "$tables"; then
      echo "$(get_ts) ${TBL_NAME_PREFIX}_$i not found"
      return 1
    fi
    echo "$(get_ts) Found ${TBL_NAME_PREFIX}_$i"
  done
  return 0
}

consumer_verified() {
  for i in `seq $NUM_TABLES`; do
    TBL_NAME="${TBL_NAME_PREFIX}_${i}"
    # Old versions of Impala have key 'thrift_json'. Newer versions have 'json_string'.
    set -x
    if ! $CURL "${CATALOG_URL}${URL_PATH}${TBL_NAME}" | jq -e ".json_string // .thrift_string"; then
      set +x
      echo "$(get_ts) ${TBL_NAME} not found"
      sleep 0.01
      return 1
    fi
    set +x
    echo "$(get_ts) Found ${TBL_NAME}"
  done
}

manual_refresh() {
  for i in `seq $NUM_TABLES`; do
    TBL_NAME="${TBL_NAME_PREFIX}_${i}"
    $IMPALA_EXEC "invalidate metadata $TBL_NAME"
  done
}

cleanup() {
  SQL=""
  for i in `seq $NUM_TABLES`; do
    SQL="$SQL; drop table if exists ${TBL_NAME_PREFIX}_${i}"
  done
  $HIVE_EXEC "$SQL"
}
