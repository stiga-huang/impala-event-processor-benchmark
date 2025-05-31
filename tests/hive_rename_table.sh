#!/bin/bash

export URL_PATH="/catalog_object?json&object_type=TABLE&object_name="

procuder() {
  TBL=${1:-tbl1}
  echo "$(get_ts) Hive> Renaming table $DB.$TBL to $DB.${TBL}_tmp"
  $HIVE_EXEC "alter table $DB.$TBL rename to $DB.${TBL}_tmp"
}

consumer_verified() {
  TBL=${1:-tbl1}
  TBL_NAME=$DB.$TBL
  set -x
  if $CURL "${CATALOG_URL}${URL_PATH}${TBL_NAME}" | jq -e ".json_string" > /dev/null; then
    set +x
    echo "$(get_ts) ${TBL_NAME} still exists"
    sleep 0.01
    return 1
  fi
  echo "$(get_ts) ${TBL_NAME} removed"
  TBL_NAME=${TBL_NAME}_tmp
  set -x
  if $CURL "${CATALOG_URL}${URL_PATH}${TBL_NAME}" | jq -e ".json_string" > /dev/null; then
    set +x
    echo "$(get_ts) Found $TBL_NAME"
    sleep 0.01
    return 0
  fi
  echo "$(get_ts) Table $TBL_NAME not found"
  return 1
}

manual_refresh() {
  TBL=${1:-tbl1}
  $IMPALA_EXEC "invalidate metadata $DB.$TBL; invalidate metadata $DB.${TBL}_tmp"
}

cleanup() {
  TBL=${1:-tbl1}
  $HIVE_EXEC "alter table $DB.${TBL}_tmp rename to $DB.$TBL"
  $IMPALA_EXEC "invalidate metadata $DB.$TBL; invalidate metadata $DB.${TBL}_tmp"
}
