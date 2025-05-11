#!/bin/bash

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

consumer_verified() {
  tables=$($IMPALA_EXEC "show tables")
  for i in {1..3}; do
    if ! grep -q "^${TBL_NAME_PREFIX}_$i$" <<< "$tables"; then
      echo "[$(date '+%Y-%m-%d %H:%M:%S')] ${TBL_NAME_PREFIX}_$i not found"
      return 1
    fi
    echo "Found ${TBL_NAME_PREFIX}_$i"
  done
  return 0
}

