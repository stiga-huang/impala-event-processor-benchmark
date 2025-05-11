#!/bin/bash

export TBL_NAME_PREFIX="hive_drop_tbl"
export NUM_TBLS=3

procuder() {
  SQL=""
  for i in `seq $NUM_TBLS`; do
    SQL="$SQL; create table if not exists ${TBL_NAME_PREFIX}_${i} (i int)"
  done
  $IMPALA_EXEC "$SQL"
  SQL=""
  for i in `seq $NUM_TBLS`; do
    SQL="$SQL; drop table ${TBL_NAME_PREFIX}_${i}"
  done
  $HIVE_EXEC "$SQL"
}

consumer_verified() {
  tables=$($IMPALA_EXEC "show tables")
  for i in {1..3}; do
    if grep -q "^${TBL_NAME_PREFIX}_$i$" <<< "$tables"; then
      echo "[$(date '+%Y-%m-%d %H:%M:%S')] ${TBL_NAME_PREFIX}_$i still exists"
      return 1
    fi
    echo "Removed ${TBL_NAME_PREFIX}_$i"
  done
  return 0
}

