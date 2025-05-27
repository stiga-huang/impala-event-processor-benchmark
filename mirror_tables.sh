#!/bin/bash

set -e

IMPALA_SHELL=impala-shell.sh
DB_NAME=scale_40k_500cols_db
NUM_PARTS=40000
NUM_TBLS=20
TBL_NAME_PREFIX=tbl


SRC_TBL_NAME="$DB_NAME.${TBL_NAME_PREFIX}1"
SRC_TBL_PATH=$($IMPALA_SHELL -B --quiet -q "describe formatted $SRC_TBL_NAME" | grep -i location | awk '{print $2}')

$IMPALA_SHELL -B --quiet -q "alter table $SRC_TBL_NAME recover partitions" &

for i in `seq 2 $NUM_TBLS`; do
  TBL_NAME="$DB_NAME.${TBL_NAME_PREFIX}${i}"
  TBL_PATH=$($IMPALA_SHELL -B --quiet -q "describe formatted $TBL_NAME" | grep -i location | awk '{print $2}')
  if [[ "$TBL_PATH" == "" ]]; then
    echo "Table path not found!"
    exit 1
  fi
  hadoop distcp -update "$SRC_TBL_PATH" "$TBL_PATH"
  $IMPALA_SHELL -B --quiet -q "alter table $TBL_NAME recover partitions" &
done

wait
