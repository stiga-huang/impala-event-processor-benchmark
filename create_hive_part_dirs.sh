#!/bin/bash

set -e

IMPALA_SHELL=impala-shell.sh
MAX_THREADS=32
#NUM_PARTS=40000
#DB_PATH=hdfs://ns1/warehouse/tablespace/external/hive/scale_40k_500cols_db.db
NUM_PARTS=4000
DB_NAME=scale_4k_500cols_db
TBL_NAME=tbl1
DATA_FILE=500_cols.parq

export DATA_FILE=$(realpath $DATA_FILE)
if ! [ -f "$DATA_FILE" ]; then
  echo "Source data file $DATA_FILE not found"
  exit 1
fi

get_ts() {
  date '+%Y-%m-%d %H:%M:%S'
}

generate_filename() {
  local part_num=$(printf "%05d" $1)
  local uuid1=$(uuidgen)
  local uuid2=$(uuidgen)
  echo "part-${part_num}-${uuid1}-${uuid2}.parq"
}

create_dir_and_file() {
  local dir_name=$1
  mkdir -p "$dir_name"
  local filename=$(generate_filename $2)
  cp "$DATA_FILE" "$dir_name/$filename"
}

export -f generate_filename
export -f create_dir_and_file

echo "$(get_ts) Fetching db path"
DB_PATH=$($IMPALA_SHELL -B --quiet -q "describe database $DB_NAME" | awk '{print $2}')
echo "$(get_ts) Db path: $DB_PATH"

echo "$(get_ts) Generating local files for $NUM_PARTS partitions using $MAX_THREADS threads"
mkdir $TBL_NAME
pushd "$TBL_NAME"
seq 0 $((NUM_PARTS - 1)) | parallel -j $MAX_THREADS create_dir_and_file "p={}" {}
popd

echo "$(get_ts) Uploading files to ${DB_PATH}/${TBL_NAME}/"
# This doesn't work if NUM_PARTS > 50000 due to argument list too long.
# Manually split it into several commands like
#  hdfs dfs -put -t 32 -l part_400k/p={0..49999} tbl_dir
#  hdfs dfs -put -t 32 -l part_400k/p={50000..99999} tbl_dir
#  ...
hdfs dfs -put -t 32 -l "$TBL_NAME"/p=* "${DB_PATH}/${TBL_NAME}/"

echo "$(get_ts) Done"
echo "For large NUM_PARTS, mirror files to other tables by distcp, E.g."
echo "  hadoop distcp -update ${DB_PATH}/${TBL_NAME} ${DB_PATH}/new_tbl"
echo "For small NUM_PARTS, using HDFS CLI is faster, E.g."
echo "  hdfs dfs -put -t 32 -l $TBL_NAME/p=* ${DB_PATH}/new_tbl"
