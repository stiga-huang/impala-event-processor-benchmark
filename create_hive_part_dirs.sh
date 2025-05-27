#!/bin/bash

set -e

IMPALA_SHELL=impala-shell.sh
MAX_THREADS=32
NUM_PARTS=40000
DB_NAME=scale_40k_500cols_db
#NUM_PARTS=4000
#DB_NAME=scale_4k_500cols_db
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

echo "$(get_ts) Fetching table path"
TBL_PATH=$($IMPALA_SHELL -B --quiet -q "describe formatted $DB_NAME.$TBL_NAME" | grep -i location | awk '{print $2}')
echo "$(get_ts) Table path: $TBL_PATH"

echo "$(get_ts) Generating local files for $NUM_PARTS partitions using $MAX_THREADS threads"
mkdir $TBL_NAME
pushd "$TBL_NAME"
seq 0 $((NUM_PARTS - 1)) | parallel -j $MAX_THREADS create_dir_and_file "p={}" {}
popd

echo "$(get_ts) Uploading files to ${TBL_PATH}"
# Argument list will be too long if uploading more than 50000 dirs.
# Uploading them in batches.
export TBL_PATH
upload_to_hdfs() {
  hdfs dfs -put -t 32 -l "$@" "${TBL_PATH}/"
}
export -f upload_to_hdfs
find "$TBL_NAME" -name 'p=*' -print0 | xargs -0 -n 50000 bash -c 'upload_to_hdfs "$@"' _

echo "$(get_ts) Done"
echo "For large NUM_PARTS, mirror files to other tables by distcp, E.g."
echo "  hadoop distcp -update ${TBL_PATH} new_tbl_path"
echo "For small NUM_PARTS, using HDFS CLI is faster, E.g."
echo "  hdfs dfs -put -t 32 -l $TBL_NAME/p=* new_tbl_path"
