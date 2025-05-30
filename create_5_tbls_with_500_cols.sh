#!/bin/bash

#DB=scale_400k_500cols_db
DB=scale_40k_500cols_db
#DB=scale_4k_500cols_db
NUM_TABLES=5
TMP_SQL=tmp.sql
IMPALA_SHELL=impala-shell.sh
#IMPALA_SHELL=impala-shell

# 20 data types
# Currently don't have BOOLEAN, TIMESTAMP and DATE since 500_cols.parq is converted
# from a text file using 0 as all columns.
DATA_TYPES=(
  "BIGINT"
  "INT"
  "SMALLINT"
  "TINYINT"
  "INT" #"BOOLEAN"
  "FLOAT"
  "DOUBLE"
  "STRING"
  "INT" #"TIMESTAMP"
  "INT" #"DATE"
  "DECIMAL(10,2)"
  "DECIMAL(10,4)"
  "DECIMAL(16,4)"
  "DECIMAL(16,6)"
  "DECIMAL(24,6)"
  "DECIMAL(24,8)"
  "DECIMAL(28,8)"
  "DECIMAL(28,10)"
  "DECIMAL(38,10)"
  "DECIMAL(38,16)"
)

$IMPALA_SHELL -B --quiet -q "create database if not exists $DB"

echo > $TMP_SQL
for i in `seq $NUM_TABLES`; do
  echo "create external table if not exists tbl$i (" >> $TMP_SQL
  for j in {0..499}; do
    data_type=${DATA_TYPES[$((j/25))]}
    col="col$j $data_type comment 'comment'"
    if [ $j -ne 499 ]; then
      col="$col,"
    fi
    echo "  $col" >> $TMP_SQL
  done
  echo ") partitioned by (p int) stored as parquet" >> $TMP_SQL
  # Set DO_NOT_UPDATE_STATS to true to speed up Hive inserts.
  echo "tblproperties('DO_NOT_UPDATE_STATS'='true');" >> $TMP_SQL
done 

$IMPALA_SHELL -d $DB -f $TMP_SQL -B --quiet
# Create the source table for INSERTs
$IMPALA_SHELL -B --quiet -d $DB -q "create external table if not exists default.src_tbl like tbl1"
