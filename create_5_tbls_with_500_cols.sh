#!/bin/bash

DB=scale_400k_500cols_db
NUM_TABLES=5
TMP_SQL=tmp.sql
#IMPALA_SHELL=bin/impala-shell.sh
IMPALA_SHELL=impala-shell

# 20 data types
DATA_TYPES=(
  "BIGINT"
  "INT"
  "SMALLINT"
  "TINYINT"
  "BOOLEAN"
  "FLOAT"
  "DOUBLE"
  "STRING"
  "TIMESTAMP"
  "DATE"
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

for i in `seq -w $NUM_TABLES`; do
  echo "create table if not exists tbl$i (" >> $TMP_SQL
  for j in {0..499}; do
    data_type=${DATA_TYPES[$((j/25))]}
    col="col$j $data_type comment 'comment'"
    if [ $j -ne 499 ]; then
      col="$col,"
    fi
    echo "  $col" >> $TMP_SQL
  done
  part_col_type=int
  if [ $((i%2)) -eq 0 ]; then
    part_col_type=string
  fi
  echo ") partitioned by (p $part_col_type) stored as parquet;" >> $TMP_SQL
  #$IMPALA_SHELL -d $DB -f $TMP_SQL -B --quiet
  #echo $i done
done 
