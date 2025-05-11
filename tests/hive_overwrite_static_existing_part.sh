#!/bin/bash

DB=scale_400k_500cols_db
COLUMNS="col0"
for i in {1..499}; do
  COLUMNS="$COLUMNS,col$i"
done

procuder() {
  echo "$(get_ts) Impala> Adding a new row to the existing partition so it has more than one row"
  $IMPALA_EXEC "insert into $DB.tbl1 partition(p=0) select $COLUMNS from $DB.tbl3 where p=0"
  echo "$(get_ts) Hive> Resetting the partition to have only one row"
  $HIVE_EXEC "set hive.stats.autogather=false; insert overwrite table $DB.tbl1 partition (p=0) select $COLUMNS from $DB.tbl3 where p=0"
}

consumer_verified() {
  #TODO: This statement takes 5s where most of the time spent in query planning (4.59s)
  # when the table has 400K partitions. Try to find a more lightweight verifier.
  # Consider SHOW PARTITIONS with WHERE clause (IMPALA-14065).
  row_count=$($IMPALA_EXEC "select count(*) from $DB.tbl1 where p=0")
  if [ $row_count -eq 1 ]; then
    echo "$(get_ts) Row count: $row_count"
    return 0
  fi
  echo "$(get_ts) Row count: $row_count"
  return 1
}

