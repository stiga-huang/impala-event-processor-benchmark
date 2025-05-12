#!/bin/bash

DB=scale_4k_500cols_db

COLS="col0"
for i in {1..499}; do
  COLS="$COLS,col$i"
done

procuder() {
  echo "$(get_ts) Impala> Resetting partition p=0 to have only one row"
  $IMPALA_EXEC "insert overwrite table $DB.tbl1 partition (p=0) select $COLS from $DB.tbl3 where p=0"
  echo "$(get_ts) Hive> Adding a new row to the existing partition so it has 2 rows"
  $HIVE_EXEC "set hive.stats.autogather=false; insert into $DB.tbl1 partition(p=0) select $COLS from $DB.tbl3 where p=0"
}

consumer_verified() {
  #TODO: This statement takes 5s where most of the time spent in query planning (4.59s)
  # when the table has 400K partitions. Try to find a more lightweight verifier.
  # Consider SHOW PARTITIONS with WHERE clause (IMPALA-14065) or checking the partition
  # in catalogd WebUI (IMPALA-9935).
  row_count=$($IMPALA_EXEC "select count(*) from $DB.tbl1 where p=0")
  if [ $row_count -eq 2 ]; then
    echo "$(get_ts) Row count: $row_count"
    return 0
  fi
  echo "$(get_ts) Row count: $row_count"
  return 1
}

