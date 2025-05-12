#!/bin/bash

DB=scale_4k_500cols_db

COLS="col0"
for i in {1..499}; do
  COLS="$COLS,col$i"
done

procuder() {
  echo "$(get_ts) Impala> Clearing partitions p>=400000"
  $IMPALA_EXEC "alter table $DB.tbl1 drop if exists partition(p>=400000)"
  echo "$(get_ts) Hive> Dynamically created 200 partitions"
  $HIVE_EXEC "set hive.stats.autogather=false; set hive.exec.max.dynamic.partitions.pernode=200; insert into $DB.tbl1 select $COLS, p+400000 from $DB.tbl3 where p<200"
}

consumer_verified() {
  #TODO: This statement takes 5s where most of the time spent in query planning (4.59s)
  # when the table has 400K partitions. Try to find a more lightweight verifier.
  # Consider SHOW PARTITIONS with WHERE clause (IMPALA-14065) or checking the partition
  # in catalogd WebUI (IMPALA-9935).
  row_count=$($IMPALA_EXEC "select count(*) from $DB.tbl1 where p=400199")
  if [ $row_count -eq 1 ]; then
    echo "$(get_ts) Row count: $row_count"
    return 0
  fi
  echo "$(get_ts) Row count: $row_count"
  return 1
}

