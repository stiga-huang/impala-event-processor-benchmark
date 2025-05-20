#!/bin/bash

procuder() {
  TBL=${1:-tbl1}
  echo "$(get_ts) Impala> Adding new rows to partition p=200..399 so each has more than one row"
  $IMPALA_EXEC "insert into $DB.$TBL partition(p) select $COLS,cast(p+200 as int) from $DB.tbl3 where p<200"
  echo "$(get_ts) Hive> Dynamically overwriting 200 partitions"
  $HIVE_EXEC "set hive.stats.autogather=false; set hive.exec.max.dynamic.partitions.pernode=200; insert overwrite $DB.$TBL select $COLS, p+200 from $DB.tbl3 where p<200"
}

consumer_verified() {
  TBL=${1:-tbl1}
  #TODO: This statement takes 5s where most of the time spent in query planning (4.59s)
  # when the table has 400K partitions. Try to find a more lightweight verifier.
  # Consider SHOW PARTITIONS with WHERE clause (IMPALA-14065) or checking the partition
  # in catalogd WebUI (IMPALA-9935).
  row_count=$($IMPALA_EXEC "select count(*) from $DB.$TBL where p=399")
  if [[ "$row_count" == "1" ]]; then
    echo "$(get_ts) Row count: $row_count"
    return 0
  fi
  echo "$(get_ts) Row count: $row_count"
  return 1
}

