#!/bin/bash

procuder() {
  TBL=${1:-tbl1}
  echo "$(get_ts) Hive> Clearing partitions p>=400000"
  $HIVE_EXEC "alter table $DB.$TBL drop if exists partition(p>=400000)"
  echo "$(get_ts) Impala> Reloading table"
  $IMPALA_EXEC "refresh $DB.$TBL"
  echo "$(get_ts) Hive> Dynamically creating 200 partitions"
  $HIVE_EXEC "set hive.stats.autogather=false; set hive.exec.max.dynamic.partitions.pernode=200; insert into $DB.$TBL select $COLS, p+400000 from default.src_tbl where p<200"
}

consumer_verified() {
  TBL=${1:-tbl1}
  #TODO: This statement takes 5s where most of the time spent in query planning (4.59s)
  # when the table has 400K partitions. Try to find a more lightweight verifier.
  # Consider SHOW PARTITIONS with WHERE clause (IMPALA-14065) or checking the partition
  # in catalogd WebUI (IMPALA-9935).
  row_count=$($IMPALA_EXEC "select count(*) from $DB.$TBL where p=400199")
  if [ $row_count -eq 1 ]; then
    echo "$(get_ts) Row count: $row_count"
    return 0
  fi
  echo "$(get_ts) Row count: $row_count"
  return 1
}

manual_refresh() {
  TBL=${1:-tbl1}
  $IMPALA_EXEC "refresh $DB.$TBL"
}
