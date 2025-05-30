#!/bin/bash

procuder() {
  TBL=${1:-tbl1}
  echo "$(get_ts) Hive> Clearing partitions p>=400000"
  $HIVE_EXEC "alter table $DB.$TBL drop if exists partition(p>=400000)"
  echo "$(get_ts) Impala> Resetting partition p=200..399 to have only one row"
  SQL="insert overwrite table $DB.$TBL partition(p) select $COLS,cast(p+200 as int) from default.src_tbl where p<200"
  if [[ "$REFRESH_SETUP" == "true" ]]; then
    echo "$(get_ts) Impala> REFRESH + INSERT OVERWRITE $DB.$TBL"
    $IMPALA_EXEC "refresh $DB.$TBL; $SQL"
  else
    # Using INVALIDATE instead of REFRESH to avoid long running DDLs blocking event processing.
    # Use REFRESH on multiple partitions if the Impala version supports.
    echo "$(get_ts) Impala> INVALIDATE + INSERT OVERWRITE $DB.$TBL"
    $IMPALA_EXEC "invalidate metadata $DB.$TBL; $SQL"
  fi
  echo "$(get_ts) Hive> Dynamically inserting 200 new partitions and 200 existing partitions"
  $HIVE_EXEC "set hive.stats.autogather=false; set hive.exec.max.dynamic.partitions.pernode=400; insert into $DB.$TBL select $COLS, p+200 from default.src_tbl where p<200 union all select $COLS, p+400000 from default.src_tbl where p<200"
}

consumer_verified() {
  TBL=${1:-tbl1}
  #TODO: This statement takes 5s where most of the time spent in query planning (4.59s)
  # when the table has 400K partitions. Try to find a more lightweight verifier.
  # Consider SHOW PARTITIONS with WHERE clause (IMPALA-14065) or checking the partition
  # in catalogd WebUI (IMPALA-9935).
  row_count=$($IMPALA_EXEC "select count(*) from $DB.$TBL where p in (399, 400199)")
  if [[ "$row_count" != "3" ]]; then
    echo "$(get_ts) Row count: $row_count"
    return 1
  fi
  echo "$(get_ts) Row count: $row_count"
  return 0
}

manual_refresh() {
  TBL=${1:-tbl1}
  $IMPALA_EXEC "refresh $DB.$TBL"
}
