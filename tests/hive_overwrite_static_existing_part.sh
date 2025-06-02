#!/bin/bash

procuder() {
  TBL=${1:-tbl1}
  echo "$(get_ts) Impala> Adding a new row to the existing partition (p=0) of $DB.$TBL so it has more than one row"
  $IMPALA_EXEC "insert into $DB.$TBL partition(p=1000) select $COLS from default.src_tbl where p=0"
  echo "$(get_ts) Hive> Resetting the partition to have only one row"
  $HIVE_EXEC "set hive.stats.autogather=false; insert overwrite table $DB.$TBL partition (p=1000) select $COLS from default.src_tbl where p=0"
}

consumer_verified() {
  TBL=${1:-tbl1}
  #TODO: This statement takes 5s where most of the time spent in query planning (4.59s)
  # when the table has 400K partitions. Try to find a more lightweight verifier.
  # Consider SHOW PARTITIONS with WHERE clause (IMPALA-14065).
  row_count=$($IMPALA_EXEC "select count(*) from $DB.$TBL where p=1000")
  if [[ "$row_count" == "1" ]]; then
    echo "$(get_ts) Row count: $row_count"
    return 0
  fi
  echo "$(get_ts) Row count: $row_count"
  return 1
}

manual_refresh() {
  TBL=${1:-tbl1}
  $IMPALA_EXEC "refresh $DB.$TBL partition (p=1000)"
}
