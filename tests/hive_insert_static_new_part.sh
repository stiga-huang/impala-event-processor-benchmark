#!/bin/bash

DB=scale_4k_500cols_db

COLS="col0"
for i in {1..499}; do
  COLS="$COLS,col$i"
done

procuder() {
  TBL=${1:-tbl1}
  echo "$(get_ts) Impala> Dropping partition p=500000 in $DB.$TBL"
  $IMPALA_EXEC "alter table $DB.$TBL drop if exists partition(p=500000)"
  echo "$(get_ts) Hive> Adding the new partition by INSERT"
  SQL="set hive.stats.autogather=false; insert into $DB.$TBL partition(p=500000) select $COLS from $DB.tbl3 where p=999"
  $HIVE_EXEC "$SQL"
}

consumer_verified() {
  TBL=${1:-tbl1}
  #TODO: This statement takes 5s where most of the time spent in query planning (4.59s)
  # when the table has 400K partitions. Try to find a more lightweight verifier.
  # Consider SHOW PARTITIONS with WHERE clause (IMPALA-14065) or checking the partition
  # in catalogd WebUI (IMPALA-9935).
  row_count=$($IMPALA_EXEC "select count(*) from $DB.$TBL where p=500000")
  if [ $row_count -eq 0 ]; then
    echo "$(get_ts) New partition not found"
    return 1
  fi
  echo "$(get_ts) Row count: $row_count"
  return 0
}

