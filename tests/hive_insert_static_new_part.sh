#!/bin/bash

DB=scale_400k_500cols_db

procuder() {
  $IMPALA_EXEC "alter table $DB.tbl1 drop if exists partition(p=500000)"
  SQL="set hive.stats.autogather=false; insert into $DB.tbl1 partition(p=500000) select col0"
  for i in {1..499}; do
    SQL="$SQL,col$i"
  done
  SQL="$SQL from $DB.tbl3 where p=0"
  $HIVE_EXEC "$SQL"
}

consumer_verified() {
  #TODO: This statement takes 5s where most of the time spent in query planning (4.59s)
  # when the table has 400K partitions. Try to find a more lightweight verifier.
  # Consider SHOW PARTITIONS with WHERE clause (IMPALA-14065).
  row_count=$($IMPALA_EXEC "select count(*) from $DB.tbl1 where p=500000")
  if [ $row_count -eq 0 ]; then
    echo "$(get_ts) New partition not found"
    return 1
  fi
  echo "$(get_ts) Row count: $row_count"
  return 0
}

