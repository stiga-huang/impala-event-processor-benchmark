#!/bin/bash

procuder() {
  TBL=${1:-tbl1}
  echo "$(get_ts) Impala> Dropping partition p=1000 in $DB.$TBL"
  $IMPALA_EXEC "alter table $DB.$TBL drop if exists partition(p=1000)"
  echo "$(get_ts) Hive> Adding partition p=1000 in $DB.$TBL"
  $HIVE_EXEC "alter table $DB.$TBL add if not exists partition(p=1000)"
}

consumer_verified() {
  TBL=${1:-tbl1}
  row_count=$($IMPALA_EXEC "select count(*) from $DB.$TBL where p=1000")
  if [[ "$row_count" == "0" ]]; then
    echo "$(get_ts) Partition doesn't exist"
    return 1
  fi
  echo "$(get_ts) Row count: $row_count"
  return 0
}

manual_refresh() {
  TBL=${1:-tbl1}
  $IMPALA_EXEC "refresh $DB.$TBL partition(p=1000)"
}

