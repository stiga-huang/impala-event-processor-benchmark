#!/bin/bash

procuder() {
  TBL=${1:-tbl1}
  echo "$(get_ts) Impala> Resetting tblproperty of $DB.$TBL"
  $IMPALA_EXEC "alter table $DB.$TBL set tblproperties('modifier'='impala')"
  echo "$(get_ts) Hive> Setting tblproperty of $DB.$TBL"
  $HIVE_EXEC "alter table $DB.$TBL set tblproperties('modifier'='hive')"
}

consumer_verified() {
  TBL=${1:-tbl1}
  if $IMPALA_EXEC "describe formatted $DB.$TBL" | grep modifier | grep hive; then
    echo "$(get_ts) tblproperties synced"
    return 0
  fi
  echo "$(get_ts) tblproperties not synced"
  return 1
}

manual_refresh() {
  TBL=${1:-tbl1}
  $IMPALA_EXEC "refresh $DB.$TBL"
}

