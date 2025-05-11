#!/bin/bash

procuder() {
  $IMPALA_EXEC "create database if not exists db1; create database if not exists db2; create database if not exists db3"
  $HIVE_EXEC "drop database db1; drop database db2; drop database db3"
}

consumer_verified() {
  dbs=$($IMPALA_EXEC "show databases")
  for i in {1..3}; do
    if grep -q "^db$i"$'\t' <<< "$dbs"; then
      echo "[$(date '+%Y-%m-%d %H:%M:%S')] db$i still exists"
      return 1
    fi
    echo "Removed db$i"
  done
  return 0
}

