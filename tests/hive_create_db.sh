#!/bin/bash

procuder() {
  $IMPALA_EXEC "drop database if exists db1; drop database if exists db2; drop database if exists db3"
  $HIVE_EXEC "create database db1; create database db2; create database db3"
}

consumer_verified() {
  dbs=$($IMPALA_EXEC "show databases")
  for i in {1..3}; do
    if ! grep -q "^db$i"$'\t' <<< "$dbs"; then
      echo "[$(date '+%Y-%m-%d %H:%M:%S')] db$i not found"
      return 1
    fi
    echo "Found db$i"
  done
  return 0
}

