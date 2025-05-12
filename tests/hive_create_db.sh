#!/bin/bash

export CATALOG_URL="http://localhost:25020/catalog_object?json&object_type=DATABASE&object_name="
export CURL="curl -s"
#export CATALOG_URL="https://vb1404.halxg.cloudera.com:25020/catalog_object?json&object_type=DATABASE&object_name="
#export CURL="curl -s --cacert /var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem --negotiate -u : "

export DB_PREFIX="hive_created_db_$(uuidgen | cut -c 1-8)"
export NUM_DBS=1

procuder() {
  SQL=""
  for i in `seq $NUM_DBS`; do
    SQL="$SQL drop database if exists ${DB_PREFIX}${i};"
  done
  $IMPALA_EXEC "$SQL"
  SQL=""
  for i in `seq $NUM_DBS`; do
    SQL="$SQL create database ${DB_PREFIX}${i};"
  done
  $HIVE_EXEC "$SQL"
}

consumer_verified_old() {
  dbs=$($IMPALA_EXEC "show databases")
  for i in `seq $NUM_DBS`; do
    if ! grep -q "^${DB_PREFIX}${i}"$'\t' <<< "$dbs"; then
      echo "$(get_ts) ${DB_PREFIX}${i} not found"
      return 1
    fi
    echo "Found db$i"
  done
  return 0
}

consumer_verified() {
  for i in `seq $NUM_DBS`; do
    set -x
    if ! $CURL "${CATALOG_URL}${DB_PREFIX}${i}" | jq -e ".json_string"; then
      set +x
      echo "$(get_ts) ${DB_PREFIX}${i} not found"
      sleep 0.01
      return 1
    fi
    set +x
    echo "$(get_ts) Found ${DB_PREFIX}${i}"
  done
}

cleanup() {
  SQL=""
  for i in `seq $NUM_DBS`; do
    SQL="$SQL drop database if exists ${DB_PREFIX}${i};"
  done
  $HIVE_EXEC "$SQL"
}
