#!/bin/bash

export CATALOG_URL="http://localhost:25020/catalog_object?json&object_type=TABLE&object_name=default."
export CURL="curl -s"
#export CATALOG_URL="https://vb1404.halxg.cloudera.com:25020/catalog_object?json&object_type=TABLE&object_name=default."
#export CURL="curl -s --cacert /var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem --negotiate -u : "

export TBL_NAME_PREFIX="hive_created_tbl_$(uuidgen | cut -c 1-8)"
export NUM_TABLES=1

procuder() {
  SQL=""
  for i in `seq $NUM_TABLES`; do
    SQL="$SQL; drop table if exists ${TBL_NAME_PREFIX}_${i}"
  done
  $IMPALA_EXEC "$SQL"
  SQL=""
  for i in `seq $NUM_TABLES`; do
    SQL="$SQL; create table ${TBL_NAME_PREFIX}_${i} (i int)"
  done
  $HIVE_EXEC "$SQL"
}

consumer_verified_old() {
  tables=$($IMPALA_EXEC "show tables")
  for i in `seq $NUM_TABLES`; do
    if ! grep -q "^${TBL_NAME_PREFIX}_$i$" <<< "$tables"; then
      echo "$(get_ts) ${TBL_NAME_PREFIX}_$i not found"
      return 1
    fi
    echo "$(get_ts) Found ${TBL_NAME_PREFIX}_$i"
  done
  return 0
}

consumer_verified() {
  for i in `seq $NUM_TABLES`; do
    TBL_NAME="${TBL_NAME_PREFIX}_${i}"
    if ! $CURL "'${CATALOG_URL}${TBL_NAME}'" | jq -e ".json_string" > /dev/null; then
      echo "$(get_ts) ${TBL_NAME} not found"
      return 1
    fi
    echo "$(get_ts) Found ${TBL_NAME}"
  done
}

cleanup() {
  SQL=""
  for i in `seq $NUM_TABLES`; do
    SQL="$SQL; drop table if exists ${TBL_NAME_PREFIX}_${i}"
  done
  $HIVE_EXEC "$SQL"
}
