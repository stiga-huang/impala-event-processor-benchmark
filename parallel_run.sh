#!/bin/bash

# Hive CREATE db/table tests
# Keeps generating CREATE/DROP events
for i in `seq 10`; do
  for j in `seq 8`; do ./run.sh tests/hive_create_db.sh; sleep 1; done > create_db_$i.log 2>&1 &
  for j in `seq 8`; do ./run.sh tests/hive_create_table.sh; sleep 1; done > create_tbl_$i.log 2>&1 &
  for j in `seq 8`; do ./run.sh tests/hive_drop_db.sh; sleep 1; done > drop_db_$i.log 2>&1 &
  for j in `seq 8`; do ./run.sh tests/hive_drop_table.sh; sleep 1; done > drop_tbl_$i.log 2>&1 &
done

# Hive INSERT tests
rm -f tbl*.log

for i in {1..5}; do
  ./run.sh tests/hive_insert_static_new_part.sh tbl$i >> tbl${i}.log 2>&1 \
  && ./run.sh tests/hive_insert_static_existing_part.sh tbl$i >> tbl${i}.log 2>&1 \
  && ./run.sh tests/hive_overwrite_static_existing_part.sh tbl$i >> tbl${i}.log 2>&1 \
  && ./run.sh tests/hive_insert_dynamic_new_part.sh tbl$i >> tbl${i}.log 2>&1 \
  && ./run.sh tests/hive_insert_dynamic_existing_part.sh tbl$i >> tbl${i}.log 2>&1 &
done

wait

grep -h '>>>>>>>>>>' create*.log drop*.log tbl*.log | sort

# Somehow the terminal is messed up. Recover it anyway.
stty sane
