#!/bin/bash

# Hive CREATE db/table tests
for i in `seq 10`; do
  ./run.sh tests/hive_create_db.sh > create_db_$i.log 2>&1 &
  ./run.sh tests/hive_create_table.sh > create_tbl_$i.log 2>&1 &
done

# Hive INSERT tests
rm tbl*.log

for i in {1..5}; do
  ./run.sh tests/hive_insert_static_new_part.sh tbl$i >> tbl${i}.log 2>&1 && ./run.sh tests/hive_insert_static_existing_part.sh tbl$i >> tbl${i}.log 2>&1 && ./run.sh tests/hive_insert_dynamic_new_part.sh tbl$i >> tbl${i}.log 2>&1 &
done

wait

grep -h '>>>>>>>>>>' create_*.log tbl*.log | sort

stty sane
