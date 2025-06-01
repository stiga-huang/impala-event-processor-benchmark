#!/bin/bash

# Hive CREATE db/table tests
# Keeps generating CREATE/DROP events
for i in `seq 5`; do
  for j in `seq 4`; do ./run.sh tests/hive_create_db.sh; sleep 10; done > create_db_$i.log 2>&1 &
  for j in `seq 4`; do ./run.sh tests/hive_create_table.sh; sleep 10; done > create_tbl_$i.log 2>&1 &
  for j in `seq 4`; do ./run.sh tests/hive_drop_table.sh; sleep 10; done > drop_tbl_$i.log 2>&1 &
done

# Hive INSERT/ALTER/RENAME table tests. Run them distributedly across all servers.
#  -j4 specifies 4 jobs to run in parallel on a node
#  {1..20} specifies the table indexes are 1 to 20
parallel --sshloginfile servers.txt -j4 --keep-order 'bash table_job.sh {}' ::: {1..20} > all_tbl.log 2>all_tbl.err

grep -h '>>>>>>>>>>' create*.log drop*.log all_tbl.log | sort

# Somehow the terminal is messed up. Recover it anyway.
stty sane
