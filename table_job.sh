#!/bin/bash

set -e

i=$1

./run.sh tests/hive_insert_static_new_part.sh tbl$i
./run.sh tests/hive_insert_static_existing_part.sh tbl$i
./run.sh tests/hive_overwrite_static_existing_part.sh tbl$i
./run.sh tests/hive_insert_dynamic_new_part.sh tbl$i
./run.sh tests/hive_insert_dynamic_existing_part.sh tbl$i
./run.sh tests/hive_overwrite_dynamic_existing_part.sh tbl$i
./run.sh tests/hive_insert_dynamic_mixed_part.sh tbl$i
./run.sh tests/hive_drop_part.sh tbl$i
./run.sh tests/hive_add_part.sh tbl$i
./run.sh tests/hive_alter_table.sh tbl$i
# Put rename test at the end since it invalidates the table
./run.sh tests/hive_rename_table.sh tbl$i
