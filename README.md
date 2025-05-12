# Benchmark for Impala HMS EventProcessor
## Overview
Measure the delay of external changes arriving in Impala. Each scenario has a producer and a consumer.
* The producer is usually an external app, e.g. Hive query, SparkSQL query, external Impala query, etc. It modifies data shared with Impala. Tables should be in the loaded state so the external modification makes the metadata stale.
* The consumer verifies the results and measures when the external changes arrive in Impala.
* The delay (lag) is the output captured for each scenario. To be specific, it’s the time between when the producer has finished and the consumer sees the expected results.

Baseline is using the solution without EventProcessor, i.e.
* Starting catalogd with --hms_event_polling_interval_s=0,
* Using REFRESH/INVALIDATE in the consumers.

Notes:
* Transactional tables are not tested for simplicity.
* Tables should be in the loaded state in Impala so the external modification makes the metadata stale.
* Consumers could be slow which adds into the delay. Try using lightweight consumers like checking /catalog WebUI of catalogd.

## Scale Factors
* Number of dbs/tables/partitions/columns/files
* File size doesn’t matter unless the storage splits the file, e.g. when using HDFS and the file size is larger than the HDFS block size. We only test files smaller than the block size since this is more common in reality.

## Data Generation
Edit variables in `create_5_tbls_with_500_cols.sh`. Use it to create partitioned tables with 500 columns.
`create_hive_part_dirs.sh` creates the partition dirs on HDFS for the first table. You will need
to manually mirror data to the other tables based on the output.
```bash
bash create_5_tbls_with_500_cols.sh
bash create_hive_part_dirs.sh
```

## Benchmark Scenarios
These are common scenarios in daily/weekly jobs.
* Hive create/drop dbs
* Hive create/drop tables
* Hive insert a static partition
  * Insert into a new partition
  * Insert into an existing partition
  * Insert overwrite an existing partition
* Hive insert dynamic partitions
  * Insert into new partitions
  * Insert into existing partitions
  * Insert overwrite existing partitions
  * Insert into both existing and new partitions
* Hive add/drop partitions
* Hive alter table changing tblproperties
* Hive creates temp table + rename it to prod table after dropping the prod table
  * CreateTableAsSelect
  * CreateTable + Insert

## Usage

Edit tests/run.sh to update the beeline and impala-shell commands. Edit variables in each test.
Run a test case by
```bash
./run.sh test_script
```
For instance
```bash
./run.sh tests/hive_create_db.sh
```
Run tests in parallel.
```bash
./parallel_run.sh
```

## Development
Each script in `tests` dir implements a `producer()` method and a `consumer_verified()` method.
* `producer()` cleanup/reset the data in Impala and produce changes outside Impala.
* `consumer_verified()` verifies the results in Impala and returns whether they are expected.

Example: `tests/hive_create_db.sh`
```bash
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
```