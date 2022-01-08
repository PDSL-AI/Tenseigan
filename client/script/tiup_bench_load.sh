#!/bin/sh

TIUP_BENCH='tiup bench tpcc'

if [ $# != 5 ]; then
echo "USAGE: $0 <host> <port> <db_name> <warehouses> <threads>"
echo "e.g.: $0 tidb-pd-1 4000 tpcc 100 50"
exit 1;
fi


nohup $TIUP_BENCH -H $1 -P $2 -D $3 --warehouses $4 --threads $5 prepare > ./log/tiup_bench_load.log 2>&1 &
