#!/bin/sh

TIUP_BENCH='tiup bench tpcc'

nohup $TIUP_BENCH -H $1 -P $2 -D $3 --warehouses $4 --threads $5 --time $6 run > ./log/tiup_bench_run.log 2>&1 &
