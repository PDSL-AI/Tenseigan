#!/bin/sh

TIDB_LIGHTNING='/data1/workspace/dc/tidb-toolkit-v5.0.0-linux-amd64/bin'

if [ $# != 1 ]; then
echo "USAGE: $0 <tidb-lightning.toml>"
echo "e.g.: $0 ./tidb-lightning.toml"
exit 1;
fi

$TIDB_LIGHTNING/tidb-lightning -config $1 > ./log/run-tidb-lightning.log
