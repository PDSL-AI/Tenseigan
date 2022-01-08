#!/bin/sh

TIUP_AUTO_TUNE='/data1/workspace/dc/tiup/bin/'

if [ $# != 1 ]; then
echo "USAGE: $0 <cluster-name>"
echo "e.g.: $0 tidb-1"
exit 1;
fi

$TIUP_AUTO_TUNE/tiup-cluster clean $1 --all --ignore-role prometheus --yes > ./log/clean_tidb_data.log
