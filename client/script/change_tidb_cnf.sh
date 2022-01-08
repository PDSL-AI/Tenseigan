#!/bin/sh

TIUP_AUTO_TUNE='/data1/workspace/dc/tiup/bin/'

if [ $# != 2 ]; then
echo "USAGE: $0 <cluster-name> <config.yaml>"
echo "e.g.: $0 tidb-1 ./config.yaml"
exit 1;
fi

$TIUP_AUTO_TUNE/tiup-cluster change-config $1 $2 > ./log/change_conf.log
