#!/bin/sh

TIUP_AUTO_TUNE='/data1/workspace/dc/tiup/bin/'
SHELL_FOLDER=$(dirname $(readlink -f "$0"))

if [ $# != 1 ]; then
echo "USAGE: $0 <cluster-name>"
echo "e.g.: $0 tidb-1"
exit 1;
fi

$TIUP_AUTO_TUNE/tiup-cluster change-config $1 $SHELL_FOLDER/$1/default.yaml > ./log/clean_conf.log
