#!/bin/sh

TIDB_LIGHTNING='/data1/workspace/dc/tidb-toolkit-v5.0.0-linux-amd64/bin'

if [ $# != 0 ]; then
echo "USAGE: $0"
echo "e.g.: $0"
exit 1;
fi

$TIDB_LIGHTNING/tidb-lightning-ctl --switch-mode=normal > ./log/tidb-lightning-switch-mode.log
