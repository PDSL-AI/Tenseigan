#!/bin/sh

TIUP_AUTO_TUNE='/data1/workspace/dc/tiup/bin/'
TOPOLOGY_FILE_DIR='/data1/workspace/ottertune/cluster'

if [ $# != 1 ]; then
echo "USAGE: $0 <cluster-name>"
echo "e.g.: $0 tidb-1"
exit 1;
fi

$TIUP_AUTO_TUNE/tiup-cluster deploy $1 v5.0.3 $TOPOLOGY_FILE_DIR/$1.yaml --user root -i /root/.ssh/kp-x5oszr3r --yes > ./log/deploy_cluster.log
