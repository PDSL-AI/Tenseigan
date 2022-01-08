#!/bin/sh

TIUP_DUMPLING='tiup dumpling'

if [ $# != 6 ]; then
echo "USAGE: $0 <user> <host> <port> <split_file_size> <threads> <dump_dir>"
echo "e.g.: $0 root tidb-pd-1 4000 256MiB 16 /data1/tidb-1"
exit 1;
fi


nohup $TIUP_DUMPLING -u $1 --host $2 -P $3 -F $4 -t $5 -o $6 > ./log/tiup_dumpling.log 2>&1 &
