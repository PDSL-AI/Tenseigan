#! /usr/bin/env bash
nohup fab run_loops:$1,True > ./log/loop.log 2>&1 < /dev/null &
