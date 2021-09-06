#!/bin/bash

if [[ -z "${NTHREADS}" ]]; then NTHREADS=1; fi

QPS=2000
MAXREQS=50000
WARMUPREQS=14000

TBENCH_MAXREQS=${MAXREQS} TBENCH_WARMUPREQS=${WARMUPREQS} \
    ./mttest_server_networked -j${NTHREADS} mycsba masstree &
echo $! > server.pid

sleep 5 # Allow server to come up

TBENCH_QPS=${QPS} TBENCH_MINSLEEPNS=10000 ./mttest_client_networked &
echo $! > client.pid

wait $(cat client.pid)

# Clean up
./kill_networked.sh
    
