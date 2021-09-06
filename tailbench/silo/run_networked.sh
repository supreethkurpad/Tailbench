#!/bin/bash
# ops-per-worker is set to a very large value, so that TBENCH_MAXREQS controls how
# many ops are performed
NUM_WAREHOUSES=1
NUM_THREADS=1

QPS=1000
MAXREQS=50000
WARMUPREQS=20000

TBENCH_MAXREQS=${MAXREQS} TBENCH_WARMUPREQS=${WARMUPREQS} \
    ./out-perf.masstree/benchmarks/dbtest_server_networked --verbose --bench \
    tpcc --num-threads ${NUM_THREADS} --scale-factor ${NUM_WAREHOUSES} \
    --retry-aborted-transactions --ops-per-worker 10000000 &

echo $! > server.pid

sleep 5 # Allow server to come up

TBENCH_QPS=${QPS} TBENCH_MINSLEEPNS=10000 \
    ./out-perf.masstree/benchmarks/dbtest_client_networked &

echo $! > client.pid

wait $(cat client.pid)

# Clean up
./kill_networked.sh
