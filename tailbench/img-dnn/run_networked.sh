#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source ${DIR}/../configs.sh

THREADS=1
REQS=100000000 # Set this very high; the harness controls maxreqs

TBENCH_WARMUPREQS=5000 TBENCH_MAXREQS=12500 ./img-dnn_server_networked \
    -r ${THREADS} -f ${DATA_ROOT}/img-dnn/models/model.xml -n ${REQS} &
echo $! > server.pid

sleep 5 # Wait for server to come up

TBENCH_QPS=500 TBENCH_MNIST_DIR=${DATA_ROOT}/img-dnn/mnist ./img-dnn_client_networked &

echo $! > client.pid

wait $(cat client.pid)
# Clean up
./kill_networked.sh
