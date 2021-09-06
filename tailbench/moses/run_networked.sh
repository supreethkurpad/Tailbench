#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source ${DIR}/../configs.sh

THREADS=1
QPS=100
WARMUPREQS=5000
MAXREQS=5000

BINDIR=./bin

# Setup
cp moses.ini.template moses.ini
sed -i -e "s#@DATA_ROOT#$DATA_ROOT#g" moses.ini

# Launch Server
TBENCH_MAXREQS=${MAXREQS} TBENCH_WARMUPREQS=${WARMUPREQS} \
    ${BINDIR}/moses_server_networked -config ./moses.ini \
    -input-file ${DATA_ROOT}/moses/testTerms \
    -threads ${THREADS} -num-tasks 1000000 -verbose 0 &

echo $! > server.pid

sleep 5

# Launch Client
TBENCH_QPS=${QPS} TBENCH_MINSLEEPNS=10000 \
    ${BINDIR}/moses_client_networked &
echo $! > client.pid

wait $(cat client.pid)

# Cleanup
./kill_networked.sh
