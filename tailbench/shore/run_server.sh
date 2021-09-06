#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source ${DIR}/../configs.sh

THREADS=1
QPS=10
WARMUPREQS=250
MAXREQS=500

DUMMYREQS=1000000 # set this really high so MAXREQS controls execution

# Point this to an appropriate location on your system
# note: instead of setting it here, we better get it from ../configs.sh
# so I anotated it out
# SCRATCH_DIR=/doesnotexist

# Setup
TMP=$(mktemp -d --tmpdir=${SCRATCH_DIR})
ln -s $TMP scratch
mkdir scratch/log && ln -s scratch/log log
mkdir scratch/diskrw && ln -s scratch/diskrw diskrw

cp ${DATA_ROOT}/shore/db-tpcc-1 scratch/ && \
    ln -s scratch/db-tpcc-1 db-tpcc-1
chmod 644 scratch/db-tpcc-1

cp shore-kits/run-templates/cmdfile.template cmdfile
sed -i -e "s#@NTHREADS#$THREADS#g" cmdfile
sed -i -e "s#@REQS#$DUMMYREQS#g" cmdfile

cp shore-kits/run-templates/shore.conf.template \
    shore.conf
sed -i -e "s#@NTHREADS#$THREADS#g" shore.conf

# Launch Server
TBENCH_SERVER="10.30.6.127" TBENCH_SERVER_PORT=3030 TBENCH_MAXREQS=${MAXREQS} TBENCH_WARMUPREQS=${WARMUPREQS} \
    shore-kits/shore_kits_server_networked -i cmdfile

rm -f log scratch cmdfile db-tpcc-1 diskrw shore.conf info server.pid \
    client.pid
