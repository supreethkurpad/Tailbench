#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source ${DIR}/../configs.sh

# Setup commands
mkdir -p results

# Run specjbb
TBENCH_PATH=../harness

export LD_LIBRARY_PATH=${TBENCH_PATH}:${LD_LIBRARY_PATH}

export CLASSPATH=./build/dist/jbb.jar:./build/dist/check.jar:${TBENCH_PATH}/tbench.jar

export PATH=${JDK_PATH}/bin:${PATH}
export TBENCH_QPS=5000 
export TBENCH_MAXREQS=25000 
export TBENCH_WARMUPREQS=25000 
export TBENCH_MINSLEEPNS=10000

if [[ -d libtbench_jni.so ]] 
then
    rm libtbench_jni.so
fi
ln -sf ./libtbench_integrated_jni.so libtbench_jni.so

chrt -r 99 ${JDK_PATH}/bin/java -Djava.library.path=. -XX:ParallelGCThreads=1 \
    -XX:+UseSerialGC -XX:NewRatio=1 -XX:NewSize=7000m -Xloggc:gc.log \
    -Xms10000m -Xmx10000m -Xrs spec.jbb.JBBmain -propfile SPECjbb_mt.props

# Teardown
rm libtbench_jni.so
rm gc.log
rm -r results

