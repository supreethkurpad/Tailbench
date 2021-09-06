#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source ${DIR}/../configs.sh

TBENCH_PATH=../harness

# Compile client module
g++ -std=c++0x -g -O3 -fPIC -I${TBENCH_PATH} -c client.cpp -o client.o
g++ -std=c++0x -g -O3 client.o ${TBENCH_PATH}/client.o \
    ${TBENCH_PATH}/tbench_client_networked.o -o client -lrt -pthread

# Create jni shared lib
g++ -std=c++11 -g -O3 -shared -fPIC -o libtbench_integrated_jni.so \
    ${TBENCH_PATH}/tbench_jni.o ${TBENCH_PATH}/client.o \
    ${TBENCH_PATH}/tbench_server_integrated.o client.o -lrt -pthread

g++ -std=c++11 -g -O3 -shared -fPIC -o libtbench_networked_jni.so \
    ${TBENCH_PATH}/tbench_jni.o ${TBENCH_PATH}/client.o \
    ${TBENCH_PATH}/tbench_server_networked.o -lrt -pthread

env PATH=${JDK_PATH}/bin/:${PATH} CLASSPATH=${TBENCH_PATH}/tbench.jar ant
