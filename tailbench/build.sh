#!/bin/bash

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [[ $# -eq 0 ]]
then
    HARNESS_DIR=harness
    APP_DIRS="img-dnn masstree moses shore silo specjbb sphinx xapian"
else
    APP_DIRS=$@
fi

for dir in ${HARNESS_DIR} ${APP_DIRS}
do
    echo "Building $dir"
    cd ${ROOT}/${dir}
    ./build.sh > build.log
    EXIT_CODE=$?
    if [[ $EXIT_CODE -ne 0 ]]
    then
        echo "WARNING: Building $dir returned error status $EXIT_CODE"
    fi
    cd -
done
