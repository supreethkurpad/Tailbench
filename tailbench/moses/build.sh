#!/bin/bash

export TBENCH_PATH=${PWD}/../harness
export CPATH=${TBENCH_PATH}${CPATH:+:$CPATH}
./bjam toolset=gcc -j32 -q
