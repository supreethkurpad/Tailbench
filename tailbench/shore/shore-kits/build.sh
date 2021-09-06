#!/bin/bash

#run on mad6
export CXXFLAGS="-std=c++98 -g"
./autogen.sh
./configure --enable-shore6 --enable-dbgsymbols SHORE_HOME=../shore-mt/
make -j32
