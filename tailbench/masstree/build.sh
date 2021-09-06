#!/bin/bash

autoconf
./configure --disable-assertions --with-malloc=tcmalloc
make -j16
