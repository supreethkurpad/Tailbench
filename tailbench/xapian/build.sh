#/bin/bash

# Install Xapian core if not already present
if [[ ! -d xapian-core-1.2.13 ]]
then
    tar -xf xapian-core-1.2.13.tar.gz
    cd xapian-core-1.2.13
    mkdir install
    ./configure --prefix=$PWD/install CXXFLAGS='-std=c++98'
    make -j16
    make install
    cd -
fi

# Build search engine
make
