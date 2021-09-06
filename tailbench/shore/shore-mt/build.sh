export CXXFLAGS="-std=c++98 -g"
./bootstrap
./configure --enable-dora --enable-dbgsymbols
make -j32
