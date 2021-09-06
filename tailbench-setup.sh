#!/bin/bash

WORKDIR=$PWD
TAILBENCHDIR=${WORKDIR}/tailbench

if [[ ! -d ${WORKDIR}/scratch ]]; then
	mkdir ${WORKDIR}/scratch
fi
cd ~
echo -e "\033[92mPerfoming apt updates\033[0m"

sudo apt-get update
sudo apt update

echo -e "\033[92mInstalling Software Properties and adding repositories\033[0m"

sudo apt-get -y install software-properties-common
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 3B4FE6ACC0B21F32
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 40976EAF437D05B5
sudo add-apt-repository "deb http://security.ubuntu.com/ubuntu xenial-security main"
sudo apt-get update

echo -e "\033[92mInstalling Tailbench Dependencies\033[0m"
sudo apt -y install openjdk-8-jdk
sudo apt-get -y --assume-yes install gcc automake autoconf libtool bison swig build-essential vim python3.7 pkg-config python3-pip zlib1g-dev uuid-dev libboost-all-dev cmake libgtk2.0-dev pkg-config libqt4-dev unzip wget libjasper-dev libpng-dev libjpeg-dev libtiff5-dev libgdk-pixbuf2.0-dev libopenexr-dev libbz2-dev tk-dev tcl-dev g++ git subversion automake libtool zlib1g-dev libicu-dev libboost-all-dev liblzma-dev python-dev graphviz imagemagick make cmake libgoogle-perftools-dev autoconf doxygen libgtop2-dev libncurses-dev ant libnuma-dev libmysqld-dev libaio-dev libjemalloc-dev libdb5.3++-dev libreadline-dev

cd ~
if [[ ! -d ./opencv-2.4.13.5 ]]; then
	wget https://github.com/opencv/opencv/archive/2.4.13.5.zip -O opencv-2.4.13.5.zip
	unzip opencv-2.4.13.5.zip
fi
cd ./opencv-2.4.13.5/
mkdir release
cd release/
cmake -G "Unix Makefiles" -DCMAKE_CXX_COMPILER=/usr/bin/g++ CMAKE_C_COMPILER=/usr/bin/gcc -DCMAKE_BUILD_TYPE=RELEASE -DCMAKE_INSTALL_PREFIX=/usr/local -DWITH_TBB=ON -DBUILD_NEW_PYTHON_SUPPORT=ON -DWITH_V4L=ON -DINSTALL_C_EXAMPLES=ON -DINSTALL_PYTHON_EXAMPLES=ON -DBUILD_EXAMPLES=ON -DWITH_QT=ON -DWITH_OPENGL=ON -DBUILD_FAT_JAVA_LIB=ON -DINSTALL_TO_MANGLED_PATHS=ON -DINSTALL_CREATE_DISTRIB=ON -DINSTALL_TESTS=ON -DENABLE_FAST_MATH=ON -DWITH_IMAGEIO=ON -DBUILD_SHARED_LIBS=OFF -DWITH_GSTREAMER=ON ..
sudo make all -j$(nproc)
sudo make install

cd ~
pip3 install developer-tools numpy matplotlib pandas scipy

echo -e "\033[92mSetting up environment variables\033[0m"

cd ${TAILBENCHDIR}
sh ./env.sh

cd ~
echo "\033[0;32mConfiguring Sphinx\033[0m\n"
cp -r ${TAILBENCHDIR}/sphinx ./sphinx
cd sphinx/

if [[ ! -d sphinxbase-5prealpha ]]; then
	tar -xf sphinxbase-5prealpha
fi

cd ./sphinxbase-5prealpha/
sudo ./autogen.sh && sudo ./configure && sudo make -j$(nproc) && sudo make install

cd ..
if [[ ! -d pocketsphinx-5prealpha ]]; then
	tar -xf pocketsphinx-5prealpha
fi

cd ./pocketsphinx-5prealpha/
sudo ./configure && sudo make -j$(nproc) && sudo make install

echo -e "\033[92mConfiguring Xapian\033[0m"
cd ~
cp -r ${TAILBENCHDIR}/xapian ./xapian
cd ./xapian

if [[ ! -d xapian-core-1.2.13 ]]; then
	tar -xf xapian-core-1.2.13
fi

cd xapian-core-1.2.13
sudo ./configure
sed -i 's/CXX = g++/CXX = g++ -std=c++03/g' Makefile
sudo make -j$(nproc) && sudo make install
cd ..	

cd ~
cd ${TAILBENCHDIR}
echo "JDK_PATH=/usr/lib/jvm/java-8-openjdk-amd64" > ./Makefile.config

echo -e "\033[92mBuilding harness\033[0m"
cd ./harness
./build.sh
cd ..

echo -e "\033[92mBuilding img-dnn\033[0m"
cd ./img-dnn
./build.sh
cd ..

echo -e "\033[92mBuilding masstree\033[0m"
cd ./masstree
./build.sh
cd ..

echo -e "\033[92mBuilding moses\033[0m"
cd ./moses
./build.sh
cd ..

echo -e "\033[92mBuilding shore\033[0m"
cd ./shore
./build.sh
cd ..

echo -e "\033[92mBuilding silo\033[0m"
cd ./silo

cd ./masstree
if [[ ! -f config.h ]]; then
	sudo ./configure
fi
cd ..
./build.sh
cd ..

echo -e "\033[92mBuilding specjbb\033[0m"
cd ./specjbb
./build.sh
cd ..

echo -e "\033[92mBuilding sphinx\033[0m"
cd ./sphinx
./build.sh
cd ..

echo -e "\033[92mBuilding xapian\033[0m"
cd ./xapian
./build.sh
cd ..
