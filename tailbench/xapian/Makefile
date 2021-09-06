
# Build settings
TBENCH_PATH = ../harness

TBENCH_INC = $(TBENCH_PATH)/tbench_client.h $(TBENCH_PATH)/tbench_server.h

TBENCH_SERVER_OBJ = $(TBENCH_PATH)/tbench_server_networked.o
TBENCH_CLIENT_OBJ = $(TBENCH_PATH)/client.o $(TBENCH_PATH)/tbench_client_networked.o
TBENCH_INTEGRATED_OBJ = $(TBENCH_PATH)/client.o $(TBENCH_PATH)/tbench_server_integrated.o

CXX = g++
XAPIAN_INSTALL_PATH = ./xapian-core-1.2.13/install/bin
CXXFLAGS = -Wall -W -O3 -g -std=c++0x `$(XAPIAN_INSTALL_PATH)/xapian-config --cxxflags` -I$(TBENCH_PATH)
LIBS = `$(XAPIAN_INSTALL_PATH)/xapian-config --libs` -lpthread -lrt

# Targets and sources

XAPIAN_INTEGRATED = xapian_integrated
XAPIAN_NETWORKED_SERVER = xapian_networked_server
XAPIAN_NETWORKED_CLIENT = xapian_networked_client

SERVER_SRCS = main.cpp server.cpp
SERVER_HDRS = tsc.h server.h

CLIENT_SRCS = client.cpp

GENTERMS = genTerms
GENTERMS_SRCS = genTerms.cpp

# Build rules
BIN = $(XAPIAN_INTEGRATED) $(GENTERMS) $(XAPIAN_NETWORKED_SERVER) \
	  $(XAPIAN_NETWORKED_CLIENT)

all : $(BIN)

$(XAPIAN_INTEGRATED) : main.o server.o client.o $(TBENCH_INTEGRATED_OBJ)
	$(CXX) -o $@ $^ $(LIBS)

$(XAPIAN_NETWORKED_SERVER) : main.o server.o $(TBENCH_SERVER_OBJ)
	$(CXX) -o $@ $^ $(LIBS)

$(XAPIAN_NETWORKED_CLIENT) : client.o $(TBENCH_CLIENT_OBJ)
	$(CXX) -o $@ $^ $(LIBS)

$(GENTERMS) : $(GENTERMS_SRCS) Makefile
	$(CXX) $(CXXFLAGS) -o $@ $(GENTERMS_SRCS) $(LIBS)

main.o : main.cpp server.h $(TBENCH_INC)
	$(CXX) $(CXXFLAGS) -c -o $@ $<

server.o : server.cpp server.h tsc.h $(TBENCH_INC)
	$(CXX) $(CXXFLAGS) -c -o $@ $<

client.o : client.cpp $(TBENCH_INC)
	$(CXX) $(CXXFLAGS) -c -o $@ $<

clean:
	rm -f $(BIN)
