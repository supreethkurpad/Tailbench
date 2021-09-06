CXX = g++
CXXFLAGS += -g -O3 $(shell pkg-config --cflags opencv) -std=c++0x
LDFLAGS = $(shell pkg-config --libs opencv)

TBENCH_PATH = ../harness
TBENCH_SERVER_OBJ = $(TBENCH_PATH)/tbench_server_networked.o
TBENCH_CLIENT_OBJ = $(TBENCH_PATH)/client.o $(TBENCH_PATH)/tbench_client_networked.o
TBENCH_INTEGRATED_OBJ = $(TBENCH_PATH)/client.o $(TBENCH_PATH)/tbench_server_integrated.o

CXXFLAGS += -I$(TBENCH_PATH)
LDFLAGS += -lrt -pthread

BINS = img-dnn_integrated img-dnn_server_networked img-dnn_client_networked train

.PHONY : all
all : $(BINS)

common.o : common.cpp common.h
	$(CXX) $(CXXFLAGS) $< -c -o $@

train.o : train.cpp common.h
	$(CXX) $(CXXFLAGS) $< -c -o $@

img-dnn.o : img-dnn.cpp common.h
	$(CXX) $(CXXFLAGS) $< -c -o $@

client.o : client.cpp common.h
	$(CXX) $(CXXFLAGS) $< -c -o $@

train : train.o common.o
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

img-dnn_integrated : img-dnn.o common.o client.o $(TBENCH_INTEGRATED_OBJ)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

img-dnn_server_networked : img-dnn.o common.o $(TBENCH_SERVER_OBJ)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

img-dnn_client_networked : common.o client.o $(TBENCH_CLIENT_OBJ)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

.PHONY : clean
clean:
	rm *.o $(BINS)
