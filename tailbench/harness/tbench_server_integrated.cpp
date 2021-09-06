/** $lic$
 * Copyright (C) 2016-2017 by Massachusetts Institute of Technology
 *
 * This file is part of TailBench.
 *
 * If you use this software in your research, we request that you reference the
 * TaiBench paper ("TailBench: A Benchmark Suite and Evaluation Methodology for
 * Latency-Critical Applications", Kasture and Sanchez, IISWC-2016) as the
 * source in any publications that use this software, and that you send us a
 * citation of your work.
 *
 * TailBench is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.
 */

#include "tbench_server.h"

#include <atomic>
#include <vector>

#include "helpers.h"
#include "server.h"

#include <assert.h>
#include <errno.h>
#include <string.h>
#include <sys/select.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>

/*******************************************************************************
 * IntegratedServer
 *******************************************************************************/
IntegratedServer::IntegratedServer(int nthreads) 
    : Server(nthreads)
    , Client(nthreads)
{ }

size_t IntegratedServer::recvReq(int id, void** data) {
    Request* req = Client::startReq();
    *data = reinterpret_cast<void*>(&req->data);
    uint64_t curNs = getCurNs();
    reqInfo[id].id = req->id;
    reqInfo[id].startNs = curNs;
    return req->len;
};

void IntegratedServer::sendResp(int id, const void* data, size_t len) {
    Response* resp = new Response();
    
    resp->type = RESPONSE;
    resp->id = reqInfo[id].id;
    resp->len = len;
    memcpy(reinterpret_cast<void*>(&resp->data), data, len);

    uint64_t curNs = getCurNs();
    assert(curNs > reqInfo[id].startNs);

    resp->svcNs = curNs - reqInfo[id].startNs;

    Client::finiReq(resp);

    delete resp;

    pthread_mutex_lock(&lock);
    ++finishedReqs;
    
    if (finishedReqs == warmupReqs) {
        Client::_startRoi();
    } else if (finishedReqs == warmupReqs + maxReqs) {
        Client::dumpStats();
        syscall(SYS_exit_group, 0);
    }

    pthread_mutex_unlock(&lock);
}


/*******************************************************************************
 * Per-thread State
 *******************************************************************************/
__thread int tid;

/*******************************************************************************
 * Global data
 *******************************************************************************/
std::atomic_int curTid;
IntegratedServer* server;

/*******************************************************************************
 * API
 *******************************************************************************/
void tBenchServerInit(int nthreads) {
    curTid = 0;
    server = new IntegratedServer(nthreads);
}

void tBenchServerThreadStart() {
    tid = curTid++;
}

void tBenchServerFinish() {
    server->dumpStats();
}

size_t tBenchRecvReq(void** data) {
    return server->recvReq(tid, data);
}

void tBenchSendResp(const void* data, size_t size) {
    return server->sendResp(tid, data, size);
}

