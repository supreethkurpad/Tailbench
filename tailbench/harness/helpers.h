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

#ifndef __HELPERS_H
#define __HELPERS_H

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>

#include <iostream>
#include <sstream>

template<typename T>
static T getOpt(const char* name, T defVal) {
    const char* opt = getenv(name);

    std::cout << name << " = " << opt << std::endl;
    if (!opt) return defVal;
    std::stringstream ss(opt);
    if (ss.str().length() == 0) return defVal;
    T res;
    ss >> res;
    if (ss.fail()) {
        std::cerr << "WARNING: Option " << name << "(" << opt << ") could not"\
            << " be parsed, using default" << std::endl;
        return defVal;
    }   
    return res;
}

static uint64_t getCurNs() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    uint64_t t = ts.tv_sec*1000*1000*1000 + ts.tv_nsec;
    return t;
}

static void sleepUntil(uint64_t targetNs) {
    uint64_t curNs = getCurNs();
    while (curNs < targetNs) {
        uint64_t diffNs = targetNs - curNs;
        struct timespec ts = {(time_t)(diffNs/(1000*1000*1000)), 
            (time_t)(diffNs % (1000*1000*1000))};
        nanosleep(&ts, NULL); //not guaranteed, hence the loop
        curNs = getCurNs();
    }
}

static int sendfull(int fd, const char* msg, int len, int flags) {
    int remaining = len;
    const char* cur = msg;
    int sent;

    while (remaining > 0) {
        sent = send(fd, reinterpret_cast<const void*>(cur), remaining, flags);
        if (sent == -1) {
            std::cerr << "send() failed: " << strerror(errno) << std::endl;
            break;
        }
        cur += sent;
        remaining -= sent;
    }

    return (len - remaining);
}

static int recvfull(int fd, char* msg, int len, int flags) {
    int remaining = len;
    char* cur = msg;
    int recvd;

    while (remaining > 0) {
        recvd = recv(fd, reinterpret_cast<void*>(cur), len, flags);
        if ((recvd == -1) || (recvd == 0)) break;
        cur += recvd;
        remaining -= recvd;
    }

    return (len - remaining);
}

#endif
