#pragma once

#include <stdint.h>

const uint64_t CYCLES_PER_USEC = (uint64_t) 2600;

inline uint64_t rdtscll(void) {
    uint32_t a,d;
    asm volatile("rdtsc" : "=a" (a), "=d" (d));
    uint64_t ret = d;
    ret <<= 32;
    ret |= a;
    return ret;
}

inline double tscToMilliSec(uint64_t tsc) {
    return ((double) tsc / CYCLES_PER_USEC) / 1000;
}

inline uint64_t milliSecToTsc(double ms) {
    return (uint64_t) (ms * CYCLES_PER_USEC * 1000);
}
