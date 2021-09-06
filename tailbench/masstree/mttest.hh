#ifndef __MTTEST_HH
#define __MTTEST_HH

#include "kvrandom.hh"
#include "str.hh"

const int mycsbaAggrFactor = 256;
const uint32_t mycsbaAggrMask = mycsbaAggrFactor - 1;
const long mycsbaSeed = 3242323423L;
const int mycsbaDbSize = 1000000;
const int mycsbaKeySize = 4 + 18 + 1;
const int mycsbaValSize = 12; // int32_t can be up to 2B => 10 digits + minus sign

enum ReqType { GET, PUT };
enum Status { SUCCESS, FAILURE };

struct Request {
    ReqType type;
    char key[mycsbaKeySize];
    char val[mycsbaValSize];
};

struct Response {
    Status status;
};

static void genKeyVal(kvrandom_lcg_nr& rand, char* key, char* val) {
    strcpy(key, "user");
    int p = 4;
    for (int i = 0; i < 18; ++i, ++p) {
        key[p] = '0' + (rand.next() % 10);
    }

    key[p] = 0;

    int32_t value = static_cast<int32_t>(rand.next());
    sprintf(val, "%d", value);

    return;
}

#endif
