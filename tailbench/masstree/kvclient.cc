#include "kvrandom.hh"
#include "mttest.hh"
#include "str.hh"
#include "tbench_client.h"

#include <cstring>
#include <vector>

/*******************************************************************************
 * Class Definitions
 *******************************************************************************/
class Client {
    private:
        std::vector<Request> reqs;
        std::vector<Request>::iterator cur;
        static Client* singleton;

        Client();

    public:
        static Client* getSingleton() { 
            if (!singleton) singleton = new Client();
            return singleton;
        };

        const Request* getReq();
};

Client::Client() {
    kvrandom_lcg_nr rand;
    rand.reset(mycsbaSeed);

    Request req;

    for (int n = 0; n < mycsbaDbSize; ++n) {
        genKeyVal(rand, req.key, req.val);
        if (n % 2 == 0) 
            req.type = GET;
        else 
            req.type = PUT;
        reqs.push_back(req);
    }

    cur = reqs.begin();
}

const Request* Client::getReq() { 
    const Request& req = *cur;
    ++cur;
    if (cur == reqs.end()) cur = reqs.begin();

    return &req;
}

/*******************************************************************************
 * Global State
 *******************************************************************************/
Client* Client::singleton = nullptr;

/*******************************************************************************
 * API
 *******************************************************************************/

void tBenchClientInit() {}

size_t tBenchClientGenReq(void* data) {
    Client* client = Client::getSingleton();
    char* cur = reinterpret_cast<char*>(data);

    for (int i = 0; i < mycsbaAggrFactor; ++i) {
        const Request* req = client->getReq();
        memcpy(reinterpret_cast<void*>(cur), 
                reinterpret_cast<const void*>(req), 
                sizeof(Request));
        cur += sizeof(Request);
    }
    return mycsbaAggrFactor * sizeof(Request);
}

