#include "bench.h"
#include "../macros.h"
#include "request.h"
#include "tbench_client.h"
#include "../util.h"

#include <cstring>

/*******************************************************************************
 * Class Definitions
 *******************************************************************************/
class Client {
    private:
        struct WorkloadDesc {
            ReqType type;
            double frequency;
        };

        static unsigned g_txn_workload_mix[5];
        static unsigned long seed;
        static Client* singleton;

        std::vector<WorkloadDesc> workload;
        util::fast_random randgen;

        Client() : randgen(seed) 
        { 
            for (size_t i = 0; i < ARRAY_NELEMS(g_txn_workload_mix); ++i) {
                WorkloadDesc w = { .type = static_cast<ReqType>(i), 
                    .frequency = static_cast<double>(g_txn_workload_mix[i]) / 100.0 };
                workload.push_back(w);
            }
        }

    public:
        static void init() {
            singleton = new Client();
        }

        static Client* getSingleton() { return singleton; }

        Request getReq() {
            Request req;

            double d = randgen.next_uniform();
            for (size_t i = 0; i < workload.size(); ++i) {
                if (((i + 1) == workload.size()) ||
                        (d < workload[i].frequency)) {
                    req.type = static_cast<ReqType>(i);
                    break;
                }

                d -= workload[i].frequency;
            }
            
            return req;
        }
};

/*******************************************************************************
 * Global State
 *******************************************************************************/
unsigned Client::g_txn_workload_mix[] = { 45, 43, 4, 4, 4 }; // default TPC-C workload mix
unsigned long Client::seed = 23984543;
Client* Client::singleton = nullptr;

/*******************************************************************************
 * API
 *******************************************************************************/
void tBenchClientInit() {
    Client::init();
}

size_t tBenchClientGenReq(void* data) {
    Request req = Client::getSingleton()->getReq();
    memcpy(data, reinterpret_cast<const void*>(&req), sizeof(req));
    return sizeof(req);
}
