#include "sm/shore/request.h"
#include "tbench_client.h"

#include <cstring>

/*******************************************************************************
 * API
 *******************************************************************************/
void tBenchClientInit() {} // Nothing to be done here

size_t tBenchClientGenReq(void* data) {
    Request req;
    req.xctType = 0; // Only ever need to send one type
    size_t len = sizeof(req);

    memcpy(data, reinterpret_cast<const void*>(&req), len);

    return len;
}
