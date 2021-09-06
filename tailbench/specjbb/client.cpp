#include "tbench_client.h"

#include <string.h>

#include <string>

void tBenchClientInit() { }

size_t tBenchClientGenReq(void* data) {
    std::string str = "Dummy Req";
    size_t len = str.size() + 1;
    memcpy(data, reinterpret_cast<const void*>(str.c_str()), len);

    return len;
}
