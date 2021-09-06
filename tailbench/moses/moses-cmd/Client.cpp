#include "tbench_client.h"

#include <cstring>
#include <string>

/*******************************************************************************
 * Tailbench client api implementation
 *******************************************************************************/
void tBenchClientInit() { }

size_t tBenchClientGenReq(void* data) {
    std::string sentence = "Hello World";
    size_t len = sentence.size() + 1;
    memcpy(data, reinterpret_cast<const void*>(sentence.c_str()), len);

    return len;
}


