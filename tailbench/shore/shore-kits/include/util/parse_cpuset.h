#ifndef __PARSE_CPUSET_H
#define __PARSE_CPUSET_H

#include <string>
#include <vector>
#include <stdlib.h>

std::vector<int> parseCpuSet(std::string cpuSetStr, int maxCpu) {
    std::vector<int> cpuSet;
    cpuSet.resize(0);

    size_t strSize = cpuSetStr.size();
    if (strSize > 0) {
        size_t pos = 0;
        while (pos < strSize) {
            size_t newPos = cpuSetStr.find_first_of(",", pos);
            if (newPos == std::string::npos) newPos = strSize;

            std::string subStr = cpuSetStr.substr(pos, newPos - pos);

            size_t hyphen = subStr.find("-");
            int begin = 0;
            int end = maxCpu;
            if (hyphen == std::string::npos) {
                begin = atoi(subStr.c_str());
                end = begin;
            } else {
                begin = atoi(subStr.substr(0, hyphen).c_str());
                if (hyphen == subStr.size() - 1) end = maxCpu;
                else {
                    end = atoi(subStr.substr(hyphen+1, subStr.size() - hyphen - 1).c_str());
                }
            }

            for (int i = begin; i <= end; i++) cpuSet.push_back(i);
            pos = newPos+1;
        }
    }

    return cpuSet;
}

#endif
