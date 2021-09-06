#ifndef __GETENV_H
#define __GETENV_H

#include <cstdlib>
#include <iostream>
#include <sstream>

template<typename T>
static T getOpt(const char* name, T defVal) {
    const char* opt = getenv(name);

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

#endif
