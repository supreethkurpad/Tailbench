#include "getopt.h"
#include "tbench_client.h"

#include <unistd.h>

#include <cstring>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <vector>

/*******************************************************************************
 * Class Definitions
 *******************************************************************************/
class TermSet {
    private:
        pthread_mutex_t lock;
        std::vector<std::string> terms;
        std::default_random_engine randEngine;
        std::uniform_int_distribution<unsigned long> termGen;
    public:
        TermSet(std::string termsFile) {
            pthread_mutex_init(&lock, NULL);

            std::ifstream fin(termsFile);
            if (fin.fail()) {
                std::cerr << "Error opening terms file" << std::endl;
                exit(-1);
            }

            const unsigned MAX_TERM_LEN = 128;
            char term[MAX_TERM_LEN];
            unsigned long termCount = 0;
            while (true) {
                fin.getline(term, MAX_TERM_LEN);
                if (fin.eof()) break;

                ++termCount;
                if (fin.fail() && fin.gcount() == MAX_TERM_LEN) {
                    std::cerr << "Term #" << termCount << " too big" << std::endl;
                    exit(-1);
                }

                terms.push_back(std::string(term));
            }

            fin.close();
            termGen = std::uniform_int_distribution<unsigned long>(0, \
                    termCount - 1);
        }

        ~TermSet() {}

        void acquireLock() { pthread_mutex_lock(&lock); }

        void releaseLock() { pthread_mutex_unlock(&lock); }

        const std::string& getTerm() {
            acquireLock();
            unsigned long idx = termGen(randEngine);
            releaseLock();
            return terms[idx];
        }
};

/*******************************************************************************
 * Global Data
 *******************************************************************************/
TermSet* termSet = nullptr;

/*******************************************************************************
 * Liblat API
 *******************************************************************************/
void tBenchClientInit() {
    std::string termsFile = getOpt<std::string>("TBENCH_TERMS_FILE", "terms.in");
    termSet = new TermSet(termsFile);
}

size_t tBenchClientGenReq(void* data) {
    std::string term = termSet->getTerm();
    size_t len = term.size();

    memcpy(data, reinterpret_cast<const void*>(term.c_str()), len + 1);

    return len + 1;
}
