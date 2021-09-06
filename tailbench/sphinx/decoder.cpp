#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "internal.h"
#include "tbench_server.h"
#include <pocketsphinx.h>
#include <err.h>

void doAsr() {
    tBenchServerThreadStart();

    err_set_logfp(NULL); // Get sphinx to be quiet
    ps_decoder_t *ps;
    cmd_ln_t *config;
    FILE *fh;
    char const *hyp, *uttid;
    int64_t bufsize = 1024*1024;
    int16* buf = nullptr;
    int rv;
    int32 score;

    config = cmd_ln_init(NULL, ps_args(), TRUE,
                 "-hmm", MODELDIR"/en-us/en-us",
                 "-lm", MODELDIR"/en-us/en-us.lm.bin",
                 "-dict", MODELDIR"/en-us/cmudict-en-us.dict",
                 NULL);
    if (config == NULL) throw AsrException("Could not init config");
    ps = ps_init(config);
    if (ps == NULL) throw AsrException("Could not init pocketsphinx");

    while (true) {
        size_t len = tBenchRecvReq(reinterpret_cast<void**>(&buf));

        rv = ps_start_utt(ps);
        if (rv < 0) throw AsrException("Could not start utterance");

        int16* cur = buf;
        int64_t remaining = len / sizeof(int16);

        while (remaining > 0) {
            size_t nsamp = std::min(remaining, bufsize);
            rv = ps_process_raw(ps, cur, nsamp, FALSE, FALSE);

            cur += nsamp;
            remaining -= nsamp;
        }

        rv = ps_end_utt(ps);
        if (rv < 0) throw AsrException("Could not end utterance");

        hyp = ps_get_hyp(ps, &score);
        if (hyp == NULL) AsrException("Could not get hypothesis");

        tBenchSendResp(reinterpret_cast<const void*>(hyp), strlen(hyp));
    }

    ps_free(ps);
    cmd_ln_free_r(config);
};

void usage() {
    std::cerr << "Usage: decoder [-t nthreads]" << std::endl;
}

int main(int argc, char *argv[])
{
    int nthreads = 1;

    int c;
    while((c = getopt(argc, argv, "t:")) != EOF) {
        switch(c) {
            case 't':
                nthreads = atoi(optarg);
                break;
            case '?':
                usage();
                return -1;
                break;
        }
    }

    std::vector<std::thread> threads;

    tBenchServerInit(nthreads);

    for (int i = 0; i < nthreads; i++)
        threads.push_back(std::thread(doAsr));

    // never reached
    for (auto& th : threads) th.join();

    tBenchServerFinish();

    return 0;
}
