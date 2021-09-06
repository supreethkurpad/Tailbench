#include <cstdlib>
#include <fstream>
#include <random>
#include <string>
#include <vector>

#include "internal.h"
#include "getopt.h"
#include "tbench_client.h"

/*******************************************************************************
 * Class Definitions
 *******************************************************************************/
class AudioSamples {
private:
    // filenames of audio samples
    std::vector<std::string> samples;

    // random number generator
    std::default_random_engine generator;
    std::uniform_int_distribution<int> distrib;

    void loadSamples(std::string an4Corpus, std::string samplesFile) {
        std::ifstream fd(samplesFile, std::ifstream::in);
        std::string line;
        while (std::getline(fd, line)) {
            if (fd.fail()) {
                throw AsrException("I/O error");
            }

            samples.push_back(an4Corpus + "/" + line);
        }
    }

public:
    AudioSamples(std::string an4Corpus, std::string samplesFile) {
        loadSamples(an4Corpus, samplesFile);
        distrib = std::uniform_int_distribution<int>(0, samples.size() - 1);
    }

    const std::string& get() {
        int idx = distrib(generator);
        return samples[idx];
    }
};

/*******************************************************************************
 * Global State
 *******************************************************************************/
AudioSamples* samples = nullptr;

/*******************************************************************************
 * API
 *******************************************************************************/
void tBenchClientInit() {
    std::string an4Corpus = getOpt<std::string>("TBENCH_AN4_CORPUS", ".");
    std::string samplesFile = getOpt<std::string>("TBENCH_AUDIO_SAMPLES", 
            "audio_samples");
    samples = new AudioSamples(an4Corpus, samplesFile);
}

size_t tBenchClientGenReq(void* data) {
    const std::string& sample = samples->get();

    std::streampos fsize = 0;
    std::ifstream file(sample, std::ios::binary);

    if (file.is_open()) {
        fsize = file.tellg();
        file.seekg(0, std::ios::end);
        fsize = file.tellg() - fsize;

        file.seekg(0, std::ios::beg);

        file.read(reinterpret_cast<char*>(data), fsize);

        file.close();
    } else {
        std::cerr << "Failed to open audio sample " << sample << std::endl;
        exit(-1);
    }

    return fsize;
}
