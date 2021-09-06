// Original Author: Eric Yuan
// Blog: http://eric-yuan.me
// 
// Substantially modified by Harshad Kasture (harshad@csail.mit.edu)
//
// A deep net hand writing classifier.  Using sparse autoencoder and softmax
// regression.  First train sparse autoencoder layer by layer, then train
// softmax regression, and fine-tune the whole network.

#include "common.h"
#include "tbench_server.h"

#include "opencv2/core/core.hpp"
#include "opencv2/imgproc/imgproc.hpp"
#include "opencv2/highgui/highgui.hpp"

#include <unistd.h>
#include <math.h>

#include <atomic>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <random>
#include <string>

using namespace cv;
using namespace std;

#define ATD at<double>
#define elif else if

Mat 
resultProdict(const Mat &x, const vector<SA> &hLayers, const SMR &smr){

    vector<Mat> acti;
    acti.push_back(x);
    for(int i=1; i<=SparseAutoencoderLayers; i++){
        Mat tmpacti = hLayers[i - 1].W1 * acti[i - 1] + repeat(hLayers[i - 1].b1, 1, x.cols);
        acti.push_back(sigmoid(tmpacti));
    }
    Mat M = smr.Weight * acti[acti.size() - 1];
    Mat tmp;
    reduce(M, tmp, 0, CV_REDUCE_MAX);
    M = M + repeat(tmp, M.rows, 1);
    Mat p;
    exp(M, p);
    reduce(p, tmp, 0, CV_REDUCE_SUM);
    divide(p, repeat(tmp, p.rows, 1), p);
    log(p, tmp);
    //cout<<tmp.t()<<endl;
    Mat result = Mat::ones(1, tmp.cols, CV_64FC1);
    for(int i=0; i<tmp.cols; i++){
        double maxele = tmp.ATD(0, i);
        int which = 0;
        for(int j=1; j<tmp.rows; j++){
            if(tmp.ATD(j, i) > maxele){
                maxele = tmp.ATD(j, i);
                which = j;
            }
        }
        result.ATD(0, i) = which;
    }
    acti.clear();

    return result;
}

void loadModel(SMR& smr, vector<SA>& HiddenLayers, string modelFile) {
    FileStorage fs(modelFile, FileStorage::READ);

    FileNode smrNode = fs["smr"];
    smrNode["Weight"] >> smr.Weight;
    smrNode["Wgrad"] >> smr.Wgrad;
    smrNode["cost"] >> smr.cost;

    HiddenLayers.clear();
    FileNode layersNode = fs["HiddenLayers"];

    for (auto it = layersNode.begin(); it != layersNode.end(); ++it) {
        SA sa;
        (*it)["W1"] >> sa.W1;
        (*it)["W2"] >> sa.W2;
        (*it)["b1"] >> sa.b1;
        (*it)["b2"] >> sa.b2;
        (*it)["W1grad"] >> sa.W1grad;
        (*it)["W2grad"] >> sa.W2grad;
        (*it)["b1grad"] >> sa.b1grad;
        (*it)["b2grad"] >> sa.b2grad;
        (*it)["cost"] >> sa.cost;

        HiddenLayers.push_back(sa);
    }
}

void printHelp(char* argv[]) {
    cerr << endl;
    cerr << "Usage: " << argv[0] << " [-f model_file] [-n max_reqs]" \
        << " [-r threads] [-h]" << endl << endl;
    cerr << "-f : Name of model file to load " << "(default: model.xml)" \
        << endl; 
    cerr << "-n : Maximum number of requests "\
        << "(default: 6000; size of the full MNIST test dataset)" << endl;
    cerr << "-r : Number of worker threads" << endl;
    cerr << "-h : Print this help and exit" << endl;
}

class Worker {
    private:
        int tid;
        pthread_t thread;

        long nReqs;
        static atomic_llong nReqsTotal;
        static long maxReqs;
        static atomic_llong correct;

        SMR smr;
        vector<SA> hiddenLayers;

        long startReq() {
            ++nReqs;
            return ++nReqsTotal;
        }

        static void* run(void* ptr) {
            Worker* worker = reinterpret_cast<Worker*>(ptr);
            worker->doRun();

            return nullptr;
        }

        void doRun() {
            tBenchServerThreadStart();

            SerializedMat* smat;
            Result res;
            while (++nReqsTotal <= maxReqs) {
                ++nReqs;

                size_t len = tBenchRecvReq(reinterpret_cast<void**>(&smat));

                cv::Mat single_testX = smat->deserialize();

                Mat result = resultProdict(single_testX, hiddenLayers, smr);

                res.res = result.at<double>(0, 0);
                tBenchSendResp(reinterpret_cast<const void*>(&res), sizeof(res));
            }
        }

    public:
        Worker(int tid, const SMR& _smr, const vector<SA>& _hiddenLayers)
            : tid(tid) 
            , nReqs(0)
            , smr(_smr)
            , hiddenLayers(_hiddenLayers)
        { }

        void run() {
            pthread_create(&thread, nullptr, Worker::run, reinterpret_cast<void*>(this));
        }

        void join() {
            pthread_join(thread, nullptr);
        }

        static long correctDecodes() { return correct; }

        static void updateMaxReqs(long _maxReqs) { maxReqs = _maxReqs; }

};

atomic_llong Worker::nReqsTotal(0);
long Worker::maxReqs(0);
atomic_llong Worker::correct(0);

int 
main(int argc, char** argv)
{
    string modelFile = "model.xml";
    int maxReqs = 6000; // Full MNIST test dataset
    int nThreads = 1;

    int c;
    while ((c = getopt(argc, argv, "f:n:r:h")) != -1) {
        switch(c) {
            case 'f':
                modelFile = optarg;
                break;
            case 'n':
                maxReqs = atoi(optarg);
                break;
            case 'r':
                nThreads = atoi(optarg);
                break;
            case 'h':
                printHelp(argv);
                return 0;
                break;
            case '?':
                printHelp(argv);
                return -1;
                break;
        }
    }

    long start, end;
    start = clock();

    vector<SA> HiddenLayers;
    SMR smr;

    loadModel(smr, HiddenLayers, modelFile);

    tBenchServerInit(nThreads);
    Worker::updateMaxReqs(maxReqs);
    vector<Worker> workers;
    for (int t = 0; t < nThreads; ++t) {
        workers.push_back(Worker(t, smr, HiddenLayers));
    }

    for (int t = 0; t < nThreads; ++t) {
        workers[t].run();
    }

    for (int t = 0; t < nThreads; ++t) {
        workers[t].join();
    }

    cout<<"correct: "<<Worker::correctDecodes()<<", total: "\
        <<maxReqs<<", accuracy: "\
        <<double(Worker::correctDecodes()) / (double)(maxReqs)<<endl;

    end = clock();
    cout<<"End-to-end run time: "<<((double)(end - start)) / CLOCKS_PER_SEC<<" second"<<endl;

    return 0;
}
