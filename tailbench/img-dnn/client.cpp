#include "common.h"
#include "getopt.h"
#include "tbench_client.h"

#include "opencv2/core/core.hpp"
#include "opencv2/imgproc/imgproc.hpp"
#include "opencv2/highgui/highgui.hpp"

#include <assert.h>

#include <iostream>
#include <random>
#include <string>

using namespace cv;

/*******************************************************************************
 * Class Definitions
 *******************************************************************************/
class Client {
    private:
        static Client* singleton;
        static const int totalImgs = 10000; // # images in MNIST test set

        std::string mnistDataDir;
        Mat testX;
        Mat testY;

        std::default_random_engine randGen;
        std::uniform_int_distribution<int> distrib;

        Client() {
            mnistDataDir = getOpt<std::string>("TBENCH_MNIST_DIR", "");
            std::string testImages = mnistDataDir + "/t10k-images-idx3-ubyte";
            std::string testLabels = mnistDataDir + "/t10k-labels-idx1-ubyte";

            readData(testX, testY, testImages, testLabels, totalImgs);
            std::cout << "Read testX successfully, including " << testX.rows \
                << " features and " << testX.cols << " samples." << std::endl;
            std::cout << "Read testY successfully, including " << testY.cols \
                << " samples" << std::endl;

            distrib = std::uniform_int_distribution<int>(0, totalImgs - 1);
        }

    public:
        static void init() {
            singleton = new Client();
        }

        static Client* getSingleton() {
            return singleton;
        }

        size_t get(void* buf) {
            int req = distrib(randGen);
            assert(req < totalImgs);

            SerializedMat smat;
            cv::Rect test_roi = cv::Rect(req, 0, 1, testX.rows);
            Mat single_testX = testX(test_roi);

            smat.serialize(single_testX);
            size_t len = sizeof(smat);
            memcpy(buf, reinterpret_cast<const void*>(&smat), len);

            return len;
        }
};

Client* Client::singleton = nullptr;

/*******************************************************************************
 * API 
 *******************************************************************************/
void tBenchClientInit() {
    Client::init();
}

size_t tBenchClientGenReq(void *data) {
    return Client::getSingleton()->get(data);
}
