#ifndef __COMMON_H
#define __COMMON_H

#include "opencv2/core/core.hpp"
#include "opencv2/imgproc/imgproc.hpp"
#include "opencv2/highgui/highgui.hpp"

#include <string>
#include <vector>

static const int SparseAutoencoderLayers = 2;
static const int nclasses = 10;

typedef struct SparseAutoencoder{
    cv::Mat W1;
    cv::Mat W2;
    cv::Mat b1;
    cv::Mat b2;
    cv::Mat W1grad;
    cv::Mat W2grad;
    cv::Mat b1grad;
    cv::Mat b2grad;
    double cost;
} SA;

typedef struct SparseAutoencoderActivation{
    cv::Mat aInput;
    cv::Mat aHidden;
    cv::Mat aOutput;
} SAA;

typedef struct SoftmaxRegession{
    cv::Mat Weight;
    cv::Mat Wgrad;
    double cost;
} SMR;

struct SerializedMat {
    static const int rows = 28*28; // Each MNIST image is 28*28
    static const int cols = 1;
    static const int type = CV_64FC1;

    double data[rows * cols];

    void serialize(const cv::Mat& mat) {
        double* ptr = data;

        for (int r = 0; r < rows; ++r) {
            for (int c = 0; c < cols; ++c) {
                *ptr++ = mat.at<double>(r, c);
            }
        }
    }

    cv::Mat deserialize() {
        cv::Mat mat = cv::Mat::zeros(rows, cols, type);
        double* ptr = data;
        for (int r = 0; r < rows; ++r) {
            for (int c = 0; c < cols; ++c) {
                double& val = mat.at<double>(r, c);
                val = *ptr++;
            }
        }

        return mat;
    }
};

struct Result {
    int res;
};

cv::Mat sigmoid(cv::Mat &M);
cv::Mat dsigmoid(cv::Mat &a);

void
readData(cv::Mat &x, cv::Mat &y, std::string xpath, std::string ypath, int number_of_images);

#endif
