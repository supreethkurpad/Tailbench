#include "common.h"

#include <cstdlib>
#include <fstream>
#include <iostream>

cv::Mat 
sigmoid(cv::Mat &M){
    cv::Mat temp;
    exp(-M, temp);
    return 1.0 / (temp + 1.0);
}

cv::Mat 
dsigmoid(cv::Mat &a){
    cv::Mat res = 1.0 - a;
    res = res.mul(a);
    return res;
}

int 
ReverseInt (int i){
    unsigned char ch1, ch2, ch3, ch4;
    ch1 = i & 255;
    ch2 = (i >> 8) & 255;
    ch3 = (i >> 16) & 255;
    ch4 = (i >> 24) & 255;
    return((int) ch1 << 24) + ((int)ch2 << 16) + ((int)ch3 << 8) + ch4;
}

cv::Mat 
concatenateMat(std::vector<cv::Mat> &vec){

    int height = vec[0].rows;
    int width = vec[0].cols;
    cv::Mat res = cv::Mat::zeros(height * width, vec.size(), CV_64FC1);
    for(int i=0; i<vec.size(); i++){
        cv::Mat img(height, width, CV_64FC1);

        vec[i].convertTo(img, CV_64FC1);
        // reshape(int cn, int rows=0), cn is num of channels.
        cv::Mat ptmat = img.reshape(0, height * width);
        cv::Rect roi = cv::Rect(i, 0, ptmat.cols, ptmat.rows);
        cv::Mat subView = res(roi);
        ptmat.copyTo(subView);
    }
    divide(res, 255.0, res);
    return res;
}

void 
read_Mnist(std::string filename, std::vector<cv::Mat> &vec){
    std::ifstream file(filename, std::ios::binary);
    if (file.is_open()){
        int magic_number = 0;
        int number_of_images = 0;
        int n_rows = 0;
        int n_cols = 0;
        file.read((char*) &magic_number, sizeof(magic_number));
        magic_number = ReverseInt(magic_number);
        file.read((char*) &number_of_images,sizeof(number_of_images));
        number_of_images = ReverseInt(number_of_images);
        file.read((char*) &n_rows, sizeof(n_rows));
        n_rows = ReverseInt(n_rows);
        file.read((char*) &n_cols, sizeof(n_cols));
        n_cols = ReverseInt(n_cols);
        for(int i = 0; i < number_of_images; ++i){
            cv::Mat tpmat = cv::Mat::zeros(n_rows, n_cols, CV_8UC1);
            for(int r = 0; r < n_rows; ++r){
                for(int c = 0; c < n_cols; ++c){
                    unsigned char temp = 0;
                    file.read((char*) &temp, sizeof(temp));
                    tpmat.at<uchar>(r, c) = (int) temp;
                }
            }
            vec.push_back(tpmat);
        }
    } else {
        std::cerr << "Failed to read MNIST data from file " << filename \
            << std::endl;
        exit(-1);
    }
}

void 
read_Mnist_Label(std::string filename, cv::Mat &mat)
{
    std::ifstream file(filename, std::ios::binary);
    if (file.is_open()){
        int magic_number = 0;
        int number_of_images = 0;
        int n_rows = 0;
        int n_cols = 0;
        file.read((char*) &magic_number, sizeof(magic_number));
        magic_number = ReverseInt(magic_number);
        file.read((char*) &number_of_images,sizeof(number_of_images));
        number_of_images = ReverseInt(number_of_images);
        for(int i = 0; i < number_of_images; ++i){
            unsigned char temp = 0;
            file.read((char*) &temp, sizeof(temp));
            mat.at<double>(0, i) = (double)temp;
        }
    }
}

void
readData(cv::Mat &x, cv::Mat &y, std::string xpath, std::string ypath, int number_of_images){

    //read MNIST iamge into OpenCV Mat vector
    int image_size = 28 * 28;
    std::vector<cv::Mat> vec;
    //vec.resize(number_of_images, cv::Mat(28, 28, CV_8UC1));
    read_Mnist(xpath, vec);
    //read MNIST label into double vector
    y = cv::Mat::zeros(1, number_of_images, CV_64FC1);
    read_Mnist_Label(ypath, y);
    x = concatenateMat(vec);
}

