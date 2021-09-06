#include "common.h"

#include <unistd.h>

#include <iostream>
#include <string>
#include <vector>

int batch;

void saveModel(const SMR& smr, const std::vector<SA>& HiddenLayers, \
               std::string modelFile) {
    cv::FileStorage fs(modelFile, cv::FileStorage::WRITE);

    // Save smr
    fs << "smr" << "{:" << "Weight" << smr.Weight << "Wgrad" << smr.Wgrad \
        << "cost" << smr.cost << "}";

    // Save HiddenLayers
    fs << "HiddenLayers" << "[";
    for (const SA& sa : HiddenLayers) {
        fs << "{:" << "W1" << sa.W1 << "W2" << sa.W2 << "b1" << sa.b1 \
            << "b2" << sa.b2 << "W1grad" << sa.W1grad << "W2grad" << sa.W2grad \
            << "b1grad" << sa.b1grad << "b2grad" << sa.b2grad \
            << "cost" << sa.cost << "}";
    }
    fs << "]";
    fs.release();
}

void
weightRandomInit(SA &sa, int inputsize, int hiddensize, int nsamples, double epsilon){

    double *pData;
    sa.W1 = cv::Mat::ones(hiddensize, inputsize, CV_64FC1);
    for(int i=0; i<hiddensize; i++){
        pData = sa.W1.ptr<double>(i);
        for(int j=0; j<inputsize; j++){
            pData[j] = cv::randu<double>();
        }
    }
    sa.W1 = sa.W1 * (2 * epsilon) - epsilon;

    sa.W2 = cv::Mat::ones(inputsize, hiddensize, CV_64FC1);
    for(int i=0; i<inputsize; i++){
        pData = sa.W2.ptr<double>(i);
        for(int j=0; j<hiddensize; j++){
            pData[j] = cv::randu<double>();
        }
    }
    sa.W2 = sa.W2 * (2 * epsilon) - epsilon;

    sa.b1 = cv::Mat::ones(hiddensize, 1, CV_64FC1);
    for(int j=0; j<hiddensize; j++){
        sa.b1.at<double>(j, 0) = cv::randu<double>();
    }
    sa.b1 = sa.b1 * (2 * epsilon) - epsilon;

    sa.b2 = cv::Mat::ones(inputsize, 1, CV_64FC1);
    for(int j=0; j<inputsize; j++){
        sa.b2.at<double>(j, 0) = cv::randu<double>();
    }
    sa.b2 = sa.b2 * (2 * epsilon) - epsilon;

    sa.W1grad = cv::Mat::zeros(hiddensize, inputsize, CV_64FC1);
    sa.W2grad = cv::Mat::zeros(inputsize, hiddensize, CV_64FC1);
    sa.b1grad = cv::Mat::zeros(hiddensize, 1, CV_64FC1);
    sa.b2grad = cv::Mat::zeros(inputsize, 1, CV_64FC1);
    sa.cost = 0.0;
}

void 
weightRandomInit(SMR &smr, int nclasses, int nfeatures, double epsilon){

    smr.Weight = cv::Mat::ones(nclasses, nfeatures, CV_64FC1);
    double *pData; 
    for(int i = 0; i<smr.Weight.rows; i++){
        pData = smr.Weight.ptr<double>(i);
        for(int j=0; j<smr.Weight.cols; j++){
            pData[j] = cv::randu<double>();        
        }
    }
    smr.Weight = smr.Weight * (2 * epsilon) - epsilon;
    smr.cost = 0.0;
    smr.Wgrad = cv::Mat::zeros(nclasses, nfeatures, CV_64FC1);
}

SAA
getSparseAutoencoderActivation(SA &sa, cv::Mat &data){
    SAA acti;
    data.copyTo(acti.aInput);
    acti.aHidden = sa.W1 * acti.aInput + repeat(sa.b1, 1, data.cols);
    acti.aHidden = sigmoid(acti.aHidden);
    acti.aOutput = sa.W2 * acti.aHidden + repeat(sa.b2, 1, data.cols);
    acti.aOutput = sigmoid(acti.aOutput);
    return acti;
}

void
sparseAutoencoderCost(SA &sa, cv::Mat &data, double lambda, double sparsityParam, double beta){

    int nfeatures = data.rows;
    int nsamples = data.cols;
    SAA acti = getSparseAutoencoderActivation(sa, data);

    cv::Mat errtp = acti.aOutput - data;
    pow(errtp, 2.0, errtp);
    errtp /= 2.0;
    double err = sum(errtp)[0] / nsamples;
    // now calculate pj which is the average activation of hidden units
    cv::Mat pj;
    reduce(acti.aHidden, pj, 1, CV_REDUCE_SUM);
    pj /= nsamples;
    // the second part is weight decay part
    double err2 = sum(sa.W1)[0] + sum(sa.W2)[0];
    err2 *= (lambda / 2.0);
    // the third part of overall cost function is the sparsity part
    cv::Mat err3;
    cv::Mat temp;
    temp = sparsityParam / pj;
    log(temp, temp);
    temp *= sparsityParam;
    temp.copyTo(err3);
    temp = (1 - sparsityParam) / (1 - pj);
    log(temp, temp);
    temp *= (1 - sparsityParam);
    err3 += temp;
    sa.cost = err + err2 + sum(err3)[0] * beta;

    // following are for calculating the grad of weights.
    cv::Mat delta3 = -(data - acti.aOutput);
    delta3 = delta3.mul(dsigmoid(acti.aOutput));
    cv::Mat temp2 = -sparsityParam / pj + (1 - sparsityParam) / (1 - pj);
    temp2 *= beta;
    cv::Mat delta2 = sa.W2.t() * delta3 + repeat(temp2, 1, nsamples);
    delta2 = delta2.mul(dsigmoid(acti.aHidden));
    cv::Mat nablaW1 = delta2 * acti.aInput.t();
    cv::Mat nablaW2 = delta3 * acti.aHidden.t();
    cv::Mat nablab1, nablab2; 
    delta3.copyTo(nablab2);
    delta2.copyTo(nablab1);
    sa.W1grad = nablaW1 / nsamples + lambda * sa.W1;
    sa.W2grad = nablaW2 / nsamples + lambda * sa.W2;
    reduce(nablab1, sa.b1grad, 1, CV_REDUCE_SUM);
    reduce(nablab2, sa.b2grad, 1, CV_REDUCE_SUM);
    sa.b1grad /= nsamples;
    sa.b2grad /= nsamples;
}

void
trainSparseAutoencoder(SA &sa, cv::Mat &data, int hiddenSize, double lambda, double sparsityParam, double beta, double lrate, int MaxIter){

    int nfeatures = data.rows;
    int nsamples = data.cols;
    weightRandomInit(sa, nfeatures, hiddenSize, nsamples, 0.12);
    int converge = 0;
    double lastcost = 0.0;
    std::cout<<"Sparse Autoencoder Learning: "<<std::endl;
    while(converge < MaxIter){

        int randomNum = rand() % (data.cols - batch);
        cv::Rect roi = cv::Rect(randomNum, 0, batch, data.rows);
        cv::Mat batchX = data(roi);

        sparseAutoencoderCost(sa, batchX, lambda, sparsityParam, beta);
        std::cout<<"learning step: "<<converge<<", Cost function value = "\
            <<sa.cost<<", randomNum = "<<randomNum<<std::endl;
        if(fabs((sa.cost - lastcost) ) <= 5e-5 && converge > 0) break;
        if(sa.cost <= 0.0) break;
        lastcost = sa.cost;
        sa.W1 -= lrate * sa.W1grad;
        sa.W2 -= lrate * sa.W2grad;
        sa.b1 -= lrate * sa.b1grad;
        sa.b2 -= lrate * sa.b2grad;
        ++ converge;
    }
}

void 
softmaxRegressionCost(cv::Mat &x, cv::Mat &y, SMR &smr, double lambda){

    int nsamples = x.cols;
    int nfeatures = x.rows;
    //calculate cost function
    cv::Mat theta(smr.Weight);
    cv::Mat M = theta * x;
    cv::Mat temp, temp2;
    temp = cv::Mat::ones(1, M.cols, CV_64FC1);
    reduce(M, temp, 0, CV_REDUCE_SUM);
    temp2 = repeat(temp, nclasses, 1);
    M -= temp2;
    exp(M, M);
    temp = cv::Mat::ones(1, M.cols, CV_64FC1);
    reduce(M, temp, 0, CV_REDUCE_SUM);
    temp2 = repeat(temp, nclasses, 1);
    divide(M, temp2, M); 
    cv::Mat groundTruth = cv::Mat::zeros(nclasses, nsamples, CV_64FC1);
    for(int i=0; i<nsamples; i++){
        groundTruth.at<double>(y.at<double>(0, i), i) = 1.0;
    }
    cv::Mat logM;
    log(M, logM);
    temp = groundTruth.mul(logM);
    smr.cost = - sum(temp)[0] / nsamples;
    cv::Mat theta2;
    pow(theta, 2.0, theta2);
    smr.cost += sum(theta2)[0] * lambda / 2;
    //calculate gradient
    temp = groundTruth - M;   
    temp = temp * x.t();
    smr.Wgrad = - temp / nsamples;
    smr.Wgrad += lambda * theta;
}

void 
trainSoftmaxRegression(SMR& smr, cv::Mat &x, cv::Mat &y, double lambda, double lrate, int MaxIter){
    int nfeatures = x.rows;
    int nsamples = x.cols;
    weightRandomInit(smr, nclasses, nfeatures, 0.12);
    int converge = 0;
    double lastcost = 0.0;
    std::cout<<"Softmax Regression Learning: "<<std::endl;
    while(converge < MaxIter){

        int randomNum = rand() % (x.cols - batch);
        cv::Rect roi = cv::Rect(randomNum, 0, batch, x.rows);
        cv::Mat batchX = x(roi);
        roi = cv::Rect(randomNum, 0, batch, y.rows);
        cv::Mat batchY = y(roi);

        softmaxRegressionCost(batchX, batchY, smr, lambda);
        std::cout<<"learning step: "<<converge<<", Cost function value = "\
            <<smr.cost<<", randomNum = "<<randomNum<<std::endl;
        if(fabs((smr.cost - lastcost) ) <= 1e-6 && converge > 0) break;
        if(smr.cost <= 0) break;
        lastcost = smr.cost;
        smr.Weight -= lrate * smr.Wgrad;
        ++ converge;
    }
}

void
fineTuneNetworkCost(cv::Mat &x, cv::Mat &y, std::vector<SA> &hLayers, SMR &smr, double lambda){

    int nfeatures = x.rows;
    int nsamples = x.cols;
    std::vector<cv::Mat> acti;

    acti.push_back(x);
    for(int i=1; i<=SparseAutoencoderLayers; i++){
        cv::Mat tmpacti = hLayers[i - 1].W1 * acti[i - 1] + repeat(hLayers[i - 1].b1, 1, x.cols);
        acti.push_back(sigmoid(tmpacti));
    }
    cv::Mat M = smr.Weight * acti[acti.size() - 1];
    cv::Mat tmp;
    reduce(M, tmp, 0, CV_REDUCE_MAX);
    M = M + repeat(tmp, M.rows, 1);
    cv::Mat p;
    exp(M, p);
    reduce(p, tmp, 0, CV_REDUCE_SUM);
    divide(p, repeat(tmp, p.rows, 1), p);

    cv::Mat groundTruth = cv::Mat::zeros(nclasses, nsamples, CV_64FC1);
    for(int i=0; i<nsamples; i++){
        groundTruth.at<double>(y.at<double>(0, i), i) = 1.0;
    }
    cv::Mat logP;
    log(p, logP);
    logP = logP.mul(groundTruth);
    smr.cost = - sum(logP)[0] / nsamples;
    pow(smr.Weight, 2.0, tmp);
    smr.cost += sum(tmp)[0] * lambda / 2;

    tmp = (groundTruth - p) * acti[acti.size() - 1].t();
    tmp /= -nsamples;
    smr.Wgrad = tmp + lambda * smr.Weight;

    std::vector<cv::Mat> delta(acti.size());
    delta[delta.size() -1] = -smr.Weight.t() * (groundTruth - p);
    delta[delta.size() -1] = delta[delta.size() -1].mul(dsigmoid(acti[acti.size() - 1]));
    for(int i = delta.size() - 2; i >= 0; i--){
        delta[i] = hLayers[i].W1.t() * delta[i + 1];
        delta[i] = delta[i].mul(dsigmoid(acti[i]));
    }
    for(int i=SparseAutoencoderLayers - 1; i >=0; i--){
        hLayers[i].W1grad = delta[i + 1] * acti[i].t();
        hLayers[i].W1grad /= nsamples;
        reduce(delta[i + 1], tmp, 1, CV_REDUCE_SUM);
        hLayers[i].b1grad = tmp / nsamples;
    }
    acti.clear();
    delta.clear();
}


void
trainFineTuneNetwork(cv::Mat &x, cv::Mat &y, std::vector<SA> &HiddenLayers, SMR &smr, double lambda, double lrate, int MaxIter){

    int converge = 0;
    double lastcost = 0.0;
    std::cout<<"Fine-Tune network Learning: "<<std::endl;
    while(converge < MaxIter){

        int randomNum = rand() % (x.cols - batch);
        cv::Rect roi = cv::Rect(randomNum, 0, batch, x.rows);
        cv::Mat batchX = x(roi);
        roi = cv::Rect(randomNum, 0, batch, y.rows);
        cv::Mat batchY = y(roi);

        fineTuneNetworkCost(batchX, batchY, HiddenLayers, smr, lambda);
        std::cout<<"learning step: "<<converge<<", Cost function value = "\
            <<smr.cost<<", randomNum = "<<randomNum<<std::endl;
        if(fabs((smr.cost - lastcost) / smr.cost) <= 1e-6 && converge > 0) break;
        if(smr.cost <= 0) break;
        lastcost = smr.cost;
        smr.Weight -= lrate * smr.Wgrad;
        for(int i=0; i<HiddenLayers.size(); i++){
            HiddenLayers[i].W1 -= lrate * HiddenLayers[i].W1grad;
            HiddenLayers[i].W2 -= lrate * HiddenLayers[i].W2grad;
            HiddenLayers[i].b1 -= lrate * HiddenLayers[i].b1grad;
            HiddenLayers[i].b2 -= lrate * HiddenLayers[i].b2grad;
        }
        ++ converge;
    }
}

void printHelp(char* argv[]) {
    std::cerr << std::endl;
    std::cerr << "Usage: " << argv[0] << " [-m mnist_dir]"  \
        << " [-f model_file]" << " [-t training_set_size]" \
        << " [-i max_training_iters]" << std::endl << std::endl;
    std::cerr << "-m : Directory where mnist data is stored (default: .mnist)" \
        << std::endl << std::endl;
    std::cerr << "-f : File to save model to" << std::endl << std::endl;
    std::cerr << "-t : Size of training set" << std::endl << std::endl;
    std::cerr << "-f : Maximum iterations during training" << std::endl \
        << std::endl;
    std::cerr << "-h : Print this help and exit" << std::endl << std::endl;
}


int main(int argc, char* argv[]) {
    std::string mnistDataDir = "mnist";
    std::string modelFile = "model.xml";
    int trainingSetSize = 60000; // Full MNIST training dataset
    int maxTrainingIter = 80000; // Max iters in original code

    int c;
    while ((c = getopt(argc, argv, "m:f:t:i:h")) != -1) {
        switch(c) {
            case 'm':
                mnistDataDir = optarg;
                break;
            case 'f':
                modelFile = optarg;
                break;
            case 't':
                trainingSetSize = atoi(optarg);
                break;
            case 'i':
                maxTrainingIter = atoi(optarg);
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

    std::vector<SA> HiddenLayers;
    SMR smr;

    cv::Mat trainX, trainY;
    std::string trainImages = mnistDataDir + "/train-images-idx3-ubyte";
    std::string trainLabels = mnistDataDir + "/train-labels-idx1-ubyte";
    readData(trainX, trainY, trainImages, trainLabels, 60000);

    // Allow training on a subset of the MNIST training data. The full
    // training dataset has 60000 entries
    cv::Rect roi = cv::Rect(0, 0, trainingSetSize, trainX.rows);
    trainX = trainX(roi);
    roi = cv::Rect(0, 0, trainingSetSize, trainY.rows);
    trainY = trainY(roi);

    std::cout <<"Read trainX successfully, including "<< trainX.rows \
        << " features and " << trainX.cols << " samples." << std::endl;
    std::cout <<"Read trainY successfully, including "<<trainY.cols<<" samples"<< std::endl;
    batch = trainX.cols / 100;
    // Finished reading data

    // pre-processing data. 
    // For some dataset, you may like to pre-processing the data,
    // however, in MNIST dataset, it actually already pre-processed. 
    // Scalar mean, stddev;
    // meanStdDev(trainX, mean, stddev);
    // cv::Mat normX = trainX - mean[0];
    // normX.copyTo(trainX);

    std::vector<cv::Mat> Activations;
    for(int i=0; i<SparseAutoencoderLayers; i++){
        cv::Mat tempX;
        if(i == 0) trainX.copyTo(tempX); else Activations[Activations.size() - 1].copyTo(tempX);
        SA tmpsa;
        trainSparseAutoencoder(tmpsa, tempX, 600, 3e-3, 0.1, 3, 2e-2, \
                maxTrainingIter);
        cv::Mat tmpacti = tmpsa.W1 * tempX + repeat(tmpsa.b1, 1, tempX.cols);
        tmpacti = sigmoid(tmpacti);
        HiddenLayers.push_back(tmpsa);
        Activations.push_back(tmpacti);
    }
    // Finished training Sparse Autoencoder
    // Now train Softmax.
    trainSoftmaxRegression(smr, Activations[Activations.size() - 1], trainY, \
            3e-3, 2e-2, maxTrainingIter);
    // Finetune using Back Propogation
    trainFineTuneNetwork(trainX, trainY, HiddenLayers, smr, 1e-4, 2e-2, \
            maxTrainingIter);

    saveModel(smr, HiddenLayers, modelFile);
}
