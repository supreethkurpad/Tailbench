/** $lic$
 * Copyright (C) 2016-2017 by Massachusetts Institute of Technology
 *
 * This file is part of TailBench.
 *
 * If you use this software in your research, we request that you reference the
 * TaiBench paper ("TailBench: A Benchmark Suite and Evaluation Methodology for
 * Latency-Critical Applications", Kasture and Sanchez, IISWC-2016) as the
 * source in any publications that use this software, and that you send us a
 * citation of your work.
 *
 * TailBench is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.
 */

#include <jni.h>
#include <stdlib.h>

#include "tbench_server.h"
#include "tbench_tbench.h" // jni generated

JNIEXPORT void JNICALL Java_tbench_tbench_tBenchServerInit(JNIEnv* env, 
        jclass cls, jint nthreads) {
    tBenchServerInit(nthreads);
}

JNIEXPORT void JNICALL Java_tbench_tbench_tBenchServerThreadStart(JNIEnv* env, 
        jclass cls) {
    tBenchServerThreadStart();
}

JNIEXPORT void JNICALL Java_tbench_tbench_tBenchServerFinish(JNIEnv* env, 
        jclass cls) {
    tBenchServerFinish();
}

JNIEXPORT jbyteArray JNICALL Java_tbench_tbench_tBenchRecvReq(JNIEnv* env, 
        jclass cls) {
    char* cdata;
    size_t len = tBenchRecvReq(reinterpret_cast<void**>(&cdata));

    jbyte* jdata = new jbyte[len];
    for (int i = 0; i < len; ++i) jdata[i] = cdata[i];

    jbyteArray arr = env->NewByteArray(len);
    env->SetByteArrayRegion(arr, 0, len, jdata);

    delete[] jdata;

    return arr;
}

JNIEXPORT void JNICALL Java_tbench_tbench_tBenchSendResp(JNIEnv* env, 
        jclass cls, jbyteArray arr, jint size) {
    jsize len = env->GetArrayLength(arr);
    jbyte* bytes = env->GetByteArrayElements(arr, nullptr);

    tBenchSendResp(reinterpret_cast<const void*>(bytes), len * sizeof(jbyte));

    env->ReleaseByteArrayElements(arr, bytes, 0);
}
