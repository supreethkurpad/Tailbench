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

package tbench;

public class tbench {
    public static native void tBenchServerInit(int nthreads);
    public static native void tBenchServerThreadStart();
    public static native void tBenchServerFinish();
    public static native byte[] tBenchRecvReq();
    public static native void tBenchSendResp(byte[] data, int size);

    static {
        System.loadLibrary("tbench_jni");
    }
}
