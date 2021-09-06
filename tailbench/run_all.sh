#!/bin/bash

run=$1
run_dir="$PWD/output/$run"
latency_output="$PWD/output/$run.out"
mkdir -p $run_dir

run_benchmark () {
    benchmark=$1
    script=$2
    cd $benchmark
    benchmark_output_dir="$run_dir/$benchmark"
    mkdir -p $benchmark_output_dir
    # tailbench benchmarks can only use core 0
    taskset -c 0 bash ./$script

    echo "------------------------------------" >> $latency_output
    echo "$benchmark:" >> $latency_output
    python3 ../utilities/parselats.py lats.bin >> $latency_output

    cp lats.bin $benchmark_output_dir
    cp lats.txt $benchmark_output_dir
    cp ps.out $benchmark_output_dir
    cp pidstat.out $benchmark_output_dir
    cp vmstat.out $benchmark_output_dir
    cp iostat.out $benchmark_output_dir
    cd ..
}

integrated="run.sh"
networked="run_networked.sh"

rm $latency_output
# run_benchmark masstree $integrated
# run_benchmark moses $integrated
run_benchmark shore $integrated
# run_benchmark img-dnn $integrated
# run_benchmark silo $integrated
# run_benchmark sphinx $integrated
run_benchmark xapian $integrated
