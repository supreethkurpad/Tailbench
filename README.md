# Tailbench

Tailbench is a benchmark suite and evaluation methodology for latency-critical applications. Tailbench consists of 9 different applications that can be used to obtain the tail latencies. The 9 applications are `harness, img-dnn, masstree, moses, shore, silo, specjbb, sphinx, xapian`. We have managed to fix all the compiling issues present in the original tailbench code. 

For more information about tailbench please refer to the following:

[[Paper]](http://people.csail.mit.edu/sanchez/papers/2016.tailbench.iiswc.pdf)
[[README]](https://github.com/iVishalr/Tailbench/blob/main/tailbench/README)

Note : This is an ongoing project.

# Setup

We have compiled all the 8 different applications and managed to run the benchmarking tool on our systems. The recommended OS would be Ubuntu 18.04. We have also written a simple setup script that downloads all the dependencies. It is advisable to run the script line by line instead of executing all the commands at once. 

The bash script is present in `tailbench-setup.sh`. We advice executing the script command by command. 

# Dataset

Please download the dataset from [TailBench Inputs](http://tailbench.csail.mit.edu/tailbench.inputs.tgz). You can also execute the following command in the terminal,

```bash
$ wget http://tailbench.csail.mit.edu/tailbench.inputs.tgz
```

OR 

```bash
$ aria2c http://tailbench.csail.mit.edu/tailbench.inputs.tgz
```

To extract the dataset, type the following command in terminal,

```bash
$ tar -xf tailbench.inputs.tgz
```

Also make sure to update the `tailbench/configs.sh` file if you have installed the dataset in a different location.
