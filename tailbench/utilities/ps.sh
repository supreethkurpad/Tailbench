#!/bin/bash

# 使用这个脚本来监测某一个进程的CPU，memory等各项性能指标
# 输出的性能指标项目分别为：
# pid,
# %men: 程序使用的内存占机器内存的百分比
# drs: data resident set size，除了代码外，数据所占用的物理页的大小
# maj_flt: major page fault
# min_flt: minor page fault
# sz: size in physical pages of the core image of the process. 
#     This includes text, data, and stack space.
#     Device mappings are currently excluded;
# rsz: resident set size, in kiloBytes
# vsz: virtual memory size of the process in KiB (1024-byte units)
#      这边要注意单位，似乎KB是1024，而kB是1000

rm -rf ps.out
while :; do
    ps -p $1 -ho pid,state,%cpu,%mem,sz,vsz,rsz,drs,maj_flt,min_flt >>ps.out
    sleep 1
done
