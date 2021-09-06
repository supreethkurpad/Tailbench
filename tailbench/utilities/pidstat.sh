#!/bin/bash

# 使用这个脚本来监测某一个进程的CPU，memory，IO，context switch等各项性能指标
pidstat --human -u -w -r -d -p $1 1 >pidstat.out
