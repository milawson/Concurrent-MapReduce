cmake_minimum_required(VERSION 3.17)
project(Concurrent_MapReduce)

set(CMAKE_CXX_STANDARD 14)

add_executable(ConcurrentMapReduceTest mapreduce.h mapreduce.c test.c)
add_executable(ConcurrentMapReduceRunTests mapreduce.h mapreduce.c runTests.c)