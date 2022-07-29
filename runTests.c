//
// Created by Mitchell Lawson on 2021-12-16.
//

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "mapreduce.h"
#include <time.h>

int my_argc;
char* my_argv[5];

// These functions are inspired by the Google article, README
// document for this project and code supplied for students in
// the past where a "word counter" mapping function was used.

void Map(char *file_name) {
    FILE *fp = fopen(file_name, "r");
    assert(fp != NULL);

    char* line = NULL;
    size_t size = 0;
    while (getline(&line, &size, fp) != -1) {
        char *token, *dummy = line;
        while ((token = strsep(&dummy, " \t\n\r")) != NULL) {
            MR_Emit(token, "1");
        }
    }

    fclose(fp);
}

void Reduce(char *key, Getter get_next, int partition_number) {
    int count = 0;
    char *value;
    value = get_next(key, partition_number);
    while (value != NULL) {
        count++;
        value = get_next(key, partition_number);
    }
    printf("%s %d\n", key, count);
}

int main() {

    printf("Test Correct Outputs\n");
    printf("Test 1 (No repeating words)\n");
    my_argv[0] = "./test";
    my_argv[1] = "../file1.txt";
    my_argv[2] = NULL;
    my_argv[3] = NULL;
    my_argv[4] = NULL;
    my_argc = 2;
    MR_Run(my_argc, my_argv, Map, 10, Reduce, 10, MR_DefaultHashPartition);
    printf("\n\n\n");


    printf("Test Correct Outputs\n");
    printf("Test 2 (Repeating words)\n");
    my_argv[0] = "./test";
    my_argv[1] = "../file2.txt";
    my_argv[2] = NULL;
    my_argv[3] = NULL;
    my_argv[4] = NULL;
    my_argc = 2;
    MR_Run(my_argc, my_argv, Map, 10, Reduce, 10, MR_DefaultHashPartition);
    printf("\n\n\n");


    printf("Test Correct Outputs\n");
    printf("Test 3 (Multiple files with overlapping words)\n");
    my_argv[0] = "./test";
    my_argv[1] = "../file1.txt";
    my_argv[2] = "../file2.txt";
    my_argv[3] = "../file3.txt";
    my_argv[4] = NULL;
    my_argc = 4;
    MR_Run(my_argc, my_argv, Map, 10, Reduce, 10, MR_DefaultHashPartition);
    printf("\n\n\n");

    ////////////////////////////////////////////////////////////

    clock_t start, end;
    double cpu_time_used;
    double collection_of_cpu_time_used[3];

    printf("Test Analysis Cases\n");
    printf("Test 1 - Best Case (Same word x100000\n");
    my_argv[0] = "./test";
    my_argv[1] = "../file4.txt";
    my_argv[2] = NULL;
    my_argv[3] = NULL;
    my_argv[4] = NULL;
    my_argc = 2;
    start = clock();
    MR_Run(my_argc, my_argv, Map, 10, Reduce, 10, MR_DefaultHashPartition);
    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    collection_of_cpu_time_used[0] = cpu_time_used;
    printf("This test took %f\n\n\n", collection_of_cpu_time_used[0]);

    printf("Test Analysis Cases\n");
    printf("Test 2 - Average Case (100000 words from online texts)\n");
    my_argv[0] = "./test";
    my_argv[1] = "../file5.txt";
    my_argv[2] = NULL;
    my_argv[3] = NULL;
    my_argv[4] = NULL;
    my_argc = 2;
    start = clock();
    MR_Run(my_argc, my_argv, Map, 10, Reduce, 10, MR_DefaultHashPartition);
    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    collection_of_cpu_time_used[1] = cpu_time_used;
    printf("This test took %f\n\n\n", collection_of_cpu_time_used[1]);

    printf("Test Analysis Cases\n");
    printf("Test 3 - Worst Case (100000 words in the form of incremented number for even distribution)\n");
    my_argv[0] = "./test";
    my_argv[1] = "../file6.txt";
    my_argv[2] = NULL;
    my_argv[3] = NULL;
    my_argv[4] = NULL;
    my_argc = 2;
    start = clock();
    MR_Run(my_argc, my_argv, Map, 10, Reduce, 10, MR_DefaultHashPartition);
    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    collection_of_cpu_time_used[2] = cpu_time_used;
    printf("This test took %f\n\n\n", collection_of_cpu_time_used[2]);

    printf("Test 1 took: %f, Test 2 took: %f, Test 3 took: %f", collection_of_cpu_time_used[0], collection_of_cpu_time_used[1], collection_of_cpu_time_used[2]);

    ////////////////////////////////////////////////////////////

    if (0) {
        printf("Test Concurrency Analysis Cases\n");
        printf("Test 1 - Best Case (More threads than files)\n");
        my_argv[0] = "./test";
        my_argv[1] = "../file4.txt";
        my_argv[2] = "../file5.txt";
        my_argv[3] = "../file6.txt";
        my_argv[4] = NULL;
        my_argc = 4;
        start = clock();
        MR_Run(my_argc, my_argv, Map, 10, Reduce, 10, MR_DefaultHashPartition);
        end = clock();
        cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
        collection_of_cpu_time_used[0] = cpu_time_used;
        printf("This test took %f\n\n\n", collection_of_cpu_time_used[0]);

        printf("Test 2 - Average Case (1 thread per file)\n");
        my_argv[0] = "./test";
        my_argv[1] = "../file4.txt";
        my_argv[2] = "../file5.txt";
        my_argv[3] = "../file6.txt";
        my_argv[4] = NULL;
        my_argc = 4;
        start = clock();
        MR_Run(my_argc, my_argv, Map, 3, Reduce, 10, MR_DefaultHashPartition);
        end = clock();
        cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
        collection_of_cpu_time_used[1] = cpu_time_used;
        printf("This test took %f\n\n\n", collection_of_cpu_time_used[1]);

        printf("Test Analysis Cases\n");
        printf("Test 3 - Worst Case (2 thread)\n");
        my_argv[0] = "./test";
        my_argv[1] = "../file4.txt";
        my_argv[2] = "../file5.txt";
        my_argv[3] = "../file6.txt";
        my_argv[4] = NULL;
        my_argc = 4;
        start = clock();
        MR_Run(my_argc, my_argv, Map, 2, Reduce, 10, MR_DefaultHashPartition);
        end = clock();
        cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
        collection_of_cpu_time_used[2] = cpu_time_used;
        printf("This test took %f\n\n\n", collection_of_cpu_time_used[2]);

        printf("Test 1 took: %f, Test 2 took: %f, Test 3 took: %f", collection_of_cpu_time_used[0], collection_of_cpu_time_used[1], collection_of_cpu_time_used[2]);
    }

    if (1) {
        printf("Test Concurrency Analysis Cases\n");
        printf("Test 1 - Best Case (More threads than files)\n");
        my_argv[0] = "./test";
        my_argv[1] = "../file4.txt";
        my_argv[2] = "../file5.txt";
        my_argv[3] = "../file6.txt";
        my_argv[4] = NULL;
        my_argc = 4;
        start = clock();
        MR_Run(my_argc, my_argv, Map, 10, Reduce, 10, MR_DefaultHashPartition);
        end = clock();
        cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
        collection_of_cpu_time_used[0] = cpu_time_used;
        printf("This test took %f\n\n\n", collection_of_cpu_time_used[0]);

        printf("Test 2 - Average Case (1 thread per file)\n");
        my_argv[0] = "./test";
        my_argv[1] = "../file4.txt";
        my_argv[2] = "../file5.txt";
        my_argv[3] = "../file6.txt";
        my_argv[4] = NULL;
        my_argc = 4;
        start = clock();
        MR_Run(my_argc, my_argv, Map, 10, Reduce, 3, MR_DefaultHashPartition);
        end = clock();
        cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
        collection_of_cpu_time_used[1] = cpu_time_used;
        printf("This test took %f\n\n\n", collection_of_cpu_time_used[1]);

        printf("Test Analysis Cases\n");
        printf("Test 3 - Worst Case (2 thread)\n");
        my_argv[0] = "./test";
        my_argv[1] = "../file4.txt";
        my_argv[2] = "../file5.txt";
        my_argv[3] = "../file6.txt";
        my_argv[4] = NULL;
        my_argc = 4;
        start = clock();
        MR_Run(my_argc, my_argv, Map, 10, Reduce, 2, MR_DefaultHashPartition);
        end = clock();
        cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
        collection_of_cpu_time_used[2] = cpu_time_used;
        printf("This test took %f\n\n\n", collection_of_cpu_time_used[2]);

        printf("Test 1 took: %f, Test 2 took: %f, Test 3 took: %f", collection_of_cpu_time_used[0], collection_of_cpu_time_used[1], collection_of_cpu_time_used[2]);
    }







}
