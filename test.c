//
// Created by Mitchell Lawson on 2021-12-16.
//

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "mapreduce.h"

// These functions came from a wordcount application recommended in the suppl

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

int main(int argc, char *argv[]) {
    MR_Run(argc, argv, Map, 10, Reduce, 10, MR_DefaultHashPartition);
}
