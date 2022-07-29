//
// Created by Mitchell Lawson on 2021-12-15.
//

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include "mapreduce.h"
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>

/////////////////////////
// Global Variables
/////////////////////////
unsigned long long primeNumber = 65003; // Some prime number for hashing
struct hashTable** p; //Partition array
Partitioner partitioner;
Mapper mapper;
Reducer reducer;
int TABLE_SIZE = 1543; //this number was given in the word count file "mapper a nd reducer functions"
int partition_number;
pthread_mutex_t filelock = PTHREAD_MUTEX_INITIALIZER;
int fileNumber;
int current_file;
int current_partition;
pthread_mutex_t *partitionlock;
struct bucket** reducenode;

/////////////////////////
// Hash Code
////////////////////////

// Node found in a bucket
struct node {
    char* value;
    struct node* next;
};

// Bucket that stores nodes
struct bucket {
    char* key;
    struct bucket* next;
    struct node* node;
    struct node* current;
    int size;
};

// The actual hash table/map
struct hashTable {
    int size;
    struct bucket** table;
    pthread_mutex_t* lock;
    pthread_mutex_t keyLock;
    int bucketSize;
};

/**
 * This function creates a hash table of a user defined size and assigns it's values to null.
 * @param size The size of the table.
 * @return the new hashTable.
 */
struct hashTable *makeTable(int size) {
    struct hashTable* newTable = (struct hashTable*)malloc(sizeof(struct hashTable));

    newTable->size = size;
    newTable->table = malloc(sizeof(struct bucket*) * size);
    newTable->lock = malloc(sizeof(pthread_mutex_t) * size);
    pthread_mutex_init(&(newTable->keyLock), NULL);
    newTable->bucketSize = 0;

    for (int i = 0; i < size; i++) {
        newTable->table[i] = NULL;

        if(pthread_mutex_init(&(newTable->lock[i]), NULL)!=0) {
            printf("WOW :(\n... The initialization for the locks has failed!!!\n");
        }
    }

    return newTable;
}

/**
 * This function will find the hash value for some string.
 * @param string The string.
 * @return The hash value or hash code.
 */
unsigned long long hashCode(char* string) {
    unsigned long long hashValue = primeNumber; // Must be a large datatype due to the possible size.

    int currentChar;

    while ((currentChar = *string++) != '\0') {
        hashValue = ((hashValue << 5) + hashValue) + currentChar;
    }

    return hashValue % TABLE_SIZE;
}

/**
 * This function will insert a new element into the correct bucket of the hash table and create a new node.
 * @param table the hash table being used.
 * @param key the key for the hash table.
 * @param value the actual value for the hashtable.
 */
void hashInsert(struct hashTable* table, char* key, char* value) {
    long long positionInTable = hashCode(key) % table->size; //Might need to change data type to long but int should be ok.

    pthread_mutex_t* lock = table->lock + positionInTable;

    // Initialize new node.
    struct node* newNode = malloc(sizeof(struct node));
    newNode->value = strdup(value); // duplicates a string
    newNode->next = NULL;

    pthread_mutex_lock(lock);

    struct bucket* currentBucket= table->table[positionInTable];
    struct bucket* tempBucket = currentBucket;

    while(tempBucket) {
        if (strcmp(tempBucket->key, key) == 0) {
            struct node* currentNode = currentBucket->node;
            newNode->next = currentNode;
            tempBucket->size++;
            tempBucket->node = newNode;
            tempBucket->current = newNode;

            pthread_mutex_unlock(lock);

            //currentBucket->node = newNode;
            return;
        }

        tempBucket = tempBucket->next;
    }

    pthread_mutex_lock(&(table->keyLock));

    table->bucketSize++;

    pthread_mutex_unlock(&table->keyLock);

    struct bucket* newBucket = malloc(sizeof(struct bucket));
    newBucket->key = strdup(key);
    //newBucket->node = malloc(sizeof(struct node));
    newBucket->node = newNode;
    newBucket->current = newBucket->node;
    newBucket->next = currentBucket;

    table->table[positionInTable] = newBucket;

    pthread_mutex_unlock(lock);
}



/////////////////////////
// "Worker" functions
/*
 * From Stack Overflow:
 * A worker is something you give a task and continue in your process, while the worker (or multiple workers)
 * process the task on a different thread. When they finish they let you know about it via a call back
 * method. I.e. a special method provided on the initial call gets called.
 */
/////////////////////////

int keyComparator(const void *s1, const void *s2)
{
    struct bucket **n1 = (struct bucket **)s1;
    struct bucket **n2 = (struct bucket **)s2;
    if(*n1==NULL && *n2==NULL) {
        return 0;
    } else if(*n1==NULL) {
        return -1;
    } else if(*n2==NULL) {
        return 1;
    } else {
        return strcmp((*n1)->key,(*n2)->key);
    }
}

char* getNext(char* key, int partition_num) {

    struct bucket* tempBucket = reducenode[partition_num];
    struct node* address = tempBucket->current;

    if(address == NULL) {
        return NULL;
    }

    tempBucket->current=address->next;

    return address->value;
}



/////////////////////////
// File functions
/////////////////////////

void* callMap(char* fileName){
    mapper(fileName);
    return NULL;
}

void reduceHelper(int i){
    if(p[i]==NULL)
        return;
    struct hashTable* tempTable = p[i];
    struct bucket* list[p[i]->bucketSize];
    long long x=0;
    for(int j=0;j<TABLE_SIZE;j++) {
        if(tempTable->table[j] ==NULL)
            continue;
        struct bucket* tempBucket = tempTable->table[j];
        while(tempBucket){
            list[x]=tempBucket;
            x++;
            tempBucket=tempBucket->next;
        }
    }

    qsort(list,p[i]->bucketSize,sizeof(struct Bucket*), keyComparator);
    for(int k=0; k<x; k++){
        reducenode[i] = list[k];
        reducer(list[k]->key, getNext, i);
    }
}

void* callReduce(){
    while(1){
        pthread_mutex_lock(&filelock);
        int x = 0;
        if(current_partition>=partition_number){
            pthread_mutex_unlock(&filelock);
            return NULL;
        }
        if(current_partition<partition_number){
            x=current_partition;
            current_partition++;
        }
        pthread_mutex_unlock(&filelock);
        reduceHelper(x);
    }
}

void *findFile(void *files)
{
    char **arguments = (char**) files;
    while(1){
        pthread_mutex_lock(&filelock);
        int x = 0;
        if(current_file>fileNumber){
            pthread_mutex_unlock(&filelock);
            return NULL;
        }
        if(current_file<=fileNumber){
            x=current_file;
            current_file++;
        }
        pthread_mutex_unlock(&filelock);
        callMap(arguments[x]);
    }
    return NULL;
}

void freeTable(struct hashTable* table) {
    for(int i=0;i<table->size;i++) {
        struct bucket* list1=table->table[i];
        struct bucket* temp2=list1;
        pthread_mutex_t *l=&(table->lock[i]);
        while(temp2) {
            struct bucket* tempBucket=temp2;
            struct node* tempNode=tempBucket->node;

            while(tempNode) {
                struct node* currentNode=tempNode;
                tempNode=tempNode->next;
                free(currentNode->value);
                free(currentNode);
            }

            temp2=temp2->next;
            free(tempBucket->key);
            free(tempBucket);
        }
        pthread_mutex_destroy(l);
    }
    free(table->lock);
    free(table->table);
    free(table);
}

/////////////////////////
// The "main" functions
/////////////////////////

/**
 * Default hash partition signature from given header file.
 *
 *
 * @param key
 * @param num_partitions
 * @return
 */
unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hashValue = primeNumber;

    int currentChar;

    while ((currentChar = *key++) != '\0') {
        hashValue = ((hashValue << 5) + hashValue) + currentChar;
    }

    return hashValue % num_partitions;
}

void MR_Emit(char *key, char *value){ //Key -> tocket, Value -> 1
    long partitionNumber;
    //printf("In MR_EMIT\n");
    if(partitioner!=NULL){
        partitionNumber = partitioner(key,partition_number);
    }
    else{
        partitionNumber= MR_DefaultHashPartition(key,partition_number);
    }
    hashInsert(p[partitionNumber],key,value);
}


void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers,
            Partitioner partition)
{
    //printf("IN MR_RUN\n");
    current_partition=0;
    current_file=1;
    partitioner=partition;
    mapper=map;
    reducer=reduce;
    partition_number=num_reducers; //Number of partitions
    fileNumber=argc-1;
    p=malloc(sizeof(struct hashTable*) * num_reducers);

    //Create Partitions
    for(int i=0;i<partition_number;i++){
        struct hashTable* table = makeTable(TABLE_SIZE);
        p[i] = table;
    }

    //Start Mapping Process
    pthread_t mappers[num_mappers];
    for(int i=0;i<num_mappers || i==argc-1; i++){
        pthread_create(&mappers[i], NULL, findFile, (void*) argv);
    }
    //Join the Mappers
    for(int i=0; i<num_mappers || i==argc-1; i++)//Start joining all the threads
    {
        pthread_join(mappers[i], NULL);
    }

    //Start Reducing Process
    pthread_t reducer[num_reducers];
    reducenode=malloc(sizeof(struct node *) * num_reducers);
    for(int i=0;i<num_reducers;i++){
        pthread_create(&reducer[i],NULL,callReduce,NULL);
    }

    //Join the Reducers
    for(int i=0;i<num_reducers;i++){
        pthread_join(reducer[i],NULL);
    }

    /*
    //Memory clean-up
    for(int i=0;i<partition_number;i++)
    {
        freeTable(p[i]);
    }


    free(partitionlock);
    free(reducenode);
    free(p);
     */
}
