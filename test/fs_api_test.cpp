//
// Created by yangguo on 11/30/22.
//


#include "gtest/gtest.h"

TEST(fstest, write) {
    char str[] = "helloworld1";
    char str1[] = "fdasfadsf2";
    FILE *fp = fopen("test.txt", "w");
    fwrite(str, strlen(str) + 1, 1, fp);
    fwrite(str1, strlen(str1) + 1, 1, fp);
}

TEST(fstest, read) {
    char str[] = "helloworld1";
    char str1[] = "fdasfadsf2";
    char buffer[20];
    char buffer1[20];
    FILE *fp = fopen("test.txt", "r");
    size_t result_size = fread(buffer, 1, strlen(str) + 1, fp);
    fread(buffer1, strlen(str1) + 1, 1, fp);
    printf("result size: %d\n", result_size);
    printf("buffer: %s\n", buffer);
    printf("buffer1: %s\n", buffer1);
}