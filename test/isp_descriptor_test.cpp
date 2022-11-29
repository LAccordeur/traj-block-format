//
// Created by yangguo on 11/29/22.
//

#include "gtest/gtest.h"

extern "C" {
#include "isp_descriptor.h"
}

static void print_isp_descriptor(struct isp_descriptor *descriptor) {
    printf("isp_type: %d\n", descriptor->isp_type);
    printf("oid: %d\n", descriptor->oid);
    printf("time_min: %d\n", descriptor->time_min);
    printf("time_max: %d\n", descriptor->time_max);
    printf("lon_min: %d\n", descriptor->lon_min);
    printf("lon_max: %d\n", descriptor->lon_max);
    printf("lat_min: %d\n", descriptor->lat_min);
    printf("lat_max: %d\n", descriptor->lat_max);
    printf("dst_block_addresses: %d\n", descriptor->dst_block_address);
    printf("dst_block_size: %d\n", descriptor->dst_block_size);
    printf("src_block_addresses_count: %d\n", descriptor->src_block_addresses_count);
    printf("src_block_addresses: %p\n\n", descriptor->src_block_addresses);
}

TEST(isp_descriptor, test) {
    struct isp_descriptor descriptor;
    descriptor.isp_type = 0;
    descriptor.oid = 1233;
    descriptor.time_min = 1;
    descriptor.time_max = 3;
    descriptor.lon_min = 12;
    descriptor.lon_max = 13;
    descriptor.lat_min = 21;
    descriptor.lat_max = 34;
    descriptor.dst_block_address = 111;
    descriptor.dst_block_size = 343;
    descriptor.src_block_addresses_count = 99;
    int *array = (int*)malloc(2 * sizeof(int));
    array[0] = 22;
    array[1] = 111;
    descriptor.src_block_addresses = array;

    int descriptor_space = calculate_isp_descriptor_space(&descriptor);
    print_isp_descriptor(&descriptor);
    char block[descriptor_space];
    serialize_isp_descriptor(&descriptor, block);

    struct isp_descriptor result;
    deserialize_isp_descriptor(block, &result);
    print_isp_descriptor(&result);
    free_isp_descriptor(&descriptor);
    free_isp_descriptor(&result);
}

