//
// Created by yangguo on 11/29/22.
//

#include "gtest/gtest.h"

extern "C" {
#include "groundhog/isp_descriptor.h"
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
    descriptor.lba_count = 3;
    struct lba lbas[3];
    lbas[0].lba_start = 1;
    lbas[0].lba_num = 1;
    lbas[1].lba_start = 5;
    lbas[1].lba_num = 3;
    lbas[2].lba_start = 9;
    lbas[2].lba_num = 2;
    descriptor.lba_array = lbas;

    int descriptor_space = calculate_isp_descriptor_space(&descriptor);
    print_isp_descriptor(&descriptor);
    char block[descriptor_space];
    serialize_isp_descriptor(&descriptor, block);

    struct isp_descriptor result;
    deserialize_isp_descriptor(block, &result);
    print_isp_descriptor(&result);

    free_isp_descriptor(&result);
}

