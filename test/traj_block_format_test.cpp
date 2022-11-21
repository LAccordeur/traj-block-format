//
// Created by yangguo on 11/14/22.
//
#include "gtest/gtest.h"

extern "C" {
#include "traj_block_format.h"
#include "log.h"
#include "porto_dataset_reader.h"
}

TEST(formattest, create) {
    int points_num = 4;
    struct traj_point **points = allocate_points_memory(points_num);
    FILE *fp = fopen("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v2.csv", "r");

    read_points_from_csv(fp, points, 0, points_num);
    print_traj_points(points, points_num);
    char block[4096];
    do_self_contained_traj_block(points, points_num, block, 4096);

    struct traj_block_header header;
    parse_traj_block_for_header(block, &header);
    printf("seg count: %d\n", header.seg_count);

    struct seg_meta meta_arr[2];
    parse_traj_block_for_seg_meta_section(block, meta_arr, 2);
    print_seg_meta(meta_arr);
    print_seg_meta(meta_arr+1);

    struct traj_point **deserialized = allocate_points_memory(2);
    parse_traj_block_for_seg_data(block, 100, deserialized, 2);
    print_traj_points(deserialized, 2);

}

