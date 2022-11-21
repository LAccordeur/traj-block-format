//
// Created by yangguo on 11/14/22.
//

#include "gtest/gtest.h"

extern "C" {
#include "traj_processing.h"
#include "porto_dataset_reader.h"
}

TEST(traj_processing, sort) {

    struct traj_point **points = allocate_points_memory(4);
    FILE *fp = fopen("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v2.csv", "r");

    read_points_from_csv(fp, points, 0, 4);
    print_traj_points(points, 4);
    sort_traj_points(points, 4);
    printf("\n\n");
    print_traj_points(points, 4);

}

TEST(traj_processing, split) {
    int points_num = 4;
    struct traj_point **points = allocate_points_memory(points_num);
    FILE *fp = fopen("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v2.csv", "r");

    read_points_from_csv(fp, points, 0, points_num);
    struct seg_meta_pair_itr meta_pair_array;
    struct seg_meta_pair meta_pairs[2];
    init_seg_meta_pair_array(&meta_pair_array, meta_pairs, 2);

    split_traj_points_via_point_num(points, points_num, 2, &meta_pair_array, 2);
    print_seg_meta_pair_itr(&meta_pair_array);
}
