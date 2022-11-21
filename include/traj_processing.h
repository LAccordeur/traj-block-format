//
// Created by yangguo on 11/11/22.
//

#ifndef TRAJ_BLOCK_FORMAT_TRAJ_PROCESSING_H
#define TRAJ_BLOCK_FORMAT_TRAJ_PROCESSING_H
#include "traj_block_format.h"

void sort_traj_points(struct traj_point **points, int array_size);

int split_traj_points_via_point_num(struct traj_point **points, int points_num, int segment_size, struct seg_meta_pair_itr *pair_array, int array_size);

#endif //TRAJ_BLOCK_FORMAT_TRAJ_PROCESSING_H
