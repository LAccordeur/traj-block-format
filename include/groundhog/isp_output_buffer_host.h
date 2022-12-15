//
// Created by yangguo on 12/15/22.
//

#ifndef TRAJ_BLOCK_FORMAT_ISP_OUTPUT_BUFFER_HOST_H
#define TRAJ_BLOCK_FORMAT_ISP_OUTPUT_BUFFER_HOST_H

int parse_points_num_from_output_buffer_page(void *data_page);

void deserialize_output_buffer_page(void *data_page, struct traj_point **points, int points_num);

#endif //TRAJ_BLOCK_FORMAT_ISP_OUTPUT_BUFFER_HOST_H
