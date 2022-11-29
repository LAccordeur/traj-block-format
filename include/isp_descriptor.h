//
// Created by yangguo on 11/28/22.
//

#ifndef TRAJ_BLOCK_FORMAT_ISP_DESCRIPTOR_H
#define TRAJ_BLOCK_FORMAT_ISP_DESCRIPTOR_H

struct isp_descriptor {
    int isp_type;  // 0 means id temporal query; 1 means spatio-temporal range query
    int oid;
    int time_min;
    int time_max;
    int lon_min;
    int lon_max;
    int lat_min;
    int lat_max;
    int dst_block_address;
    int dst_block_size;
    int src_block_addresses_count;
    int *src_block_addresses;
};

int calculate_isp_descriptor_space(struct isp_descriptor *descriptor);

void serialize_isp_descriptor(struct isp_descriptor *source, void *destination);

void deserialize_isp_descriptor(void *source, struct isp_descriptor *destination);

void free_isp_descriptor(struct isp_descriptor *descriptor);

#endif //TRAJ_BLOCK_FORMAT_ISP_DESCRIPTOR_H
