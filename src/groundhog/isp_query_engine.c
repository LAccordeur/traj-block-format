//
// Created by yangguo on 12/14/22.
//

#include "groundhog/isp_query_engine.h"

static void id_temporal_query_page(void *data_block, struct isp_output_buffer *output_buffer, struct isp_descriptor *descriptor) {
    // parse data block
    struct traj_block_header block_header;
    parse_traj_block_for_header(data_block, &block_header);
    struct seg_meta meta_array[block_header.seg_count];
    parse_traj_block_for_seg_meta_section(data_block, meta_array, block_header.seg_count);
    for (int j = 0; j < block_header.seg_count; j++) {
        struct seg_meta meta_item = meta_array[j];
        if (descriptor->time_min <= meta_item.time_max && descriptor->time_max >= meta_item.time_min) {
            int data_seg_points_num = meta_item.seg_size / get_traj_point_size();
            char *seg_data_base = (char*)data_block + meta_item.seg_offset;
            struct traj_point seg_point;
            for (int i = 0; i < data_seg_points_num; i++) {
                void* source = seg_data_base + get_traj_point_size() * i;
                deserialize_traj_point(source, &seg_point);
                if (seg_point.oid == descriptor->oid
                    && descriptor->time_min <= seg_point.timestamp_sec
                    && descriptor->time_max >= seg_point.timestamp_sec) {
                    put_point_data_to_isp_output_buffer(output_buffer, source, get_traj_point_size());
                }
            }
        }
    }
}

static void spatio_temporal_query_page(void *data_block, struct isp_output_buffer *output_buffer, struct isp_descriptor *descriptor) {
    // parse data block
    struct traj_block_header block_header;
    parse_traj_block_for_header(data_block, &block_header);
    struct seg_meta meta_array[block_header.seg_count];
    parse_traj_block_for_seg_meta_section(data_block, meta_array, block_header.seg_count);
    for (int j = 0; j < block_header.seg_count; j++) {
        struct seg_meta meta_item = meta_array[j];
        if (descriptor->time_min <= meta_item.time_max && descriptor->time_max >= meta_item.time_min) {
            int data_seg_points_num = meta_item.seg_size / get_traj_point_size();
            char *seg_data_base = (char*)data_block + meta_item.seg_offset;
            struct traj_point seg_point;
            for (int i = 0; i < data_seg_points_num; i++) {
                void* source = seg_data_base + get_traj_point_size() * i;
                deserialize_traj_point(source, &seg_point);
                if (descriptor->lon_min <= seg_point.normalized_longitude
                    && descriptor->lon_max >= seg_point.normalized_longitude
                    && descriptor->lat_min <= seg_point.normalized_latitude
                    && descriptor->lat_max >= seg_point.normalized_latitude
                    && descriptor->time_min <= seg_point.timestamp_sec
                    && descriptor->time_max >= seg_point.timestamp_sec) {
                    put_point_data_to_isp_output_buffer(output_buffer, source, get_traj_point_size());
                }
            }
        }
    }
}

void run_id_temporal_query(struct iovec* iov, size_t nr_segs, struct isp_output_buffer *output_buffer, struct isp_descriptor *descriptor) {
    for (int i = 0; i < nr_segs; i++) {
        struct iovec iov_item = iov[i];
        for (int offset = 0; offset < iov_item.iov_len; offset += ISP_BUFFER_PAGE_SIZE) {
            id_temporal_query_page(iov_item.iov_base + offset, output_buffer, descriptor);
        }
    }
}

void run_spatio_temporal_range_query(struct iovec* iov, size_t nr_segs, struct isp_output_buffer *output_buffer, struct isp_descriptor *descriptor) {
    for (int i = 0; i < nr_segs; i++) {
        struct iovec iov_item = iov[i];
        for (int offset = 0; offset < iov_item.iov_len; offset += ISP_BUFFER_PAGE_SIZE) {
            spatio_temporal_query_page(iov_item.iov_base + offset, output_buffer, descriptor);
        }
    }
}
