//
// Created by yangguo on 12/14/22.
//

#include "groundhog/isp_output_buffer.h"
#include "groundhog/traj_block_format.h"
#include "const.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

void allocate_isp_output_buffer(struct isp_output_buffer *output_buffer, struct iovec *iov, size_t capacity) {
    output_buffer->iov_capacity = capacity;
    output_buffer->iov = iov;

    for (size_t i = 0; i < capacity; i++) {
        //output_buffer->iov[i].iov_base = alloc_pages(1, ZONE_PS_DDR_LOW);
        output_buffer->iov[i].iov_base = malloc(ISP_BUFFER_PAGE_SIZE);
        output_buffer->iov[i].iov_len = ISP_BUFFER_PAGE_SIZE;
    }
    output_buffer->current_iov_index = 0;
    output_buffer->current_iov_offset = 4;  // the first byte are used to record the count of tuples in the page
    output_buffer->current_tuple_count = 0;
    output_buffer->used_bytes_count = 4;
    // xil_printf("[ISP] allocate %d page buffer for pushdown output\n", capacity);
}

void free_isp_output_buffer(struct isp_output_buffer *output_buffer) {
    for (size_t i = 0; i < output_buffer->iov_capacity; i++) {
        //free_mem(output_buffer->iov[i].iov_base, ISP_BUFFER_PAGE_SIZE);
        free(output_buffer->iov[i].iov_base);
    }
    // xil_printf("[ISP] free %d page buffer for pushdown output\n", output_buffer->iov_capacity);
}

size_t put_point_data_to_isp_output_buffer(struct isp_output_buffer *output_buffer, const void *data, size_t data_bytes_count) {
    if ((output_buffer->used_bytes_count >= output_buffer->iov_capacity * ISP_BUFFER_PAGE_SIZE)
        || (output_buffer->used_bytes_count + data_bytes_count >= output_buffer->iov_capacity * ISP_BUFFER_PAGE_SIZE)) {
        return 0;
    }

    if (output_buffer->current_iov_offset + data_bytes_count > ISP_BUFFER_PAGE_SIZE) {
        // this buffer page cannot hold the full data, we first update the count and then skip to the next page
        memcpy(output_buffer->iov[output_buffer->current_iov_index].iov_base, &output_buffer->current_tuple_count, 4);
        if (output_buffer->current_iov_index + 1 >= output_buffer->iov_capacity) {
            return 0;
        }
        output_buffer->current_iov_index = output_buffer->current_iov_index + 1;
        output_buffer->current_iov_offset = 4;
        output_buffer->current_tuple_count = 0;
        output_buffer->used_bytes_count = ISP_BUFFER_PAGE_SIZE * output_buffer->current_iov_index + output_buffer->current_iov_offset;
        printf("use new page\n");
        print_isp_output_buffer_info(output_buffer);
    }

    memcpy(output_buffer->iov[output_buffer->current_iov_index].iov_base + output_buffer->current_iov_offset, data, data_bytes_count);
    output_buffer->current_iov_offset = output_buffer->current_iov_offset + data_bytes_count;
    output_buffer->current_tuple_count += (data_bytes_count / get_traj_point_size());
    output_buffer->used_bytes_count += data_bytes_count;

    return data_bytes_count;
}

void format_the_last_isp_buffer_page(struct isp_output_buffer *output_buffer) {
    memcpy(output_buffer->iov[output_buffer->current_iov_index].iov_base, &output_buffer->current_tuple_count, 4);
}

int parse_points_num_from_output_buffer_page(void *data_page) {
    int count = 0;
    memcpy(&count, data_page, 4);
    return count;
}

void deserialize_output_buffer_page(void *data_page, struct traj_point **points, int points_num) {
    int offset_base = 4;
    int point_data_size = get_traj_point_size();
    for (int i = 0; i < points_num; i++) {

        deserialize_traj_point(data_page + offset_base + i * point_data_size, points[i]);
    }
}

void print_isp_output_buffer_info(struct isp_output_buffer *output_buffer) {
    printf("iov_capacity: %d\n", output_buffer->iov_capacity);
    printf("current_iov_index: %d\n", output_buffer->current_iov_index);
    printf("current_iov_offset: %d\n", output_buffer->current_iov_offset);
    printf("current_tuple_count: %d\n", output_buffer->current_tuple_count);
    printf("used_bytes_count: %d\n", output_buffer->used_bytes_count);
}

