//
// Created by yangguo on 11/14/22.
//
#include "groundhog/traj_processing.h"
#include <stdlib.h>
#include "groundhog/traj_block_format.h"
#include "groundhog/normalization_util.h"
#include <stdio.h>

static int compare_pointed_to_data(const void *a, const void *b) {
    struct traj_point *ptr_a = *((struct traj_point**)a);
    struct traj_point *ptr_b = *((struct traj_point**)b);

    /*if (ptr_a->timestamp_sec < ptr_b->timestamp_sec) {
        return -1;
    } else if (ptr_a->timestamp_sec > ptr_b->timestamp_sec) {
        return 1;
    } else {
        return 0;
    }*/

    if (ptr_a->oid < ptr_b->oid) {
        return -1;
    } else if (ptr_a->oid > ptr_b->oid) {
        return 1;
    } else {
        return (ptr_a->timestamp_sec < ptr_b->timestamp_sec) - (ptr_b->timestamp_sec < ptr_a->timestamp_sec);
    }

}

/**
 * sort struct traj_point **  according to zcurve value
 * @param a
 * @param b
 * @return
 */
static int cmp_zcurve(const void *a, const void *b) {
    struct traj_point *point_a = *(struct traj_point **)a;
    struct traj_point *point_b = *(struct traj_point **)b;
    long zcurve_a = generate_zcurve_value(point_a->normalized_longitude, point_a->normalized_latitude, point_a->timestamp_sec);
    long zcurve_b = generate_zcurve_value(point_b->normalized_longitude, point_b->normalized_latitude, point_b->timestamp_sec);
    return zcurve_a > zcurve_b ? 1 : -1;
}

static int cmp_longitude(const void *a, const void *b) {
    struct traj_point *point_a = *(struct traj_point **)a;
    struct traj_point *point_b = *(struct traj_point **)b;
    return point_a->normalized_longitude > point_b->normalized_longitude ? 1 : -1;
}

static int cmp_latitude(const void *a, const void *b) {
    struct traj_point *point_a = *(struct traj_point **)a;
    struct traj_point *point_b = *(struct traj_point **)b;
    return point_a->normalized_latitude > point_b->normalized_latitude ? 1 : -1;
}

static int cmp_timestamp(const void *a, const void *b) {
    struct traj_point *point_a = *(struct traj_point **)a;
    struct traj_point *point_b = *(struct traj_point **)b;
    return point_a->timestamp_sec > point_b->timestamp_sec ? 1 : -1;
}

static int cmp_zcurve_spatial(const void *a, const void *b) {
    struct traj_point *point_a = *(struct traj_point **)a;
    struct traj_point *point_b = *(struct traj_point **)b;
    long zcurve_a = generate_zcurve_value_spatial(point_a->normalized_longitude, point_a->normalized_latitude);
    long zcurve_b = generate_zcurve_value_spatial(point_b->normalized_longitude, point_b->normalized_latitude);
    return zcurve_a > zcurve_b ? 1 : -1;
}


static int cmp_zcurve_time_preferred(const void *a, const void *b) {
    struct traj_point *point_a = *(struct traj_point **)a;
    struct traj_point *point_b = *(struct traj_point **)b;
    int time_partition_a = point_a->timestamp_sec / (60 * 5); // partition length is 5 min
    int time_partition_b = point_b->timestamp_sec / (60 * 5);
    long zcurve_a = generate_zcurve_value_spatial(point_a->normalized_longitude, point_a->normalized_latitude);
    long zcurve_b = generate_zcurve_value_spatial(point_b->normalized_longitude, point_b->normalized_latitude);
    if (time_partition_a > time_partition_b) {
        return 1;
    } else if (time_partition_a < time_partition_b) {
        return -1;
    } else {
        return (zcurve_a > zcurve_b) - (zcurve_b > zcurve_a);
    }

}

static int cmp_zcurve_space_preferred(const void *a, const void *b) {
    struct traj_point *point_a = *(struct traj_point **)a;
    struct traj_point *point_b = *(struct traj_point **)b;

    long space_partition_a = generate_zcurve_value_spatial(point_a->normalized_longitude, point_a->normalized_latitude) >> 11;
    long space_partition_b = generate_zcurve_value_spatial(point_b->normalized_longitude, point_b->normalized_latitude) >> 11;

    if (space_partition_a > space_partition_b) {
        return 1;
    } else if (space_partition_a < space_partition_b) {
        return -1;
    } else {
        return (point_a->timestamp_sec > point_b->timestamp_sec) - (point_b->timestamp_sec > point_a->timestamp_sec);
    }

}

static int cmp_zcurve_no_preferred(const void *a, const void *b) {
    struct traj_point *point_a = *(struct traj_point **)a;
    struct traj_point *point_b = *(struct traj_point **)b;

    long zcurve_a = generate_zcurve_value(point_a->normalized_longitude, point_a->normalized_latitude, point_a->timestamp_sec / 5);
    long zcurve_b = generate_zcurve_value(point_b->normalized_longitude, point_b->normalized_latitude, point_b->timestamp_sec / 5);

    if (zcurve_a > zcurve_b) {
        return 1;
    } else {
        return -1;
    }

}


void sort_traj_points(struct traj_point **points, int array_size) {
    qsort(points, array_size, sizeof(struct traj_point*), compare_pointed_to_data);
}

void sort_traj_points_longitude(struct traj_point **points, int array_size) {
    qsort(points, array_size, sizeof(struct traj_point*), cmp_longitude);
}

void sort_traj_points_latitude(struct traj_point **points, int array_size) {
    qsort(points, array_size, sizeof(struct traj_point*), cmp_latitude);
}

void sort_traj_points_timestamp(struct traj_point **points, int array_size) {
    qsort(points, array_size, sizeof(struct traj_point*), cmp_timestamp);
}

void sort_traj_points_zcurve_st(struct traj_point **points, int array_size) {
    qsort(points, array_size, sizeof(struct traj_point*), cmp_zcurve);
}

void sort_traj_points_zcurve(struct traj_point **points, int array_size) {
    qsort(points, array_size, sizeof(struct traj_point*), cmp_zcurve_spatial);
}

void sort_traj_points_zcurve_with_option(struct traj_point **points, int array_size, int option) {
    if (option == 1) {
        qsort(points, array_size, sizeof(struct traj_point *), cmp_zcurve_time_preferred);
    } else if (option == 2) {
        qsort(points, array_size, sizeof(struct traj_point *), cmp_zcurve_space_preferred);
    } else {
        qsort(points, array_size, sizeof(struct traj_point *), cmp_zcurve_no_preferred);
    }
}

int split_traj_points_via_point_num(struct traj_point **points, int points_num, int segment_size, struct seg_meta_pair_itr *pair_array, int array_size) {

    int segment_num;
    if (points_num % segment_size != 0) {
        segment_num = (points_num / segment_size) + 1;
    } else {
        segment_num = points_num / segment_size;
    }

    if (segment_num > array_size) {
        return -1;
    }

    for (int i = 0; i < segment_num; i++) {
        struct traj_point **segment_base = points + i * segment_size;
        struct seg_meta_pair *pair = &(pair_array->meta_pair_array_base)[i];
        struct seg_meta *meta = &((pair_array->meta_pair_array_base)[i].meta);
        pair_array->current_index = i;
        init_seg_meta(meta);

        int segment_point_num = segment_size;
        if (i == segment_num - 1) {
            segment_point_num = points_num - segment_size * (segment_num - 1);
        }
        extract_spatiotemporal_seg_meta(segment_base, segment_point_num, meta);

        int buffer_size = segment_point_num * get_traj_point_size();
        void* buffer = malloc(buffer_size);
        convert_struct_to_serialized_traj(segment_base, buffer, segment_point_num);
        pair->seg_data = buffer;
        pair->seg_length = buffer_size;
    }

    return 0;
}

int split_traj_points_via_point_num_and_seg_num(struct traj_point **points, int points_num, struct seg_meta_pair_itr *pair_array, int segment_num) {
    int segment_size = points_num / segment_num;

    for (int i = 0; i < segment_num; i++) {
        struct traj_point **segment_base = points + i * segment_size;
        struct seg_meta_pair *pair = &(pair_array->meta_pair_array_base)[i];
        struct seg_meta *meta = &((pair_array->meta_pair_array_base)[i].meta);
        pair_array->current_index = i;
        init_seg_meta(meta);

        int segment_point_num = segment_size;
        if (i == segment_num - 1) {
            segment_point_num = points_num - segment_size * (segment_num - 1);
        }
        extract_spatiotemporal_seg_meta(segment_base, segment_point_num, meta);

        int buffer_size = segment_point_num * get_traj_point_size();
        void* buffer = malloc(buffer_size);
        convert_struct_to_serialized_traj(segment_base, buffer, segment_point_num);
        pair->seg_data = buffer;
        pair->seg_length = buffer_size;
    }

}

void print_seg_meta(struct seg_meta *meta) {
    printf("lon_min: %d, lon_max: %d, lat_min: %d, lat_max: %d, time_min: %d, time_max: %d, seg_offset: %d, seg_size: %d\n",
           meta->lon_min, meta->lon_max, meta->lat_min, meta->lat_max, meta->time_min, meta->time_max, meta->seg_offset, meta->seg_size);
}

void print_seg_meta_pair(struct seg_meta_pair *meta_pair) {
    printf("seg_meta_section: ");
    print_seg_meta(&(meta_pair->meta));
    printf("seg_data ptr: %p\n", meta_pair->seg_data);
    printf("seg_length: %d\n", meta_pair->seg_length);
}

void print_seg_meta_pair_itr(struct seg_meta_pair_itr *meta_pair_itr) {
    for (int i = 0; i <= meta_pair_itr->current_index; i++) {
        struct seg_meta_pair *meta_pair = &(meta_pair_itr->meta_pair_array_base[i]);
        print_seg_meta_pair(meta_pair);
        printf("\n");
    }
    printf("total_size: %d\n", meta_pair_itr->array_size);
    printf("current index: %d\n\n", meta_pair_itr->current_index);
}
