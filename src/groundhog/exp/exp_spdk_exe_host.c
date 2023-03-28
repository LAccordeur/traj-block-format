//
// Created by yangguo on 23-2-10.
//
//
// Created by yangguo on 22-12-13.
//
/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 */

#include "spdk/stdinc.h"

#include "spdk/nvme.h"
#include "spdk/vmd.h"
#include "spdk/nvme_zns.h"
#include "spdk/env.h"
#include "spdk/string.h"
#include "spdk/log.h"
#include "groundhog/traj_block_format.h"
#include "groundhog/isp_descriptor.h"
#include "groundhog/simple_query_engine.h"
#include "groundhog/query_workload_reader.h"

/*struct ctrlr_entry {
    struct spdk_nvme_ctrlr		*ctrlr;
    TAILQ_ENTRY(ctrlr_entry)	link;
    char				name[1024];
};

struct ns_entry {
    struct spdk_nvme_ctrlr	*ctrlr;
    struct spdk_nvme_ns	*ns;
    TAILQ_ENTRY(ns_entry)	link;
    struct spdk_nvme_qpair	*qpair;
};*/

static TAILQ_HEAD(, ctrlr_entry) g_controllers = TAILQ_HEAD_INITIALIZER(g_controllers);
static TAILQ_HEAD(, ns_entry) g_namespaces = TAILQ_HEAD_INITIALIZER(g_namespaces);
static struct spdk_nvme_transport_id g_trid = {};

static bool g_vmd = false;

static void
register_ns(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_ns *ns)
{
    struct ns_entry *entry;

    if (!spdk_nvme_ns_is_active(ns)) {
        return;
    }

    entry = malloc(sizeof(struct ns_entry));
    if (entry == NULL) {
        perror("ns_entry malloc");
        exit(1);
    }

    entry->ctrlr = ctrlr;
    entry->ns = ns;
    TAILQ_INSERT_TAIL(&g_namespaces, entry, link);

    printf("  Namespace ID: %d size: %juGB\n", spdk_nvme_ns_get_id(ns),
           spdk_nvme_ns_get_size(ns) / 1000000000);
}

struct hello_world_sequence {
    struct ns_entry	*ns_entry;
    char		*buf;
    unsigned        using_cmb_io;
    int         block_index;
    int         total_block_num;
    int		is_completed;
    int points_num;
    bool is_id_temporal_query;
    struct id_temporal_predicate id_temporal;
    struct spatio_temporal_range_predicate spatio_temporal;
};

static struct traj_point **points_buffer;
static int points_buffer_size = 100;
static void fill_points_buffer(int points_num) {
    points_buffer = allocate_points_memory(points_num);
}

static void free_points_buffer(int points_num) {
    free_points_memory(points_buffer, points_num);
}

static int
id_temporal_raw_traj_block_without_meta_filtering(struct hello_world_sequence *sequence) {
    printf("oid: %d\n", sequence->id_temporal.oid);

    struct id_temporal_predicate *predicate = &sequence->id_temporal;
    char* block = sequence->buf;
    int result_count = 0;

    struct traj_point **points = points_buffer;
    for (int block_count = 0; block_count < sequence->total_block_num; block_count++, block+=4096) {

        struct traj_block_header header;
        parse_traj_block_for_header(block, &header);
        //printf("seg count in each block: %d\n", header.seg_count);
        struct seg_meta meta_array[header.seg_count];
        parse_traj_block_for_seg_meta_section(block, meta_array, header.seg_count);
        for (int j = 0; j < header.seg_count; j++) {
            struct seg_meta meta_item = meta_array[j];

            int data_seg_points_num = meta_item.seg_size / get_traj_point_size();

            //struct traj_point **points = allocate_points_memory(data_seg_points_num);
            parse_traj_block_for_seg_data(block, meta_item.seg_offset, points, data_seg_points_num);

            for (int k = 0; k < data_seg_points_num; k++) {
                struct traj_point *point = points[k];
                if (point->oid == predicate->oid
                    && predicate->time_min <= point->timestamp_sec
                    && predicate->time_max >= point->timestamp_sec) {
                    result_count++;
                    /*printf("oid: %d\n", point->oid);
                    printf("timestamp_sec: %d\n", point->timestamp_sec);
                    printf("normalized longitude: %d\n", point->normalized_longitude);
                    printf("normalized latitude: %d\n\n", point->normalized_latitude);*/
                }
            }

            //print_traj_points(points, data_seg_points_num);
            //free_points_memory(points, data_seg_points_num);

        }
    }
    printf("result count: %d\n", result_count);
    return result_count;
}

static int
id_temporal_raw_traj_block_with_meta_filtering(struct hello_world_sequence *sequence) {
    printf("oid: %d\n", sequence->id_temporal.oid);

    struct id_temporal_predicate *predicate = &sequence->id_temporal;
    char* block = sequence->buf;
    int result_count = 0;

    struct traj_point **points = points_buffer;
    for (int block_count = 0; block_count < sequence->total_block_num; block_count++, block+=4096) {

        struct traj_block_header header;
        parse_traj_block_for_header(block, &header);
        //printf("seg count in each block: %d\n", header.seg_count);
        struct seg_meta meta_array[header.seg_count];
        parse_traj_block_for_seg_meta_section(block, meta_array, header.seg_count);
        for (int j = 0; j < header.seg_count; j++) {
            struct seg_meta meta_item = meta_array[j];
            //xil_printf("[ISP] seg_meta index: %d, seg offset: %d, seg size: %d, time_min: %d, time_max: %d\n", j, meta_item.seg_offset, meta_item.seg_size, meta_item.time_min, meta_item.time_max);
            if (predicate->time_min <= meta_item.time_max && predicate->time_max >= meta_item.time_min) {
                int data_seg_points_num = meta_item.seg_size / get_traj_point_size();
                char *seg_data_base = (char*)block + meta_item.seg_offset;
                struct traj_point seg_point;
                for (int i = 0; i < data_seg_points_num; i++) {
                    void* source = seg_data_base + get_traj_point_size() * i;
                    //xil_printf("point src ptr: %p\n", source);
                    deserialize_traj_point(source, &seg_point);
                    //xil_printf("[ISP] oid: %d, timestamp_sec: %d, lon: %d, lat: %d\n", seg_point.oid, seg_point.timestamp_sec, seg_point.normalized_longitude, seg_point.normalized_latitude);
                    if (seg_point.oid == predicate->oid
                        && predicate->time_min <= seg_point.timestamp_sec
                        && predicate->time_max >= seg_point.timestamp_sec) {
                        //xil_printf("[ISP] oid: %d, timestamp_sec: %d, lon: %d, lat: %d\n", seg_point.oid, seg_point.timestamp_sec, seg_point.normalized_longitude, seg_point.normalized_latitude);
                        result_count++;
                    }
                }
            }

        }
    }
    printf("result count: %d\n", result_count);
    return result_count;
}

static int
spatio_temporal_raw_traj_block_without_meta_filtering(struct hello_world_sequence *sequence) {
    printf("spatio-temporal query\n");

    struct spatio_temporal_range_predicate *predicate = &sequence->spatio_temporal;
    char* block = sequence->buf;
    int result_count = 0;

    //struct traj_point **points = allocate_points_memory(50);
    struct traj_point **points = points_buffer;
    for (int block_count = 0; block_count < sequence->total_block_num; block_count++, block+=4096) {

        struct traj_block_header header;
        parse_traj_block_for_header(block, &header);
        //printf("seg count in each block: %d\n", header.seg_count);
        struct seg_meta meta_array[header.seg_count];
        parse_traj_block_for_seg_meta_section(block, meta_array, header.seg_count);
        for (int j = 0; j < header.seg_count; j++) {
            struct seg_meta meta_item = meta_array[j];

            int data_seg_points_num = meta_item.seg_size / get_traj_point_size();


            parse_traj_block_for_seg_data(block, meta_item.seg_offset, points, data_seg_points_num);

            for (int k = 0; k < data_seg_points_num; k++) {
                struct traj_point *point = points[k];
                if (predicate->lon_min <= point->normalized_longitude
                    && predicate->lon_max >= point->normalized_longitude
                    && predicate->lat_min <= point->normalized_latitude
                    && predicate->lat_max >= point->normalized_latitude
                    && predicate->time_min <= point->timestamp_sec
                    && predicate->time_max >= point->timestamp_sec) {
                    result_count++;
//                    printf("oid: %d\n", point->oid);
//                    printf("timestamp_sec: %d\n", point->timestamp_sec);
//                    printf("normalized longitude: %d\n", point->normalized_longitude);
//                    printf("normalized latitude: %d\n\n", point->normalized_latitude);
                }
            }
            /*struct seg_meta meta_item = meta_array[j];
            if (predicate->time_min <= meta_item.time_max && predicate->time_max >= meta_item.time_min
                && predicate->lon_min <= meta_item.lon_max && predicate->lon_max >= meta_item.lon_min
                && predicate->lat_min <= meta_item.lat_max && predicate->lat_max >= meta_item.lat_min) {
                int data_seg_points_num = meta_item.seg_size / get_traj_point_size();
                char *seg_data_base = (char*)block + meta_item.seg_offset;
                struct traj_point seg_point;
                for (int i = 0; i < data_seg_points_num; i++) {
                    void* source = seg_data_base + get_traj_point_size() * i;
                    deserialize_traj_point(source, &seg_point);
                    if (predicate->lon_min <= seg_point.normalized_longitude
                        && predicate->lon_max >= seg_point.normalized_longitude
                        && predicate->lat_min <= seg_point.normalized_latitude
                        && predicate->lat_max >= seg_point.normalized_latitude
                        && predicate->time_min <= seg_point.timestamp_sec
                        && predicate->time_max >= seg_point.timestamp_sec) {
                        result_count++;
                    }
                }
            }*/

            //print_traj_points(points, data_seg_points_num);


        }
    }
    //free_points_memory(points, 50);
    printf("result count: %d\n", result_count);
    return result_count;
}

static int
spatio_temporal_raw_traj_block_with_meta_filtering(struct hello_world_sequence *sequence) {
    printf("spatio-temporal query\n");

    struct spatio_temporal_range_predicate *predicate = &sequence->spatio_temporal;
    char* block = sequence->buf;
    int result_count = 0;

    //struct traj_point **points = allocate_points_memory(50);
    struct traj_point **points = points_buffer;
    for (int block_count = 0; block_count < sequence->total_block_num; block_count++, block+=4096) {

        struct traj_block_header header;
        parse_traj_block_for_header(block, &header);
        //printf("seg count in each block: %d\n", header.seg_count);
        struct seg_meta meta_array[header.seg_count];
        parse_traj_block_for_seg_meta_section(block, meta_array, header.seg_count);
        for (int j = 0; j < header.seg_count; j++) {
/*            struct seg_meta meta_item = meta_array[j];

            int data_seg_points_num = meta_item.seg_size / get_traj_point_size();


            parse_traj_block_for_seg_data(block, meta_item.seg_offset, points, data_seg_points_num);

            for (int k = 0; k < data_seg_points_num; k++) {
                struct traj_point *point = points[k];
                if (predicate->lon_min <= point->normalized_longitude
                    && predicate->lon_max >= point->normalized_longitude
                    && predicate->lat_min <= point->normalized_latitude
                    && predicate->lat_max >= point->normalized_latitude
                    && predicate->time_min <= point->timestamp_sec
                    && predicate->time_max >= point->timestamp_sec) {
                    result_count++;
//                    printf("oid: %d\n", point->oid);
//                    printf("timestamp_sec: %d\n", point->timestamp_sec);
//                    printf("normalized longitude: %d\n", point->normalized_longitude);
//                    printf("normalized latitude: %d\n\n", point->normalized_latitude);
                }
            }*/
            struct seg_meta meta_item = meta_array[j];
            if (predicate->time_min <= meta_item.time_max && predicate->time_max >= meta_item.time_min
                && predicate->lon_min <= meta_item.lon_max && predicate->lon_max >= meta_item.lon_min
                && predicate->lat_min <= meta_item.lat_max && predicate->lat_max >= meta_item.lat_min) {
                int data_seg_points_num = meta_item.seg_size / get_traj_point_size();
                char *seg_data_base = (char*)block + meta_item.seg_offset;
                struct traj_point seg_point;
                for (int i = 0; i < data_seg_points_num; i++) {
                    void* source = seg_data_base + get_traj_point_size() * i;
                    deserialize_traj_point(source, &seg_point);
                    if (predicate->lon_min <= seg_point.normalized_longitude
                        && predicate->lon_max >= seg_point.normalized_longitude
                        && predicate->lat_min <= seg_point.normalized_latitude
                        && predicate->lat_max >= seg_point.normalized_latitude
                        && predicate->time_min <= seg_point.timestamp_sec
                        && predicate->time_max >= seg_point.timestamp_sec) {
                        result_count++;
                    }
                }
            }

            //print_traj_points(points, data_seg_points_num);


        }
    }
    //free_points_memory(points, 50);
    printf("result count: %d\n", result_count);
    return result_count;
}



static void
exe_complete(void *arg, const struct spdk_nvme_cpl *completion)
{
    struct hello_world_sequence *sequence = arg;

    /* Assume the I/O was successful */
    sequence->is_completed = 1;
    /* See if an error occurred. If so, display information
     * about it, and set completion value so that I/O
     * caller is aware that an error occurred.
     */
    if (spdk_nvme_cpl_is_error(completion)) {
        spdk_nvme_qpair_print_completion(sequence->ns_entry->qpair, (struct spdk_nvme_cpl *)completion);
        fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
        fprintf(stderr, "Read I/O failed, aborting run\n");
        sequence->is_completed = 2;
        exit(1);
    }

    /*
     * The read I/O has completed.  Print the contents of the
     *  buffer, free the buffer, then mark the sequence as
     *  completed.  This will trigger the hello_world() function
     *  to exit its polling loop.
     */
    int count = 0;
    /*if (sequence->is_id_temporal_query) {
        id_temporal_raw_traj_block(sequence);
    } else {
        spatio_temporal_raw_traj_block(sequence);
    }*/
    //spdk_free(sequence->buf);
    sequence->points_num += count;

}

static void
exe_complete_with_computation_without_meta_filtering(void *arg, const struct spdk_nvme_cpl *completion)
{
    struct hello_world_sequence *sequence = arg;

    /* Assume the I/O was successful */
    sequence->is_completed = 1;
    /* See if an error occurred. If so, display information
     * about it, and set completion value so that I/O
     * caller is aware that an error occurred.
     */
    if (spdk_nvme_cpl_is_error(completion)) {
        spdk_nvme_qpair_print_completion(sequence->ns_entry->qpair, (struct spdk_nvme_cpl *)completion);
        fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
        fprintf(stderr, "Read I/O failed, aborting run\n");
        sequence->is_completed = 2;
        exit(1);
    }

    /*
     * The read I/O has completed.  Print the contents of the
     *  buffer, free the buffer, then mark the sequence as
     *  completed.  This will trigger the hello_world() function
     *  to exit its polling loop.
     */
    int count = 0;
    if (sequence->is_id_temporal_query) {
        id_temporal_raw_traj_block_without_meta_filtering(sequence);
    } else {
        spatio_temporal_raw_traj_block_without_meta_filtering(sequence);
    }
    //spdk_free(sequence->buf);
    sequence->points_num += count;

}

static void
exe_complete_with_computation_with_meta_filtering(void *arg, const struct spdk_nvme_cpl *completion)
{
    struct hello_world_sequence *sequence = arg;

    /* Assume the I/O was successful */
    sequence->is_completed = 1;
    /* See if an error occurred. If so, display information
     * about it, and set completion value so that I/O
     * caller is aware that an error occurred.
     */
    if (spdk_nvme_cpl_is_error(completion)) {
        spdk_nvme_qpair_print_completion(sequence->ns_entry->qpair, (struct spdk_nvme_cpl *)completion);
        fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
        fprintf(stderr, "Read I/O failed, aborting run\n");
        sequence->is_completed = 2;
        exit(1);
    }

    /*
     * The read I/O has completed.  Print the contents of the
     *  buffer, free the buffer, then mark the sequence as
     *  completed.  This will trigger the hello_world() function
     *  to exit its polling loop.
     */
    int count = 0;
    if (sequence->is_id_temporal_query) {
        id_temporal_raw_traj_block_with_meta_filtering(sequence);
    } else {
        spatio_temporal_raw_traj_block_with_meta_filtering(sequence);
    }
    //spdk_free(sequence->buf);
    sequence->points_num += count;

}

static void
reset_zone_complete(void *arg, const struct spdk_nvme_cpl *completion)
{
    struct hello_world_sequence *sequence = arg;

    /* Assume the I/O was successful */
    sequence->is_completed = 1;
    /* See if an error occurred. If so, display information
     * about it, and set completion value so that I/O
     * caller is aware that an error occurred.
     */
    if (spdk_nvme_cpl_is_error(completion)) {
        spdk_nvme_qpair_print_completion(sequence->ns_entry->qpair, (struct spdk_nvme_cpl *)completion);
        fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
        fprintf(stderr, "Reset zone I/O failed, aborting run\n");
        sequence->is_completed = 2;
        exit(1);
    }
}

static void
reset_zone_and_wait_for_completion(struct hello_world_sequence *sequence)
{
    if (spdk_nvme_zns_reset_zone(sequence->ns_entry->ns, sequence->ns_entry->qpair,
                                 0, /* starting LBA of the zone to reset */
                                 false, /* don't reset all zones */
                                 reset_zone_complete,
                                 sequence)) {
        fprintf(stderr, "starting reset zone I/O failed\n");
        exit(1);
    }
    while (!sequence->is_completed) {
        spdk_nvme_qpair_process_completions(sequence->ns_entry->qpair, 0);
    }
    sequence->is_completed = 0;
}

static void
generate_id_temporal_computation_descriptor(void *buf, struct id_temporal_predicate *predicate) {
    struct isp_descriptor desc = {.isp_type = 111, .oid = predicate->oid, .time_min = predicate->time_min, .time_max = predicate->time_max, .lba_count = 0};
    int desc_size = calculate_isp_descriptor_space(&desc);
    if (desc_size >= 4096) {
        printf("the descriptor size is too big\n");
    }
    serialize_isp_descriptor(&desc, buf);
}

static void
init_sequence(struct hello_world_sequence *sequence, size_t block_size, int total_block_num, int block_index, struct ns_entry	*ns_entry, size_t *sz, struct id_temporal_predicate *id_predicate, struct spatio_temporal_range_predicate *range_predicate, bool is_id_temporal_query) {
    /*
         * Use spdk_dma_zmalloc to allocate a 4KB zeroed buffer.  This memory
         * will be pinned, which is required for data buffers used for SPDK NVMe
         * I/O operations.
         */
    sequence->using_cmb_io = 1;
    sequence->buf = spdk_nvme_ctrlr_map_cmb(ns_entry->ctrlr, sz);
    if (sequence->buf == NULL || *sz < block_size) {
        sequence->using_cmb_io = 0;
        sequence->buf = spdk_zmalloc(block_size * total_block_num, 0x1000, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    }
    if (sequence->buf == NULL) {
        printf("ERROR: write buffer allocation failed\n");
        return;
    }

    printf("buf pointer: %p\n", sequence->buf);
    //struct id_temporal_predicate predicate = {.oid = 20000380, .time_min = 1372636853, .time_max = 1372637853};
    sequence->id_temporal = *id_predicate;
    sequence->spatio_temporal = *range_predicate;

    if (sequence->using_cmb_io) {
        printf("INFO: using controller memory buffer for IO\n");
    } else {
        printf("INFO: using host memory buffer for IO\n");
    }
    sequence->is_completed = 0;
    sequence->ns_entry = ns_entry;

    /*
     * If the namespace is a Zoned Namespace, rather than a regular
     * NVM namespace, we need to reset the first zone, before we
     * write to it. This not needed for regular NVM namespaces.
     */
    if (spdk_nvme_ns_get_csi(ns_entry->ns) == SPDK_NVME_CSI_ZNS) {
        reset_zone_and_wait_for_completion(sequence);
    }

    /*
     * Print "Hello world!" to sequence.buf.  We will write this data to LBA
     *  0 on the namespace, and then later read it back into a separate buffer
     *  to demonstrate the full I/O path.
     */
    sequence->block_index = block_index;
    sequence->total_block_num = total_block_num;
    sequence->points_num = 0;
    sequence->is_id_temporal_query = is_id_temporal_query;

}

static void
hello_world(void)
{
    struct ns_entry			*ns_entry;
    int				rc;


    TAILQ_FOREACH(ns_entry, &g_namespaces, link) {
        /*
         * Allocate an I/O qpair that we can use to submit read/write requests
         *  to namespaces on the controller.  NVMe controllers typically support
         *  many qpairs per controller.  Any I/O qpair allocated for a controller
         *  can submit I/O to any namespace on that controller.
         *
         * The SPDK NVMe driver provides no synchronization for qpair accesses -
         *  the application must ensure only a single thread submits I/O to a
         *  qpair, and that same thread must also check for completions on that
         *  qpair.  This enables extremely efficient I/O processing by making all
         *  I/O operations completely lockless.
         */
        ns_entry->qpair = spdk_nvme_ctrlr_alloc_io_qpair(ns_entry->ctrlr, NULL, 0);
        if (ns_entry->qpair == NULL) {
            printf("ERROR: spdk_nvme_ctrlr_alloc_io_qpair() failed\n");
            return;
        }


        FILE *id_query_fp = fopen("/home/yangguo/Downloads/porto_10w_id_24h.query", "r");
        struct id_temporal_predicate **id_predicates = allocate_id_temporal_predicate_mem(50);
        read_id_temporal_queries_from_csv(id_query_fp, id_predicates, 50);

        FILE *st_query_fp = fopen("/home/yangguo/Downloads/porto_10w_1h_01.query", "r");
        struct spatio_temporal_range_predicate **st_predicates = allocate_spatio_temporal_predicate_mem(50);
        read_spatio_temporal_queries_from_csv(st_query_fp, st_predicates, 50);

        fill_points_buffer(points_buffer_size);
        int read_block_size = 4096;

        int total_count = 0;
        clock_t begin = clock();
        bool is_id_temporal_query = false;

        int read_block_num = 256;

        for (int i = 0; i < 8; i++) {
            if (i == 5) {
                struct hello_world_sequence sequence;
                size_t sz;
                init_sequence(&sequence, read_block_size, read_block_num, 1, ns_entry, &sz, id_predicates[i],
                              st_predicates[i], is_id_temporal_query);

                clock_t begin1 = clock();
                rc = spdk_nvme_ns_cmd_read(ns_entry->ns, ns_entry->qpair, sequence.buf,
                                           0, /* LBA start */
                                           read_block_num, /* number of LBAs */
                                           exe_complete_with_computation_with_meta_filtering, &sequence, 0);

                if (rc != 0) {
                    fprintf(stderr, "starting write I/O failed\n");
                    exit(1);
                }


                while (!sequence.is_completed) {
                    spdk_nvme_qpair_process_completions(ns_entry->qpair, 0);
                }
                clock_t end1 = clock();
                double time_spent1 = (double) (end1 - begin1) / CLOCKS_PER_SEC;
                printf("pure read operation time: %f\n", time_spent1);
                total_count += sequence.points_num;
                spdk_free(sequence.buf);
            }
        }
        clock_t end = clock();
        double time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
        printf("read operation time with memory allocation: %f\n", time_spent);

        spdk_nvme_ctrlr_free_io_qpair(ns_entry->qpair);
        printf("total point count: %d\n", total_count);
        free_points_buffer(points_buffer_size);
    }
}

static bool
probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
         struct spdk_nvme_ctrlr_opts *opts)
{
    printf("Attaching to %s\n", trid->traddr);

    return true;
}

static void
attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
          struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts)
{
    int nsid;
    struct ctrlr_entry *entry;
    struct spdk_nvme_ns *ns;
    const struct spdk_nvme_ctrlr_data *cdata;

    entry = malloc(sizeof(struct ctrlr_entry));
    if (entry == NULL) {
        perror("ctrlr_entry malloc");
        exit(1);
    }

    printf("Attached to %s\n", trid->traddr);

    /*
     * spdk_nvme_ctrlr is the logical abstraction in SPDK for an NVMe
     *  controller.  During initialization, the IDENTIFY data for the
     *  controller is read using an NVMe admin command, and that data
     *  can be retrieved using spdk_nvme_ctrlr_get_data() to get
     *  detailed information on the controller.  Refer to the NVMe
     *  specification for more details on IDENTIFY for NVMe controllers.
     */
    cdata = spdk_nvme_ctrlr_get_data(ctrlr);

    snprintf(entry->name, sizeof(entry->name), "%-20.20s (%-20.20s)", cdata->mn, cdata->sn);

    entry->ctrlr = ctrlr;
    TAILQ_INSERT_TAIL(&g_controllers, entry, link);

    /*
     * Each controller has one or more namespaces.  An NVMe namespace is basically
     *  equivalent to a SCSI LUN.  The controller's IDENTIFY data tells us how
     *  many namespaces exist on the controller.  For Intel(R) P3X00 controllers,
     *  it will just be one namespace.
     *
     * Note that in NVMe, namespace IDs start at 1, not 0.
     */
    for (nsid = spdk_nvme_ctrlr_get_first_active_ns(ctrlr); nsid != 0;
         nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, nsid)) {
        ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
        if (ns == NULL) {
            continue;
        }
        register_ns(ctrlr, ns);
    }
}

static void
cleanup(void)
{
    struct ns_entry *ns_entry, *tmp_ns_entry;
    struct ctrlr_entry *ctrlr_entry, *tmp_ctrlr_entry;
    struct spdk_nvme_detach_ctx *detach_ctx = NULL;

    TAILQ_FOREACH_SAFE(ns_entry, &g_namespaces, link, tmp_ns_entry) {
        TAILQ_REMOVE(&g_namespaces, ns_entry, link);
        free(ns_entry);
    }

    TAILQ_FOREACH_SAFE(ctrlr_entry, &g_controllers, link, tmp_ctrlr_entry) {
        TAILQ_REMOVE(&g_controllers, ctrlr_entry, link);
        spdk_nvme_detach_async(ctrlr_entry->ctrlr, &detach_ctx);
        free(ctrlr_entry);
    }

    if (detach_ctx) {
        spdk_nvme_detach_poll(detach_ctx);
    }
}

static void
usage(const char *program_name)
{
    printf("%s [options]", program_name);
    printf("\t\n");
    printf("options:\n");
    printf("\t[-d DPDK huge memory size in MB]\n");
    printf("\t[-g use single file descriptor for DPDK memory segments]\n");
    printf("\t[-i shared memory group ID]\n");
    printf("\t[-r remote NVMe over Fabrics target address]\n");
    printf("\t[-V enumerate VMD]\n");
#ifdef DEBUG
    printf("\t[-L enable debug logging]\n");
#else
    printf("\t[-L enable debug logging (flag disabled, must reconfigure with --enable-debug)\n");
#endif
}

static int
parse_args(int argc, char **argv, struct spdk_env_opts *env_opts)
{
    int op, rc;

    spdk_nvme_trid_populate_transport(&g_trid, SPDK_NVME_TRANSPORT_PCIE);
    snprintf(g_trid.subnqn, sizeof(g_trid.subnqn), "%s", SPDK_NVMF_DISCOVERY_NQN);

    while ((op = getopt(argc, argv, "d:gi:r:L:V")) != -1) {
        switch (op) {
            case 'V':
                g_vmd = true;
                break;
            case 'i':
                env_opts->shm_id = spdk_strtol(optarg, 10);
                if (env_opts->shm_id < 0) {
                    fprintf(stderr, "Invalid shared memory ID\n");
                    return env_opts->shm_id;
                }
                break;
            case 'g':
                env_opts->hugepage_single_segments = true;
                break;
            case 'r':
                if (spdk_nvme_transport_id_parse(&g_trid, optarg) != 0) {
                    fprintf(stderr, "Error parsing transport address\n");
                    return 1;
                }
                break;
            case 'd':
                env_opts->mem_size = spdk_strtol(optarg, 10);
                if (env_opts->mem_size < 0) {
                    fprintf(stderr, "Invalid DPDK memory size\n");
                    return env_opts->mem_size;
                }
                break;
            case 'L':
                rc = spdk_log_set_flag(optarg);
                if (rc < 0) {
                    fprintf(stderr, "unknown flag\n");
                    usage(argv[0]);
                    exit(EXIT_FAILURE);
                }
#ifdef DEBUG
                spdk_log_set_print_level(SPDK_LOG_DEBUG);
#endif
                break;
            default:
                usage(argv[0]);
                return 1;
        }
    }

    return 0;
}

int
main(int argc, char **argv)
{
    int rc;
    struct spdk_env_opts opts;

    /*
     * SPDK relies on an abstraction around the local environment
     * named env that handles memory allocation and PCI device operations.
     * This library must be initialized first.
     *
     */
    spdk_env_opts_init(&opts);
    rc = parse_args(argc, argv, &opts);
    if (rc != 0) {
        return rc;
    }

    opts.name = "hello_world";
    if (spdk_env_init(&opts) < 0) {
        fprintf(stderr, "Unable to initialize SPDK env\n");
        return 1;
    }

    printf("Initializing NVMe Controllers\n");

    if (g_vmd && spdk_vmd_init()) {
        fprintf(stderr, "Failed to initialize VMD."
                        " Some NVMe devices can be unavailable.\n");
    }

    /*
     * Start the SPDK NVMe enumeration process.  probe_cb will be called
     *  for each NVMe controller found, giving our application a choice on
     *  whether to attach to each controller.  attach_cb will then be
     *  called for each controller after the SPDK NVMe driver has completed
     *  initializing the controller we chose to attach.
     */
    rc = spdk_nvme_probe(&g_trid, NULL, probe_cb, attach_cb, NULL);
    if (rc != 0) {
        fprintf(stderr, "spdk_nvme_probe() failed\n");
        rc = 1;
        goto exit;
    }

    if (TAILQ_EMPTY(&g_controllers)) {
        fprintf(stderr, "no NVMe controllers found\n");
        rc = 1;
        goto exit;
    }

    printf("Initialization complete.\n");
    clock_t begin = clock();
    hello_world();
    clock_t end = clock();
    double time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
    printf("hello world total time: %f\n", time_spent);
    cleanup();
    if (g_vmd) {
        spdk_vmd_fini();
    }

    exit:
    cleanup();
    spdk_env_fini();
    return rc;
}