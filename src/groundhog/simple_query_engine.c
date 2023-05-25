//
// Created by yangguo on 11/16/22.
//

#include "groundhog/simple_query_engine.h"
#include "groundhog/porto_dataset_reader.h"
#include "groundhog/traj_block_format.h"
#include "groundhog/config.h"

#include <stdlib.h>
#include "groundhog/log.h"
#include "groundhog/isp_output_format.h"
#include "time.h"
#include "groundhog/bloom/bloom.h"
#include "groundhog/normalization_util.h"
#include "groundhog/traj_processing.h"

static bool enable_estimated_result_size = true;


struct host_result_buffer {
    void *buffer_ptr;
    int buffer_size;
    int point_count;
};


int
run_spatio_temporal_query_in_host(struct spatio_temporal_range_predicate *predicate, struct traj_storage *data_storage,
                                  int block_logical_addr_count, int *block_logical_addr_vec);

int
run_spatio_temporal_query_device(struct spatio_temporal_range_predicate *predicate, struct traj_storage *data_storage,
                                 struct seg_meta_section_entry_storage *meta_storage, int block_logical_addr_count,
                                 int *block_logical_addr_vec, bool enable_result_estimation);

int
run_spatio_temporal_query_device_fpga(struct spatio_temporal_range_predicate *predicate, struct traj_storage *data_storage,
                                      struct seg_meta_section_entry_storage *meta_storage, int block_logical_addr_count,
                                      int *block_logical_addr_vec, bool enable_result_estimation);

int
run_id_temporal_query_device(struct id_temporal_predicate *predicate, struct traj_storage *data_storage,
                                 struct seg_meta_section_entry_storage *meta_storage, int block_logical_addr_count,
                                 int *block_logical_addr_vec, bool enable_result_estimation);

int
run_id_temporal_query_device_fpga(struct id_temporal_predicate *predicate, struct traj_storage *data_storage,
                                  struct seg_meta_section_entry_storage *meta_storage, int block_logical_addr_count,
                                  int *block_logical_addr_vec, bool enable_result_estimation);

int
run_id_temporal_query_in_host(struct id_temporal_predicate *predicate, struct traj_storage *data_storage,
                              int block_logical_addr_count, int *block_logical_addr_vec);

        static int check_oid_exist(const int *oids, int array_size, int checked_oid) {
    for (int i = 0; i < array_size; i++) {
        if (oids[i] == checked_oid) {
            return 1;
        }
    }
    return -1;
}

static void parse_seg_meta_section(void *metadata, struct seg_meta *meta_array, int array_size) {
    char* m = metadata;
    int seg_meta_size = get_seg_meta_size();
    for (int i= 0; i < array_size; i++) {
        char* offset = m + i * seg_meta_size;
        deserialize_seg_meta(offset, meta_array + i);
    }
}

void init_query_engine(struct simple_query_engine *engine) {
    init_index_entry_storage(&(engine->index_storage));
    init_traj_storage(&(engine->data_storage));
    init_seg_meta_entry_storage(&(engine->seg_meta_storage));
}

void init_query_engine_with_persistence(struct simple_query_engine *engine, struct my_file *data_file, struct my_file *index_file, struct my_file *meta_file) {
    init_index_entry_storage_with_persistence(&(engine->index_storage), index_file->filename, index_file->file_operation_mode, index_file->fs_mode);
    init_traj_storage_with_persistence(&(engine->data_storage), data_file->filename, data_file->file_operation_mode, data_file->fs_mode);
    init_seg_meta_entry_storage_with_persistence(&(engine->seg_meta_storage), meta_file->filename, meta_file->file_operation_mode, meta_file->fs_mode);
}

void free_query_engine(struct simple_query_engine *engine) {
    free_index_entry_storage(&(engine->index_storage));
    free_traj_storage(&(engine->data_storage));
    free_seg_meta_entry_storage(&(engine->seg_meta_storage));
}

void ingest_data_via_time_partition(struct simple_query_engine *engine, FILE *fp, int block_num) {
    struct traj_storage *data_storage = &engine->data_storage;
    struct index_entry_storage *index_storage = &engine->index_storage;
    struct seg_meta_section_entry_storage *meta_storage = &engine->seg_meta_storage;
    // trajectory block info
    int points_num = calculate_points_num_via_block_size(TRAJ_BLOCK_SIZE, SPLIT_SEGMENT_NUM);

    for (int i = 0; i < block_num; i++) {
        struct traj_point **points = allocate_points_memory(points_num);
        read_points_from_csv(fp,points, i*points_num, points_num);
        // convert and put this data to traj storage
        void *data = malloc(TRAJ_BLOCK_SIZE);
        sort_traj_points(points, points_num);
        do_self_contained_traj_block(points, points_num, data, TRAJ_BLOCK_SIZE);
        struct address_pair data_addresses = append_traj_block_to_storage(data_storage, data);

        // update index
        struct index_entry *entry = malloc(sizeof(struct index_entry));
        init_index_entry(entry);
        fill_index_entry(entry, points, points_num, data_addresses.physical_ptr, data_addresses.logical_adr);
        append_index_entry_to_storage(index_storage, entry);

        // update seg_meta store
        struct seg_meta_section_entry *seg_entry = (struct seg_meta_section_entry *)malloc(sizeof(struct seg_meta_section_entry));
        struct traj_block_header header;
        parse_traj_block_for_header(data, &header);
        int meta_section_size = get_seg_meta_section_size(data);
        void* meta_section = malloc(meta_section_size);
        extract_seg_meta_section(data, meta_section);
        seg_entry->seg_meta_count = header.seg_count;
        seg_entry->seg_meta_section = meta_section;
        seg_entry->block_logical_adr = data_addresses.logical_adr;
        append_to_seg_meta_entry_storage(meta_storage, seg_entry);

        free_points_memory(points, points_num);
    }

    debug_print("[ingest_data_via_time_partition] num of ingesting data points: %d\n", points_num * (block_num - 1));

}



void ingest_data_via_zcurve_partition(struct simple_query_engine *engine, FILE *fp, int block_num) {
    struct traj_storage *data_storage = &engine->data_storage;
    struct index_entry_storage *index_storage = &engine->index_storage;
    struct seg_meta_section_entry_storage *meta_storage = &engine->seg_meta_storage;
    // trajectory block info
    int points_num = calculate_points_num_via_block_size(TRAJ_BLOCK_SIZE, SPLIT_SEGMENT_NUM);

    int buffer_size = 1024 * 8; // the number of block
    for (int i = 0; i < block_num; i+= buffer_size) {

        int used_buffer_size = block_num - i < buffer_size ? block_num - i : buffer_size;
        int total_points_num = points_num * used_buffer_size;

        struct traj_point **points_buffer = allocate_points_memory(total_points_num);
        read_points_from_csv(fp, points_buffer, i * points_num, total_points_num);
        sort_traj_points_zcurve(points_buffer, total_points_num);

        for (int j = 0; j < used_buffer_size; j++) {
            struct traj_point **points = points_buffer + j * points_num;

            // convert and put this data to traj storage
            void *data = malloc(TRAJ_BLOCK_SIZE);
            do_self_contained_traj_block(points, points_num, data, TRAJ_BLOCK_SIZE);
            struct address_pair data_addresses = append_traj_block_to_storage(data_storage, data);

            // update index
            struct index_entry *entry = malloc(sizeof(struct index_entry));
            init_index_entry(entry);
            fill_index_entry(entry, points, points_num, data_addresses.physical_ptr, data_addresses.logical_adr);
            append_index_entry_to_storage(index_storage, entry);

            // update seg_meta store
            struct seg_meta_section_entry *seg_entry = (struct seg_meta_section_entry *)malloc(sizeof(struct seg_meta_section_entry));
            struct traj_block_header header;
            parse_traj_block_for_header(data, &header);
            int meta_section_size = get_seg_meta_section_size(data);
            void* meta_section = malloc(meta_section_size);
            extract_seg_meta_section(data, meta_section);
            seg_entry->seg_meta_count = header.seg_count;
            seg_entry->seg_meta_section = meta_section;
            seg_entry->block_logical_adr = data_addresses.logical_adr;
            append_to_seg_meta_entry_storage(meta_storage, seg_entry);
        }

        free_points_memory(points_buffer, total_points_num);
    }


    debug_print("[ingest_data_via_zcurve_partition] num of ingesting data points: %d\n", points_num * (block_num - 1));

}

void ingest_synthetic_data_via_time_partition(struct simple_query_engine *engine, int block_num) {

    struct traj_storage *data_storage = &engine->data_storage;
    struct index_entry_storage *index_storage = &engine->index_storage;
    struct seg_meta_section_entry_storage *meta_storage = &engine->seg_meta_storage;
    // trajectory block info
    int points_num = calculate_points_num_via_block_size(TRAJ_BLOCK_SIZE, SPLIT_SEGMENT_NUM);

    for (int i = 0; i < block_num; i++) {
        struct traj_point **points = allocate_points_memory(points_num);
        //generate_synthetic_points(points, i*points_num, points_num);
        generate_synthetic_random_points(points, i*points_num, points_num, 29491199);
        // convert and put this data to traj storage
        void *data = malloc(TRAJ_BLOCK_SIZE);
        sort_traj_points(points, points_num);
        do_self_contained_traj_block(points, points_num, data, TRAJ_BLOCK_SIZE);
        struct address_pair data_addresses = append_traj_block_to_storage(data_storage, data);

        // update index
        struct index_entry *entry = malloc(sizeof(struct index_entry));
        init_index_entry(entry);
        fill_index_entry(entry, points, points_num, data_addresses.physical_ptr, data_addresses.logical_adr);
        append_index_entry_to_storage(index_storage, entry);

        // update seg_meta store
        struct seg_meta_section_entry *seg_entry = (struct seg_meta_section_entry *)malloc(sizeof(struct seg_meta_section_entry));
        struct traj_block_header header;
        parse_traj_block_for_header(data, &header);
        int meta_section_size = get_seg_meta_section_size(data);
        void* meta_section = malloc(meta_section_size);
        extract_seg_meta_section(data, meta_section);
        seg_entry->seg_meta_count = header.seg_count;
        seg_entry->seg_meta_section = meta_section;
        seg_entry->block_logical_adr = data_addresses.logical_adr;
        append_to_seg_meta_entry_storage(meta_storage, seg_entry);

        free_points_memory(points, points_num);
    }
    debug_print("[ingest_synthetic_data_via_time_partition] num of ingesting data points: %d\n", points_num * (block_num - 1));
}

void ingest_and_flush_data_via_time_partition(struct simple_query_engine *engine, FILE *fp, int block_num) {
    // ingest to memory
    ingest_data_via_time_partition(engine, fp, block_num);

    // flush memory to disk
    struct traj_storage *data_storage = &engine->data_storage;
    struct index_entry_storage *index_storage = &engine->index_storage;
    struct seg_meta_section_entry_storage *meta_storage = &engine->seg_meta_storage;

    // flush data storage
    flush_traj_storage(data_storage);
    // serialized and flush index storage
    struct serialized_index_storage serialized_index;
    init_serialized_index_storage(&serialized_index);
    serialize_index_entry_storage(index_storage, &serialized_index);
    flush_serialized_index_storage(&serialized_index, index_storage->my_fp->filename, index_storage->my_fp->fs_mode);
    free_serialized_index_storage(&serialized_index);
    // serialize and flush seg meta storage
    struct serialized_seg_meta_section_entry_storage serialized_seg_meta;
    init_serialized_seg_meta_section_entry_storage(&serialized_seg_meta);
    serialize_seg_meta_section_entry_storage(meta_storage, &serialized_seg_meta);
    flush_serialized_seg_meta_storage(&serialized_seg_meta, meta_storage->my_fp->filename, meta_storage->my_fp->fs_mode);
    free_serialized_seg_meta_section_entry_storage(&serialized_seg_meta);

}

void ingest_and_flush_data_via_zcurve_partition(struct simple_query_engine *engine, FILE *fp, int block_num) {
    // ingest to memory
    ingest_data_via_zcurve_partition(engine, fp, block_num);

    // flush memory to disk
    struct traj_storage *data_storage = &engine->data_storage;
    struct index_entry_storage *index_storage = &engine->index_storage;
    struct seg_meta_section_entry_storage *meta_storage = &engine->seg_meta_storage;

    // flush data storage
    flush_traj_storage(data_storage);
    // serialized and flush index storage
    struct serialized_index_storage serialized_index;
    init_serialized_index_storage(&serialized_index);
    serialize_index_entry_storage(index_storage, &serialized_index);
    flush_serialized_index_storage(&serialized_index, index_storage->my_fp->filename, index_storage->my_fp->fs_mode);
    free_serialized_index_storage(&serialized_index);
    // serialize and flush seg meta storage
    struct serialized_seg_meta_section_entry_storage serialized_seg_meta;
    init_serialized_seg_meta_section_entry_storage(&serialized_seg_meta);
    serialize_seg_meta_section_entry_storage(meta_storage, &serialized_seg_meta);
    flush_serialized_seg_meta_storage(&serialized_seg_meta, meta_storage->my_fp->filename, meta_storage->my_fp->fs_mode);
    free_serialized_seg_meta_section_entry_storage(&serialized_seg_meta);

}

void ingest_and_flush_synthetic_data_via_time_partition(struct simple_query_engine *engine, int block_num) {
    // ingest to memory
    ingest_synthetic_data_via_time_partition(engine, block_num);

    // flush memory to disk
    struct traj_storage *data_storage = &engine->data_storage;
    struct index_entry_storage *index_storage = &engine->index_storage;
    struct seg_meta_section_entry_storage *meta_storage = &engine->seg_meta_storage;

    // flush data storage
    flush_traj_storage(data_storage);
    // serialized and flush index storage
    struct serialized_index_storage serialized_index;
    init_serialized_index_storage(&serialized_index);
    serialize_index_entry_storage(index_storage, &serialized_index);
    flush_serialized_index_storage(&serialized_index, index_storage->my_fp->filename, index_storage->my_fp->fs_mode);
    free_serialized_index_storage(&serialized_index);
    // serialize and flush seg meta storage
    struct serialized_seg_meta_section_entry_storage serialized_seg_meta;
    init_serialized_seg_meta_section_entry_storage(&serialized_seg_meta);
    serialize_seg_meta_section_entry_storage(meta_storage, &serialized_seg_meta);
    flush_serialized_seg_meta_storage(&serialized_seg_meta, meta_storage->my_fp->filename, meta_storage->my_fp->fs_mode);
    free_serialized_seg_meta_section_entry_storage(&serialized_seg_meta);
}

void rebuild_query_engine_from_file(struct simple_query_engine *engine) {
    struct traj_storage *data_storage = &engine->data_storage;
    struct index_entry_storage *index_storage = &engine->index_storage;
    struct seg_meta_section_entry_storage *meta_storage = &engine->seg_meta_storage;

    rebuild_index_storage(index_storage->my_fp->filename, index_storage->my_fp->fs_mode, index_storage);
    rebuild_seg_meta_storage(meta_storage->my_fp->filename, meta_storage->my_fp->fs_mode, meta_storage);

}

/**
 * not used
 * @param engine
 * @param predicate
 * @return
 */
int id_temporal_query(struct simple_query_engine *engine, struct id_temporal_predicate *predicate) {
    int result_count = 0;
    // check the index
    struct index_entry_storage *index_storage = &engine->index_storage;
    struct traj_storage *data_storage = &engine->data_storage;
    for (int i = 0; i <= index_storage->current_index; i++) {
        struct index_entry *entry = index_storage->index_entry_base[i];
        if (check_oid_exist(entry->oid_array, entry->oid_array_size, predicate->oid) > 0
        && predicate->time_min <= entry->time_max
        && predicate->time_max >= entry->time_min) {
            void *data_block = NULL;
            if (entry->block_physical_ptr != NULL) {
                data_block = entry->block_physical_ptr;
            } else {
                data_block = malloc(TRAJ_BLOCK_SIZE);
                fetch_traj_data_via_logical_pointer(data_storage, entry->block_logical_adr, data_block);
            }

            // parse data block
            struct traj_block_header block_header;
            parse_traj_block_for_header(data_block, &block_header);
            struct seg_meta meta_array[block_header.seg_count];
            parse_traj_block_for_seg_meta_section(data_block, meta_array, block_header.seg_count);
            for (int j = 0; j < block_header.seg_count; j++) {
                struct seg_meta meta_item = meta_array[j];
                if (predicate->time_min <= meta_item.time_max && predicate->time_max >= meta_item.time_min) {
                    int data_seg_points_num = meta_item.seg_size / get_traj_point_size();
                    struct traj_point **points = allocate_points_memory(data_seg_points_num);
                    parse_traj_block_for_seg_data(data_block, meta_item.seg_offset, points, data_seg_points_num);
                    for (int k = 0; k < data_seg_points_num; k++) {
                        struct traj_point *point = points[k];
                        if (point->oid == predicate->oid
                        && predicate->time_min <= point->timestamp_sec
                        && predicate->time_max >= point->timestamp_sec) {
                            result_count++;
                        }
                    }
                    free_points_memory(points, data_seg_points_num);
                }
            }

        }
    }

    return result_count;

}



/**
 * not used
 * @param engine
 * @param predicate
 * @return
 */
int spatio_temporal_query(struct simple_query_engine *engine, struct spatio_temporal_range_predicate *predicate) {
    int result_count = 0;
    // check the index
    struct index_entry_storage *index_storage = &engine->index_storage;
    struct traj_storage *data_storage = &engine->data_storage;
    for (int i = 0; i <= index_storage->current_index; i++) {
        struct index_entry *entry = index_storage->index_entry_base[i];
        if (predicate->lon_min <= entry->lon_max
            && predicate->lon_max >= entry->lon_min
            && predicate->lat_min <= entry->lat_max
            && predicate->lat_max >= entry->lat_min
            && predicate->time_min <= entry->time_max
            && predicate->time_max >= entry->time_min) {
            void *data_block = NULL;
            if (entry->block_physical_ptr != NULL) {
                data_block = entry->block_physical_ptr;
            } else {
                data_block = malloc(TRAJ_BLOCK_SIZE);
                fetch_traj_data_via_logical_pointer(data_storage, entry->block_logical_adr, data_block);
                printf("block pointer: %d\n", entry->block_logical_adr);
            }

            // parse data block
            struct traj_block_header block_header;
            parse_traj_block_for_header(data_block, &block_header);
            struct seg_meta meta_array[block_header.seg_count];
            parse_traj_block_for_seg_meta_section(data_block, meta_array, block_header.seg_count);
            for (int j = 0; j < block_header.seg_count; j++) {
                struct seg_meta meta_item = meta_array[j];
                if (predicate->time_min <= meta_item.time_max && predicate->time_max >= meta_item.time_min
                && predicate->lon_min <= meta_item.lon_max && predicate->lon_max >= meta_item.lon_min
                && predicate->lat_min <= meta_item.lat_max && predicate->lat_max >= meta_item.lat_min) {
                    int data_seg_points_num = meta_item.seg_size / get_traj_point_size();
                    struct traj_point **points = allocate_points_memory(data_seg_points_num);
                    parse_traj_block_for_seg_data(data_block, meta_item.seg_offset, points, data_seg_points_num);
                    for (int k = 0; k < data_seg_points_num; k++) {
                        struct traj_point *point = points[k];
                        if (predicate->lon_min <= point->normalized_longitude
                            && predicate->lon_max >= point->normalized_longitude
                            && predicate->lat_min <= point->normalized_latitude
                            && predicate->lat_max >= point->normalized_latitude
                            && predicate->time_min <= point->timestamp_sec
                            && predicate->time_max >= point->timestamp_sec) {
                            result_count++;
                        }
                    }
                    free_points_memory(points, data_seg_points_num);
                }
            }

        }
    }
    return result_count;
}


static void assemble_isp_desc_for_id_temporal(struct isp_descriptor *isp_desc, struct id_temporal_predicate *predicate, int estimated_result_block_num, struct lba *lba_vec, int lba_vec_size) {
    isp_desc->isp_type = 0;
    isp_desc->estimated_result_page_num = estimated_result_block_num;
    isp_desc->oid = predicate->oid;
    isp_desc->lba_array = lba_vec;
    isp_desc->lba_count = lba_vec_size;
    isp_desc->time_min = predicate->time_min;
    isp_desc->time_max = predicate->time_max;
    isp_desc->lon_min = 0;
    isp_desc->lon_max = 0;
    isp_desc->lat_min = 0;
    isp_desc->lat_max = 0;
}

static void assemble_isp_desc_for_spatial_temporal(struct isp_descriptor *isp_desc, struct spatio_temporal_range_predicate *predicate, int estimated_result_block_num, struct lba *lba_vec, int lba_vec_size) {
    isp_desc->isp_type = 1;
    isp_desc->estimated_result_page_num = estimated_result_block_num;
    isp_desc->oid = 0;
    isp_desc->lba_array = lba_vec;
    isp_desc->lba_count = lba_vec_size;
    isp_desc->time_min = predicate->time_min;
    isp_desc->time_max = predicate->time_max;
    isp_desc->lon_min = predicate->lon_min;
    isp_desc->lon_max = predicate->lon_max;
    isp_desc->lat_min = predicate->lat_min;
    isp_desc->lat_max = predicate->lat_max;
}


int estimate_id_temporal_result_size(struct seg_meta_section_entry_storage *storage, struct id_temporal_predicate *predicate) {
    // TODO fix bug: over-estimated size since there is no oid info
    int result_size = 0;
    for (int i = 0; i <= storage->current_index; i++) {
        struct seg_meta_section_entry *entry = storage->base[i];
        struct seg_meta meta_array[entry->seg_meta_count];
        parse_seg_meta_section(entry->seg_meta_section, meta_array, entry->seg_meta_count);
        for (int j = 0; j < entry->seg_meta_count; j++) {
            struct seg_meta meta_item = meta_array[j];
            if (predicate->time_min <= meta_item.time_max && predicate->time_max >= meta_item.time_min) {
                result_size += meta_item.seg_size;
            }
        }
    }
    return result_size;

}


int estimate_spatio_temporal_result_size(struct seg_meta_section_entry_storage *storage, struct spatio_temporal_range_predicate *predicate) {
    int result_size = 0;
    for (int i = 0; i <= storage->current_index; i++) {
        struct seg_meta_section_entry *entry = storage->base[i];
        struct seg_meta meta_array[entry->seg_meta_count];
        parse_seg_meta_section(entry->seg_meta_section, meta_array, entry->seg_meta_count);
        for (int j = 0; j < entry->seg_meta_count; j++) {
            struct seg_meta meta_item = meta_array[j];

            if (predicate->time_min <= meta_item.time_max && predicate->time_max >= meta_item.time_min
                && predicate->lon_min <= meta_item.lon_max && predicate->lon_max >= meta_item.lon_min
                && predicate->lat_min <= meta_item.lat_max && predicate->lat_max >= meta_item.lat_min) {
                result_size += meta_item.seg_size;
            }
        }
    }
    return result_size;
}

int estimate_id_temporal_result_size_for_blocks(struct seg_meta_section_entry_storage *storage, struct id_temporal_predicate *predicate, int *block_addr_vec, int block_addr_vec_size) {
    int result_size = 0;
    /*for (int i = 0; i <= storage->current_index; i++) {

        struct seg_meta_section_entry *entry = storage->base[i];
        for (int j = 0; j < block_addr_vec_size; j++) {
            if (entry->block_logical_adr == block_addr_vec[j]) {
                struct seg_meta meta_array[entry->seg_meta_count];
                parse_seg_meta_section(entry->seg_meta_section, meta_array, entry->seg_meta_count);
                for (int k = 0; k < entry->seg_meta_count; k++) {
                    struct seg_meta meta_item = meta_array[k];

                    if (predicate->time_min <= meta_item.time_max && predicate->time_max >= meta_item.time_min
                        && predicate->lon_min <= meta_item.lon_max && predicate->lon_max >= meta_item.lon_min
                        && predicate->lat_min <= meta_item.lat_max && predicate->lat_max >= meta_item.lat_min) {
                        result_size += meta_item.seg_size;
                    }
                }
            }
        }
    }*/

    for (int i = 0; i < block_addr_vec_size; i++) {
        int block_addr = block_addr_vec[i];
        //printf("block addr: %d\n", block_addr);
        struct seg_meta_section_entry * entry = storage->base[block_addr];
        struct seg_meta meta_array[entry->seg_meta_count];
        parse_seg_meta_section(entry->seg_meta_section, meta_array, entry->seg_meta_count);
        for (int k = 0; k < entry->seg_meta_count; k++) {
            struct seg_meta meta_item = meta_array[k];

            bloom_filter rebuild_filter;
            bit_vect rebuild_bit_vect;
            void* bit_mem = meta_item.oid_filter;
            bloom_filter_rebuild_default(&rebuild_filter, &rebuild_bit_vect, bit_mem, MY_OID_FILTER_SIZE * 8);
            bool oid_contained = bloom_filter_test(&rebuild_filter, &predicate->oid, 4);

            if (oid_contained && predicate->time_min <= meta_item.time_max && predicate->time_max >= meta_item.time_min) {
                result_size += meta_item.seg_size;
            }
        }
    }

    return result_size + 16 * (result_size / 0x1000);    // extra space for metadata in isp output block
}

int estimate_spatio_temporal_result_size_for_blocks(struct seg_meta_section_entry_storage *storage, struct spatio_temporal_range_predicate *predicate, int *block_addr_vec, int block_addr_vec_size) {
    int result_size = 0;
    /*for (int i = 0; i <= storage->current_index; i++) {

        struct seg_meta_section_entry *entry = storage->base[i];
        for (int j = 0; j < block_addr_vec_size; j++) {
            if (entry->block_logical_adr == block_addr_vec[j]) {
                struct seg_meta meta_array[entry->seg_meta_count];
                parse_seg_meta_section(entry->seg_meta_section, meta_array, entry->seg_meta_count);
                for (int k = 0; k < entry->seg_meta_count; k++) {
                    struct seg_meta meta_item = meta_array[k];

                    if (predicate->time_min <= meta_item.time_max && predicate->time_max >= meta_item.time_min
                        && predicate->lon_min <= meta_item.lon_max && predicate->lon_max >= meta_item.lon_min
                        && predicate->lat_min <= meta_item.lat_max && predicate->lat_max >= meta_item.lat_min) {
                        result_size += meta_item.seg_size;
                    }
                }
            }
        }
    }*/

    for (int i = 0; i < block_addr_vec_size; i++) {
        int block_addr = block_addr_vec[i];
        struct seg_meta_section_entry * entry = storage->base[block_addr];
        struct seg_meta meta_array[entry->seg_meta_count];
        parse_seg_meta_section(entry->seg_meta_section, meta_array, entry->seg_meta_count);
        for (int k = 0; k < entry->seg_meta_count; k++) {
            struct seg_meta meta_item = meta_array[k];

            if (predicate->time_min <= meta_item.time_max && predicate->time_max >= meta_item.time_min
                && predicate->lon_min <= meta_item.lon_max && predicate->lon_max >= meta_item.lon_min
                && predicate->lat_min <= meta_item.lat_max && predicate->lat_max >= meta_item.lat_min) {
                result_size += meta_item.seg_size;
            }
        }
    }

    return result_size + 16 * (result_size / 0x1000);    // extra space for metadata in isp output block
}

int assemble_lba_vec(struct lba *lba_vec, int base_lba, int *block_logical_adr_vec, int block_logical_adr_vec_size) {
    for (int i = 0; i < block_logical_adr_vec_size; i++) {
        lba_vec[i].start_lba = base_lba + block_logical_adr_vec[i];
        lba_vec[i].sector_count = 1;
    }
    return block_logical_adr_vec_size;
}

int assemble_lba_vec_via_block_meta(struct lba *lba_vec, int base_lba, struct continuous_block_meta *block_meta_vec, int block_logical_adr_vec_size) {
    int factor = TRAJ_BLOCK_SIZE / SECTOR_SIZE;
    for (int i = 0; i < block_logical_adr_vec_size; i++) {
        lba_vec[i].start_lba = base_lba + block_meta_vec[i].block_start * factor;
        lba_vec[i].sector_count = block_meta_vec[i].block_count * factor;
    }
    return block_logical_adr_vec_size;
}

/**
 * not used
 * @param engine
 * @param predicate
 * @return
 */
int id_temporal_query_isp(struct simple_query_engine *engine, struct id_temporal_predicate *predicate) {
    int result_count = 0;
    // check the index
    struct index_entry_storage *index_storage = &engine->index_storage;
    struct traj_storage *data_storage = &engine->data_storage;
    struct seg_meta_section_entry_storage *meta_storage = &engine->seg_meta_storage;

    int block_logical_adr_vec[256];
    int block_logical_adr_count = 0;
    for (int i = 0; i <= index_storage->current_index; i++) {
        struct index_entry *entry = index_storage->index_entry_base[i];
        if (check_oid_exist(entry->oid_array, entry->oid_array_size, predicate->oid) > 0
            && predicate->time_min <= entry->time_max
            && predicate->time_max >= entry->time_min) {

            if (block_logical_adr_count > 255) {
                printf("Too many blocks in one isp. Not supported now\n");
                return -1;
            }

            block_logical_adr_vec[block_logical_adr_count] = entry->block_logical_adr;
            block_logical_adr_count++;

        }
    }

    //int estimated_result_size = estimate_id_temporal_result_size(meta_storage, predicate);
    int estimated_result_size = block_logical_adr_count * 0x1000;
    if (estimated_result_size == 0) {
        return 0;
    }
    if (estimated_result_size % 0x1000 != 0) {
        estimated_result_size = ((estimated_result_size / 0x1000) + 1) * 0x1000;
    }
    int estimated_result_block_num = estimated_result_size / 0x1000;
    void* result_buffer = malloc(estimated_result_size);

    struct isp_descriptor isp_desc;
    struct lba lba_vec[256];
    int lba_vec_size = assemble_lba_vec(lba_vec, DATA_FILE_OFFSET, block_logical_adr_vec, block_logical_adr_count);
    assemble_isp_desc_for_id_temporal(&isp_desc, predicate, estimated_result_block_num, lba_vec, lba_vec_size);
    do_isp_for_trajectory_data(data_storage, result_buffer, estimated_result_size, &isp_desc, 0);

    for (int block_count = 0; block_count < estimated_result_block_num; block_count++, result_buffer+=4096) {
        int points_num = parse_points_num_from_output_buffer_page(result_buffer);
        //printf("block points num: %d\n", points_num);
        if (points_num > 0) {
            struct traj_point **points = allocate_points_memory(points_num);
            deserialize_output_buffer_page(result_buffer, points, points_num);
            for (int k = 0; k < points_num; k++) {
                struct traj_point *point = points[k];
                //printf("oid: %d, time: %d, lon: %d, lat: %d\n", point->oid, point->timestamp_sec, point->normalized_longitude, point->normalized_latitude);
                if (point->oid == predicate->oid
                    && predicate->time_min <= point->timestamp_sec
                    && predicate->time_max >= point->timestamp_sec) {
                    result_count++;
                }
            }
            free_points_memory(points, points_num);
        }
    }

    return result_count;
}

/**
 * not used
 * @param engine
 * @param predicate
 * @return
 */
int spatio_temporal_query_isp(struct simple_query_engine *engine, struct spatio_temporal_range_predicate *predicate) {
    int result_count = 0;
    // check the index
    struct index_entry_storage *index_storage = &engine->index_storage;
    struct traj_storage *data_storage = &engine->data_storage;
    struct seg_meta_section_entry_storage *meta_storage = &engine->seg_meta_storage;

    int block_logical_adr_vec[256];
    int block_logical_adr_count = 0;
    for (int i = 0; i <= index_storage->current_index; i++) {
        struct index_entry *entry = index_storage->index_entry_base[i];
        if (predicate->lon_min <= entry->lon_max
            && predicate->lon_max >= entry->lon_min
            && predicate->lat_min <= entry->lat_max
            && predicate->lat_max >= entry->lat_min
            && predicate->time_min <= entry->time_max
            && predicate->time_max >= entry->time_min) {

            if (block_logical_adr_count > 255) {
                printf("Too many blocks in one isp. Not supported now\n");
                return -1;
            }

            block_logical_adr_vec[block_logical_adr_count] = entry->block_logical_adr;
            block_logical_adr_count++;

        }
    }

    //int estimated_result_size = estimate_id_temporal_result_size(meta_storage, predicate);
    int estimated_result_size = block_logical_adr_count * 0x1000;
    if (estimated_result_size == 0) {
        return 0;
    }
    if (estimated_result_size % 0x1000 != 0) {
        estimated_result_size = ((estimated_result_size / 0x1000) + 1) * 0x1000;
    }
    int estimated_result_block_num = estimated_result_size / 0x1000;
    void* result_buffer = malloc(estimated_result_size);

    struct isp_descriptor isp_desc;
    struct lba lba_vec[256];
    int lba_vec_size = assemble_lba_vec(lba_vec, DATA_FILE_OFFSET, block_logical_adr_vec, block_logical_adr_count);
    assemble_isp_desc_for_spatial_temporal(&isp_desc, predicate, estimated_result_block_num, lba_vec, lba_vec_size);
    do_isp_for_trajectory_data(data_storage, result_buffer, estimated_result_size, &isp_desc, 0);

    for (int block_count = 0; block_count < estimated_result_block_num; block_count++, result_buffer+=4096) {
        int points_num = parse_points_num_from_output_buffer_page(result_buffer);
        //printf("block points num: %d\n", points_num);
        if (points_num > 0) {
            struct traj_point **points = allocate_points_memory(points_num);
            deserialize_output_buffer_page(result_buffer, points, points_num);
            for (int k = 0; k < points_num; k++) {
                struct traj_point *point = points[k];
                //printf("oid: %d, time: %d, lon: %d, lat: %d\n", point->oid, point->timestamp_sec, point->normalized_longitude, point->normalized_latitude);
                if (predicate->lon_min <= point->normalized_longitude
                    && predicate->lon_max >= point->normalized_longitude
                    && predicate->lat_min <= point->normalized_latitude
                    && predicate->lat_max >= point->normalized_latitude
                    && predicate->time_min <= point->timestamp_sec
                    && predicate->time_max >= point->timestamp_sec) {
                    result_count++;
                }
            }
            free_points_memory(points, points_num);
        }
    }

    return result_count;
}

int calculate_aggregate_block_vec_size(int *block_addr_vec, int block_addr_vec_size) {
    if (block_addr_vec_size == 1) {
        return 1;
    }

    int aggregate_block_size = 0;
    for (int i = 0; i < block_addr_vec_size - 1; i++) {
        if (block_addr_vec[i] + 1 != block_addr_vec[i + 1]) {
            aggregate_block_size++;
        }
    }
    aggregate_block_size++;
    return aggregate_block_size;

}

void aggregate_blocks(int *block_addr_vec, int block_addr_vec_size, struct continuous_block_meta *block_meta_vec, int block_meta_vec_size) {

    if (calculate_aggregate_block_vec_size(block_addr_vec, block_addr_vec_size) > block_meta_vec_size) {
        fprintf(stderr, "[simple query engine] not matched aggregate block num\n");
        return;
    }

    if (block_addr_vec_size == 1) {
        block_meta_vec[0].block_start = block_addr_vec[0];
        block_meta_vec[0].block_count = 1;
        return;
    }

    int base = 0;
    int cursor = 0;
    int current_block_meta_vec_index = 0;
    for (int i = 0; i < block_addr_vec_size - 1; i++) {
        if (block_addr_vec[i] + 1 == block_addr_vec[i + 1]) {
            // move cursor
            cursor++;
        } else {
//            printf("base: %d; base element: %d\n", base, block_addr_vec[base]);
//            printf("cursor: %d\n", cursor);
//            printf("count: %d\n\n", cursor - base + 1);
            block_meta_vec[current_block_meta_vec_index].block_start = block_addr_vec[base];
            block_meta_vec[current_block_meta_vec_index].block_count = cursor - base + 1;
            current_block_meta_vec_index++;
            // start a new continuous block region
            base = i+1;
            cursor = i+1;
        }
    }

    // handle the last one
//    printf("base: %d; base element: %d\n", base, block_addr_vec[base]);
//    printf("cursor: %d\n", cursor);
//    printf("count: %d\n\n", cursor - base + 1);
    block_meta_vec[current_block_meta_vec_index].block_start = block_addr_vec[base];
    block_meta_vec[current_block_meta_vec_index].block_count = cursor - base + 1;
}

void print_aggregated_blocks_meta(struct continuous_block_meta *block_meta_vec, int block_meta_vec_size) {
    for (int i = 0; i < block_meta_vec_size; i++) {
        printf("block start: %d\n", block_meta_vec[i].block_start);
        printf("block count: %d\n\n", block_meta_vec[i].block_count);
    }
}

static struct traj_point **points_buffer;
static int points_buffer_size = 400;
static void fill_points_buffer(int points_num) {
    points_buffer = allocate_points_memory(points_num);
}

static void free_points_buffer(int points_num) {
    free_points_memory(points_buffer, points_num);
}

static int mycount = 0;
static int spatio_temporal_query_raw_trajectory_block(void* data_block, struct spatio_temporal_range_predicate *predicate) {
    int result_count = 0;
    struct traj_block_header block_header;
    parse_traj_block_for_header(data_block, &block_header);
    struct traj_point tmp_point;

    mycount++;
    //printf("segment count: %d, index: %d\n", block_header.seg_count, mycount);
    struct seg_meta meta_array[block_header.seg_count];
    parse_traj_block_for_seg_meta_section(data_block, meta_array, block_header.seg_count);
    for (int j = 0; j < block_header.seg_count; j++) {
        struct seg_meta meta_item = meta_array[j];
        if (predicate->time_min <= meta_item.time_max && predicate->time_max >= meta_item.time_min
            && predicate->lon_min <= meta_item.lon_max && predicate->lon_max >= meta_item.lon_min
            && predicate->lat_min <= meta_item.lat_max && predicate->lat_max >= meta_item.lat_min) {
            int data_seg_points_num = meta_item.seg_size / get_traj_point_size();
            //struct traj_point **points = allocate_points_memory(data_seg_points_num);
            struct traj_point **points = points_buffer;
            if (points_buffer_size < data_seg_points_num) {
                printf("the points buffer is not big enough\n");
            }
            //parse_traj_block_for_seg_data(data_block, meta_item.seg_offset, points, data_seg_points_num);
            struct traj_point *point_ptr = (struct traj_point *)((char *) data_block + meta_item.seg_offset);

            for (int k = 0; k < data_seg_points_num; k++) {
                //struct traj_point *point = points[k];
                struct traj_point *point = &point_ptr[k];
                if (predicate->lon_min <= point->normalized_longitude
                    && predicate->lon_max >= point->normalized_longitude
                    && predicate->lat_min <= point->normalized_latitude
                    && predicate->lat_max >= point->normalized_latitude
                    && predicate->time_min <= point->timestamp_sec
                    && predicate->time_max >= point->timestamp_sec) {
                    result_count++;
                    tmp_point.oid = point->oid; // simulate the operation to write result to output buffer
                    tmp_point.normalized_longitude = point->normalized_longitude;
                    tmp_point.normalized_latitude = point->normalized_latitude;
                    tmp_point.timestamp_sec = point->timestamp_sec;
                }
            }
            //free_points_memory(points, data_seg_points_num);
        }
    }
    return result_count;
}

static int id_temporal_query_raw_trajectory_block(void* data_block, struct id_temporal_predicate *predicate) {
    int result_count = 0;
    struct traj_block_header block_header;
    struct traj_point tmp_point;

    parse_traj_block_for_header(data_block, &block_header);
    struct seg_meta meta_array[block_header.seg_count];
    parse_traj_block_for_seg_meta_section(data_block, meta_array, block_header.seg_count);
    for (int j = 0; j < block_header.seg_count; j++) {
        struct seg_meta meta_item = meta_array[j];
        bloom_filter  rebuild_filter;
        bit_vect rebuilt_bit_vec;
        void* bit_mem = meta_item.oid_filter;
        bloom_filter_rebuild_default(&rebuild_filter, &rebuilt_bit_vec, bit_mem, MY_OID_FILTER_SIZE * 8);
        bool oid_contained = bloom_filter_test(&rebuild_filter, &predicate->oid, 4);

        if (oid_contained && predicate->time_min <= meta_item.time_max && predicate->time_max >= meta_item.time_min) {
            int data_seg_points_num = meta_item.seg_size / get_traj_point_size();
            //struct traj_point **points = allocate_points_memory(data_seg_points_num);
            struct traj_point **points = points_buffer;
            if (points_buffer_size < data_seg_points_num) {
                printf("the points buffer is not big enough\n");
            }
            parse_traj_block_for_seg_data(data_block, meta_item.seg_offset, points, data_seg_points_num);
            for (int k = 0; k < data_seg_points_num; k++) {
                struct traj_point *point = points[k];
                if (predicate->oid <= point->oid
                    && predicate->time_min <= point->timestamp_sec
                    && predicate->time_max >= point->timestamp_sec) {
                    result_count++;
                    tmp_point.oid = point->oid; // simulate the operation to write result to output buffer
                    tmp_point.normalized_longitude = point->normalized_longitude;
                    tmp_point.normalized_latitude = point->normalized_latitude;
                    tmp_point.timestamp_sec = point->timestamp_sec;
                }
            }
            //free_points_memory(points, data_seg_points_num);
        }
    }
    return result_count;
}

static int spatio_temporal_query_raw_trajectory_block_without_meta_filtering(void* data_block, struct spatio_temporal_range_predicate *predicate) {
    struct traj_point result;
    int result_count = 0;
    struct traj_block_header block_header;
    parse_traj_block_for_header(data_block, &block_header);

    struct traj_point tmp_point;

    struct seg_meta meta_array[block_header.seg_count];
    parse_traj_block_for_seg_meta_section(data_block, meta_array, block_header.seg_count);
    for (int j = 0; j < block_header.seg_count; j++) {
        struct seg_meta meta_item = meta_array[j];

        int data_seg_points_num = meta_item.seg_size / get_traj_point_size();

        struct traj_point **points = points_buffer;
        if (points_buffer_size < data_seg_points_num) {
            printf("the points buffer is not big enough\n");
        }
        //parse_traj_block_for_seg_data(data_block, meta_item.seg_offset, points, data_seg_points_num);

        struct traj_point *point_ptr = (struct traj_point *)((char *) data_block + meta_item.seg_offset);
        //printf("oid: %d, time: %d, lon: %d, lat: %d\n", points[0]->oid, points[0]->timestamp_sec, points[0]->normalized_longitude, points[0]->normalized_latitude);
        for (int k = 0; k < data_seg_points_num; k++) {
            //struct traj_point *point = points[k];
            struct traj_point *point = &point_ptr[k];
            if (predicate->lon_min <= point->normalized_longitude
                    && predicate->lon_max >= point->normalized_longitude
                    && predicate->lat_min <= point->normalized_latitude
                    && predicate->lat_max >= point->normalized_latitude
                    && predicate->time_min <= point->timestamp_sec
                    && predicate->time_max >= point->timestamp_sec) {

                result_count++;
                tmp_point.oid = point->oid; // simulate the operation to write result to output buffer
                tmp_point.normalized_longitude = point->normalized_longitude;
                tmp_point.normalized_latitude = point->normalized_latitude;
                tmp_point.timestamp_sec = point->timestamp_sec;
            }

        }

    }

    return result_count;
}

static int spatio_temporal_query_raw_trajectory_block_without_meta_filtering_test(void* data_block, struct spatio_temporal_range_predicate *predicate) {
    struct traj_point result;
    int result_count = 0;
    struct traj_block_header block_header;
    parse_traj_block_for_header(data_block, &block_header);

    struct traj_point tmp_point;


    char* meta_ptr = ((char*) data_block + get_header_size());
    for (int j = 0; j < block_header.seg_count; j++) {
        struct seg_meta *meta_item = (struct seg_meta *)(meta_ptr + j * get_seg_meta_size());
        int data_seg_points_num = meta_item->seg_size / get_traj_point_size();

        struct traj_point *point_ptr = (struct traj_point *)((char *) data_block + meta_item->seg_offset);
        for (int k = 0; k < data_seg_points_num; k++) {

            struct traj_point *point = &point_ptr[k];
            if (predicate->lon_min <= point->normalized_longitude
                && predicate->lon_max >= point->normalized_longitude
                && predicate->lat_min <= point->normalized_latitude
                && predicate->lat_max >= point->normalized_latitude
                && predicate->time_min <= point->timestamp_sec
                && predicate->time_max >= point->timestamp_sec) {

                result_count++;
                tmp_point.oid = point->oid; // simulate the operation to write result to output buffer
                tmp_point.normalized_longitude = point->normalized_longitude;
                tmp_point.normalized_latitude = point->normalized_latitude;
                tmp_point.timestamp_sec = point->timestamp_sec;
            }

        }

    }

    return result_count;
}

static void init_result_buffer(struct host_result_buffer *buffer, int buffer_size) {
    buffer->point_count = 0;
    buffer->buffer_size = buffer_size;
    buffer->buffer_ptr = malloc(buffer_size);
}

static void free_result_buffer(struct host_result_buffer *buffer) {
    free(buffer->buffer_ptr);
}

static void put_result_to_buffer(struct host_result_buffer *result_buffer, void* data, int size) {
    if (result_buffer->point_count * size + size > result_buffer->buffer_size) {
        printf("no space left in buffer\n");
        return;
    }
    struct traj_point *dst = (struct traj_point*) result_buffer->buffer_ptr;
    struct traj_point *src = (struct traj_point*) data;
    dst[result_buffer->point_count] = *src;
    result_buffer->point_count++;
}

static int spatio_temporal_query_raw_trajectory_block_with_result_buffer_without_meta_filtering(void* data_block, struct spatio_temporal_range_predicate *predicate, struct host_result_buffer* result_buffer) {
    struct traj_point result;
    int result_count = 0;
    struct traj_block_header block_header;
    parse_traj_block_for_header(data_block, &block_header);


    struct seg_meta meta_array[block_header.seg_count];
    parse_traj_block_for_seg_meta_section(data_block, meta_array, block_header.seg_count);
    for (int j = 0; j < block_header.seg_count; j++) {
        struct seg_meta meta_item = meta_array[j];

        int data_seg_points_num = meta_item.seg_size / get_traj_point_size();

        struct traj_point **points = points_buffer;
        if (points_buffer_size < data_seg_points_num) {
            printf("the points buffer is not big enough\n");
        }
        parse_traj_block_for_seg_data(data_block, meta_item.seg_offset, points, data_seg_points_num);

        //struct traj_point *point_ptr = (struct traj_point *)((char *) data_block + meta_item.seg_offset);
        //printf("oid: %d, time: %d, lon: %d, lat: %d\n", points[0]->oid, points[0]->timestamp_sec, points[0]->normalized_longitude, points[0]->normalized_latitude);
        for (int k = 0; k < data_seg_points_num; k++) {
            struct traj_point *point = points[k];
            //struct traj_point *point = &point_ptr[k];
            if (predicate->lon_min <= point->normalized_longitude
                && predicate->lon_max >= point->normalized_longitude
                && predicate->lat_min <= point->normalized_latitude
                && predicate->lat_max >= point->normalized_latitude
                && predicate->time_min <= point->timestamp_sec
                && predicate->time_max >= point->timestamp_sec) {

                result_count++;
                put_result_to_buffer(result_buffer, point, 16);
            }

        }

    }

    return result_count;
}

static int id_temporal_query_raw_trajectory_block_without_meta_filtering(void* data_block, struct id_temporal_predicate *predicate) {
    int result_count = 0;
    struct traj_block_header block_header;
    parse_traj_block_for_header(data_block, &block_header);

    struct traj_point tmp_point;

    struct seg_meta meta_array[block_header.seg_count];
    parse_traj_block_for_seg_meta_section(data_block, meta_array, block_header.seg_count);
    for (int j = 0; j < block_header.seg_count; j++) {
        struct seg_meta meta_item = meta_array[j];

        int data_seg_points_num = meta_item.seg_size / get_traj_point_size();
        //struct traj_point **points = allocate_points_memory(data_seg_points_num);
        struct traj_point **points = points_buffer;
        if (points_buffer_size < data_seg_points_num) {
            printf("the points buffer is not big enough\n");
        }
        //print_traj_points(points, data_seg_points_num);

        parse_traj_block_for_seg_data(data_block, meta_item.seg_offset, points, data_seg_points_num);
        //printf("oid: %d, time: %d, lon: %d, lat: %d\n", points[0]->oid, points[0]->timestamp_sec, points[0]->normalized_longitude, points[0]->normalized_latitude);
        for (int k = 0; k < data_seg_points_num; k++) {
            struct traj_point *point = points[k];
            if (predicate->oid == point->oid
                && predicate->time_min <= point->timestamp_sec
                && predicate->time_max >= point->timestamp_sec) {
                result_count++;
                tmp_point.oid = point->oid; // simulate the operation to write result to output buffer
                tmp_point.normalized_longitude = point->normalized_longitude;
                tmp_point.normalized_latitude = point->normalized_latitude;
                tmp_point.timestamp_sec = point->timestamp_sec;

            }
        }
        //free_points_memory(points, data_seg_points_num);
        //}
    }
    return result_count;
}

static int spatio_temporal_query_isp_output_block(void* data_block, struct spatio_temporal_range_predicate *predicate) {
    int result_count = 0;
    int points_num = parse_points_num_from_output_buffer_page(data_block);
    //printf("block points num: %d\n", points_num);
    if (points_num > 0) {
        //struct traj_point **points = allocate_points_memory(points_num);
        struct traj_point **points = points_buffer;
        deserialize_output_buffer_page(data_block, points, points_num);
        //struct traj_point *point_ptr = (struct traj_point *) ((char*)data_block + 4);
        for (int k = 0; k < points_num; k++) {
            struct traj_point *point = points[k];
            //printf("oid: %d, time: %d, lon: %d, lat: %d\n", point->oid, point->timestamp_sec, point->normalized_longitude, point->normalized_latitude);
            /*if (predicate->lon_min <= point->normalized_longitude
                && predicate->lon_max >= point->normalized_longitude
                && predicate->lat_min <= point->normalized_latitude
                && predicate->lat_max >= point->normalized_latitude
                && predicate->time_min <= point->timestamp_sec
                && predicate->time_max >= point->timestamp_sec) {
                result_count++;
            }*/
            result_count++;
        }
        //free_points_memory(points, points_num);
    }
    return result_count;
}

static int id_temporal_query_isp_output_block(void* data_block, struct id_temporal_predicate *predicate) {
    int result_count = 0;
    int points_num = parse_points_num_from_output_buffer_page(data_block);
    //printf("block points num: %d\n", points_num);
    if (points_num > 0) {
        //struct traj_point **points = allocate_points_memory(points_num);
        struct traj_point **points = points_buffer;
        deserialize_output_buffer_page(data_block, points, points_num);
        for (int k = 0; k < points_num; k++) {
            struct traj_point *point = points[k];

            result_count++;
        }
        //free_points_memory(points, points_num);
    }
    return result_count;
}

static int spatio_temporal_query_isp_fpga_output_blocks(void* data_blocks, struct spatio_temporal_range_predicate *predicate) {
    int result_count = 0;
    memcpy(&result_count, data_blocks, 4);

    struct traj_point *points_base = (struct traj_point *)((char *)data_blocks + 16);   // output format is good, so do not need deserialization

    for (int i = 0; i < result_count; i++) {
        struct traj_point tmp = points_base[i];
        //printf("oid: %d, lon: %d, lat: %d, time: %d\n", tmp.oid, tmp.normalized_longitude, tmp.normalized_latitude, tmp.timestamp_sec);
    }
    return result_count;
}

static int id_temporal_query_isp_fpga_output_blocks(void* data_blocks, struct id_temporal_predicate *predicate) {
    int result_count = 0;
    memcpy(&result_count, data_blocks, 4);

    struct traj_point *points_base = (struct traj_point *)((char *)data_blocks + 16);
    for (int i = 0; i < result_count; i++) {
        struct traj_point tmp = points_base[i];
        //printf("oid: %d, lon: %d, lat: %d, time: %d\n", tmp.oid, tmp.normalized_longitude, tmp.normalized_latitude, tmp.timestamp_sec);
    }
    return result_count;
}

static double calculate_goodness_for_spatio_temporal(struct seg_meta_section_entry_storage *storage, int block_logical_pointer, struct spatio_temporal_range_predicate *predicate) {
    int result_size = 0;
    int total_size = 0;
    /*for (int i = 0; i <= storage->current_index; i++) {
        struct seg_meta_section_entry *entry = storage->base[i];
        if (entry->block_logical_adr == block_logical_pointer) {
            //printf("index: %d, real value: %d\n", i, block_logical_pointer);
            struct seg_meta meta_array[entry->seg_meta_count];
            parse_seg_meta_section(entry->seg_meta_section, meta_array, entry->seg_meta_count);
            for (int j = 0; j < entry->seg_meta_count; j++) {
                struct seg_meta meta_item = meta_array[j];
                total_size += meta_item.seg_size;
                if (predicate->time_min <= meta_item.time_max && predicate->time_max >= meta_item.time_min
                    && predicate->lon_min <= meta_item.lon_max && predicate->lon_max >= meta_item.lon_min
                    && predicate->lat_min <= meta_item.lat_max && predicate->lat_max >= meta_item.lat_min) {
                    result_size += meta_item.seg_size;
                }
            }
        }
    }*/
    //for (int i = 0; i <= storage->current_index; i++) {
        struct seg_meta_section_entry *entry = storage->base[block_logical_pointer];
        if (entry->block_logical_adr == block_logical_pointer) {
            //printf("index: %d, real value: %d\n", i, block_logical_pointer);
            struct seg_meta meta_array[entry->seg_meta_count];
            parse_seg_meta_section(entry->seg_meta_section, meta_array, entry->seg_meta_count);
            for (int j = 0; j < entry->seg_meta_count; j++) {
                struct seg_meta meta_item = meta_array[j];
                total_size += meta_item.seg_size;
                if (predicate->time_min <= meta_item.time_max && predicate->time_max >= meta_item.time_min
                    && predicate->lon_min <= meta_item.lon_max && predicate->lon_max >= meta_item.lon_min
                    && predicate->lat_min <= meta_item.lat_max && predicate->lat_max >= meta_item.lat_min) {
                    result_size += meta_item.seg_size;
                }
            }
        } else {
            printf("please use the iteration way\n");
        }
    //}
    return 1.0 * result_size / total_size;
}

int spatio_temporal_query_without_pushdown(struct simple_query_engine *engine, struct spatio_temporal_range_predicate *predicate, bool enable_host_index) {

    struct index_entry_storage *index_storage = &engine->index_storage;
    struct traj_storage *data_storage = &engine->data_storage;
    struct seg_meta_section_entry_storage *meta_storage = &engine->seg_meta_storage;

    // calculate the matched block num
    int block_logical_addr_count = 0;
    for (int i = 0; i <= index_storage->current_index; i++) {
        struct index_entry *entry = index_storage->index_entry_base[i];
        if (enable_host_index) {
            if (predicate->lon_min <= entry->lon_max
                && predicate->lon_max >= entry->lon_min
                && predicate->lat_min <= entry->lat_max
                && predicate->lat_max >= entry->lat_min
                && predicate->time_min <= entry->time_max
                && predicate->time_max >= entry->time_min) {
                block_logical_addr_count++;
            }
        } else {
            block_logical_addr_count++;
        }
    }

    if (block_logical_addr_count == 0) {
        return 0;
    }

    // get the match block id vector
    int block_logical_addr_vec[block_logical_addr_count];
    int addr_vec_index = 0;
    for (int i = 0; i <= index_storage->current_index; i++) {
        struct index_entry *entry = index_storage->index_entry_base[i];
        if (enable_host_index) {
            if (predicate->lon_min <= entry->lon_max
                && predicate->lon_max >= entry->lon_min
                && predicate->lat_min <= entry->lat_max
                && predicate->lat_max >= entry->lat_min
                && predicate->time_min <= entry->time_max
                && predicate->time_max >= entry->time_min) {
                block_logical_addr_vec[addr_vec_index] = entry->block_logical_adr;
                addr_vec_index++;
            }
        } else {
            block_logical_addr_vec[addr_vec_index] = entry->block_logical_adr;
            addr_vec_index++;
        }
    }


    printf("[host] read block num: %d\n", block_logical_addr_count);
    // run query
    int result_count = run_spatio_temporal_query_in_host(predicate, data_storage, block_logical_addr_count,
                                                         block_logical_addr_vec);

    return result_count;
}


int
run_spatio_temporal_query_in_host(struct spatio_temporal_range_predicate *predicate, struct traj_storage *data_storage,
                                  int block_logical_addr_count, int *block_logical_addr_vec) {
    clock_t start, start_config, end;
    clock_t start_read, end_read, pure_read;
    clock_t start_computation, end_computation, pure_computation;
    pure_read = 0;
    pure_computation = 0;
    start_config = clock();
    int result_count = 0;
    int aggregated_block_vec_size = calculate_aggregate_block_vec_size(block_logical_addr_vec, block_logical_addr_count);
    struct continuous_block_meta aggregated_block_vec[aggregated_block_vec_size];
    aggregate_blocks(block_logical_addr_vec, block_logical_addr_count, aggregated_block_vec, aggregated_block_vec_size);

    fill_points_buffer(points_buffer_size);


    start = clock();
    /*for (int i = 0; i < aggregated_block_vec_size; i++) {
        struct continuous_block_meta block_meta = aggregated_block_vec[i];

        char *buffer = malloc(block_meta.block_count * TRAJ_BLOCK_SIZE);
        if (buffer == NULL) {
            printf("[error] simple query engine: cannot allocate memory\n");
            return -1;
        }
        start_read = clock();
        fetch_continuous_traj_data_block(data_storage, block_meta.block_start, block_meta.block_count, buffer);
        end_read = clock();
        pure_read += (end_read - start_read);
        for (int index = 0; index < block_meta.block_count; index++) {
            int count = spatio_temporal_query_raw_trajectory_block_without_meta_filtering(buffer + index * TRAJ_BLOCK_SIZE, predicate);
            result_count += count;
        }
        free(buffer);

    }*/

    //printf("aggregated block vec size: %d\n", aggregated_block_vec_size);
    // split aggregated block if its size is larger than 256
    for (int i = 0; i < aggregated_block_vec_size; i++) {
        struct continuous_block_meta block_meta = aggregated_block_vec[i];
        int block_count = block_meta.block_count;
        int block_start = block_meta.block_start;
        //printf("block start: %d, block count: %d\n", block_start, block_count);
        for (int j = 0; j < block_count; j+=256) {
            int current_block_count = block_count - j > 256 ? 256 : block_count - j;
            int current_block_start = block_start + j;
            char *buffer = malloc(current_block_count * TRAJ_BLOCK_SIZE);
            memset(buffer, 0, current_block_count * TRAJ_BLOCK_SIZE);

            struct host_result_buffer result_buffer;
            init_result_buffer(&result_buffer, current_block_count * TRAJ_BLOCK_SIZE);

            start_read = clock();
            fetch_continuous_traj_data_block(data_storage, current_block_start, current_block_count, buffer);
            end_read = clock();
            pure_read += (end_read - start_read);
            for (int index = 0; index < current_block_count; index++) {
                start_computation = clock();
                //int count = spatio_temporal_query_raw_trajectory_block_with_result_buffer_without_meta_filtering(buffer + index * TRAJ_BLOCK_SIZE, predicate, &result_buffer);
                int count = spatio_temporal_query_raw_trajectory_block_without_meta_filtering(buffer + index * TRAJ_BLOCK_SIZE, predicate);
                //int count = spatio_temporal_query_raw_trajectory_block(buffer + index * TRAJ_BLOCK_SIZE, predicate);
                result_count += count;
                end_computation = clock();
                pure_computation += (end_computation - start_computation);
            }
            free(buffer);
            free_result_buffer(&result_buffer);
        }
        /*for (int j = 0; j < block_count; j++) {
            char *buffer = malloc( TRAJ_BLOCK_SIZE);
            start_read = clock();
            fetch_continuous_traj_data_block(data_storage, block_start + j, 1, buffer);
            end_read = clock();
            pure_read += (end_read - start_read);

            int count = spatio_temporal_query_raw_trajectory_block_without_meta_filtering(buffer, predicate);
            result_count += count;

            free(buffer);
        }*/
    }



    end = clock();
    free_points_buffer(points_buffer_size);
    printf("[host] pure read time: %f\n",(double)(pure_read));
    printf("[host] pure computation time: %f\n",(double)(pure_computation));
    printf("[host] pure read and computation time: %f\n",(double)(end-start));
    printf("[host] query time: %f\n",(double)(end-start_config));
    return result_count;
}

int spatio_temporal_query_with_full_pushdown(struct simple_query_engine *engine, struct spatio_temporal_range_predicate *predicate, bool enable_host_index) {

    struct index_entry_storage *index_storage = &engine->index_storage;
    struct traj_storage *data_storage = &engine->data_storage;
    struct seg_meta_section_entry_storage *meta_storage = &engine->seg_meta_storage;

    // calculate the matched block num
    int block_logical_addr_count = 0;
    for (int i = 0; i <= index_storage->current_index; i++) {
        struct index_entry *entry = index_storage->index_entry_base[i];
        if (enable_host_index) {
            if (predicate->lon_min <= entry->lon_max
                && predicate->lon_max >= entry->lon_min
                && predicate->lat_min <= entry->lat_max
                && predicate->lat_max >= entry->lat_min
                && predicate->time_min <= entry->time_max
                && predicate->time_max >= entry->time_min) {
                block_logical_addr_count++;
            }
        } else {
            block_logical_addr_count++;
        }
    }
    if (block_logical_addr_count == 0) {
        return 0;
    }

    // get the match block id vector
    int block_logical_addr_vec[block_logical_addr_count];
    int addr_vec_index = 0;
    for (int i = 0; i <= index_storage->current_index; i++) {
        struct index_entry *entry = index_storage->index_entry_base[i];
        if (enable_host_index) {
            if (predicate->lon_min <= entry->lon_max
                && predicate->lon_max >= entry->lon_min
                && predicate->lat_min <= entry->lat_max
                && predicate->lat_max >= entry->lat_min
                && predicate->time_min <= entry->time_max
                && predicate->time_max >= entry->time_min) {
                block_logical_addr_vec[addr_vec_index] = entry->block_logical_adr;
                addr_vec_index++;
            }
        } else {
            block_logical_addr_vec[addr_vec_index] = entry->block_logical_adr;
            addr_vec_index++;
        }
    }
    printf("[isp cpu] read block num: %d\n", block_logical_addr_count);
    // run query
    int result_count = run_spatio_temporal_query_device(predicate, data_storage, meta_storage, block_logical_addr_count,
                                                        block_logical_addr_vec, enable_estimated_result_size);
    return result_count;
}

int spatio_temporal_query_with_full_pushdown_fpga(struct simple_query_engine *engine, struct spatio_temporal_range_predicate *predicate, bool enable_host_index) {

    struct index_entry_storage *index_storage = &engine->index_storage;
    struct traj_storage *data_storage = &engine->data_storage;
    struct seg_meta_section_entry_storage *meta_storage = &engine->seg_meta_storage;

    // calculate the matched block num
    int block_logical_addr_count = 0;
    for (int i = 0; i <= index_storage->current_index; i++) {
        struct index_entry *entry = index_storage->index_entry_base[i];
        if (enable_host_index) {
            if (predicate->lon_min <= entry->lon_max
                && predicate->lon_max >= entry->lon_min
                && predicate->lat_min <= entry->lat_max
                && predicate->lat_max >= entry->lat_min
                && predicate->time_min <= entry->time_max
                && predicate->time_max >= entry->time_min) {
                block_logical_addr_count++;
            }
        } else {
            block_logical_addr_count++;
        }
    }
    if (block_logical_addr_count == 0) {
        return 0;
    }

    // get the match block id vector
    int block_logical_addr_vec[block_logical_addr_count];
    int addr_vec_index = 0;
    for (int i = 0; i <= index_storage->current_index; i++) {
        struct index_entry *entry = index_storage->index_entry_base[i];
        if (enable_host_index) {
            if (predicate->lon_min <= entry->lon_max
                && predicate->lon_max >= entry->lon_min
                && predicate->lat_min <= entry->lat_max
                && predicate->lat_max >= entry->lat_min
                && predicate->time_min <= entry->time_max
                && predicate->time_max >= entry->time_min) {
                block_logical_addr_vec[addr_vec_index] = entry->block_logical_adr;
                addr_vec_index++;
            }
        } else {
            block_logical_addr_vec[addr_vec_index] = entry->block_logical_adr;
            addr_vec_index++;
        }
    }

    printf("[isp fpga] read block num: %d\n", block_logical_addr_count);
    // run query
    int result_count = run_spatio_temporal_query_device_fpga(predicate, data_storage, meta_storage, block_logical_addr_count,
                                                        block_logical_addr_vec, enable_estimated_result_size);
    return result_count;
}

int
run_spatio_temporal_query_device(struct spatio_temporal_range_predicate *predicate, struct traj_storage *data_storage,
                                 struct seg_meta_section_entry_storage *meta_storage, int block_logical_addr_count,
                                 int *block_logical_addr_vec, bool enable_result_estimation) {
    int result_count = 0;

    clock_t start, end, pure_read;
    pure_read = 0;
    clock_t start_all, end_all;

    int total_result_block_num = 0;
    start_all = clock();
    fill_points_buffer(points_buffer_size);
    for (int i = 0; i < block_logical_addr_count; i += 256) {
        int block_addr_vec_size = block_logical_addr_count - i > 256 ? 256 : block_logical_addr_count - i;
        int estimated_result_size = block_addr_vec_size * 0x1000;
        if (enable_result_estimation) {
            estimated_result_size = estimate_spatio_temporal_result_size_for_blocks(meta_storage, predicate,
                                                                                    &block_logical_addr_vec[i],
                                                                                    block_addr_vec_size);
        }
        //printf("estimated result size: %d\n", estimated_result_size);
        if (estimated_result_size == 0) {
            continue;
        }
        if (estimated_result_size % 0x1000 != 0) {
            estimated_result_size = ((estimated_result_size / 0x1000) + 1) * 0x1000;
        }
        int estimated_result_block_num = estimated_result_size / 0x1000;
        //int estimated_result_block_num = 1;
        total_result_block_num += estimated_result_block_num;

        void* result_buffer = malloc(estimated_result_size);

        int block_meta_vec_size = calculate_aggregate_block_vec_size(&block_logical_addr_vec[i], block_addr_vec_size);
        struct continuous_block_meta block_meta_vec[block_meta_vec_size];
        aggregate_blocks(&block_logical_addr_vec[i], block_addr_vec_size, block_meta_vec, block_meta_vec_size);

        struct isp_descriptor isp_desc;
        struct lba lba_vec[block_meta_vec_size];
        assemble_lba_vec_via_block_meta(lba_vec, DATA_FILE_OFFSET, block_meta_vec, block_meta_vec_size);
        assemble_isp_desc_for_spatial_temporal(&isp_desc, predicate, estimated_result_block_num, lba_vec, block_meta_vec_size);

        start = clock();
        do_isp_for_trajectory_data(data_storage, result_buffer, estimated_result_size, &isp_desc, 0);
        end = clock();
        pure_read += (end - start);

        for (int block_count = 0; block_count < estimated_result_block_num; block_count++) {
            int count = spatio_temporal_query_isp_output_block(result_buffer + block_count * 4096, predicate);
            result_count += count;
        }

        free(result_buffer);

        //printf("pure read time [isp]: %f\n",(double)(end-start));
    }
    free_points_buffer(points_buffer_size);
    end_all = clock();
    printf("[isp cpu] estimated result block number: %d\n", total_result_block_num);
    printf("[isp cpu] pure read time: %f\n",(double)pure_read);
    printf("[isp cpu] query time (including estimation): %f\n", (double)(end_all - start_all));


    return result_count;
}

int
run_spatio_temporal_query_device_fpga(struct spatio_temporal_range_predicate *predicate, struct traj_storage *data_storage,
                                 struct seg_meta_section_entry_storage *meta_storage, int block_logical_addr_count,
                                 int *block_logical_addr_vec, bool enable_result_estimation) {
    int result_count = 0;

    clock_t start, end, pure_read;
    pure_read = 0;
    clock_t start_all, end_all;
    int total_result_block_num = 0;
    start_all = clock();
    fill_points_buffer(points_buffer_size);
    for (int i = 0; i < block_logical_addr_count; i += 256) {
        int block_addr_vec_size = block_logical_addr_count - i > 256 ? 256 : block_logical_addr_count - i;
        int estimated_result_size = block_addr_vec_size * 0x1000;
        if (enable_result_estimation) {
            estimated_result_size = estimate_spatio_temporal_result_size_for_blocks(meta_storage, predicate,
                                                                                    &block_logical_addr_vec[i],
                                                                                    block_addr_vec_size);
        }
        //printf("estimated result size: %d\n", estimated_result_size);
        if (estimated_result_size == 0) {
            continue;
        }
        if (estimated_result_size % 0x1000 != 0) {
            estimated_result_size = ((estimated_result_size / 0x1000) + 1) * 0x1000;
        }
        int estimated_result_block_num = estimated_result_size / 0x1000;
        //int estimated_result_block_num = 1;
        total_result_block_num += estimated_result_block_num;


        void* result_buffer = malloc(estimated_result_size);

        int block_meta_vec_size = calculate_aggregate_block_vec_size(&block_logical_addr_vec[i], block_addr_vec_size);
        struct continuous_block_meta block_meta_vec[block_meta_vec_size];
        aggregate_blocks(&block_logical_addr_vec[i], block_addr_vec_size, block_meta_vec, block_meta_vec_size);

        struct isp_descriptor isp_desc;
        struct lba lba_vec[block_meta_vec_size];
        assemble_lba_vec_via_block_meta(lba_vec, DATA_FILE_OFFSET, block_meta_vec, block_meta_vec_size);
        assemble_isp_desc_for_spatial_temporal(&isp_desc, predicate, estimated_result_block_num, lba_vec, block_meta_vec_size);

        start = clock();
        do_isp_for_trajectory_data(data_storage, result_buffer, estimated_result_size, &isp_desc, 1);
        end = clock();
        pure_read += (end - start);
        result_count += spatio_temporal_query_isp_fpga_output_blocks(result_buffer, predicate);

        free(result_buffer);


    }
    free_points_buffer(points_buffer_size);
    end_all = clock();
    printf("[isp fpga] estimated result block number: %d\n", total_result_block_num);
    printf("[isp fpga] pure read time: %f\n",(double)pure_read);
    printf("[isp fpga] query time (including estimation): %f\n", (double)(end_all - start_all));

    return result_count;
}


int spatio_temporal_query_with_adaptive_pushdown(struct simple_query_engine *engine, struct spatio_temporal_range_predicate *predicate, bool enable_host_index) {

    struct index_entry_storage *index_storage = &engine->index_storage;
    struct traj_storage *data_storage = &engine->data_storage;
    struct seg_meta_section_entry_storage *meta_storage = &engine->seg_meta_storage;

    // calculate the matched block num
    int block_logical_addr_count = 0;
    for (int i = 0; i <= index_storage->current_index; i++) {
        struct index_entry *entry = index_storage->index_entry_base[i];
        if (enable_host_index) {
            if (predicate->lon_min <= entry->lon_max
                && predicate->lon_max >= entry->lon_min
                && predicate->lat_min <= entry->lat_max
                && predicate->lat_max >= entry->lat_min
                && predicate->time_min <= entry->time_max
                && predicate->time_max >= entry->time_min) {
                block_logical_addr_count++;
            }
        } else {
            block_logical_addr_count++;
        }
    }
    if (block_logical_addr_count == 0) {
        return 0;
    }

    // get the match block id vector
    int block_logical_addr_vec[block_logical_addr_count];
    int addr_vec_index = 0;
    for (int i = 0; i <= index_storage->current_index; i++) {
        struct index_entry *entry = index_storage->index_entry_base[i];
        if (enable_host_index) {
            if (predicate->lon_min <= entry->lon_max
                && predicate->lon_max >= entry->lon_min
                && predicate->lat_min <= entry->lat_max
                && predicate->lat_max >= entry->lat_min
                && predicate->time_min <= entry->time_max
                && predicate->time_max >= entry->time_min) {
                block_logical_addr_vec[addr_vec_index] = entry->block_logical_adr;
                addr_vec_index++;
            }
        } else {
            block_logical_addr_vec[addr_vec_index] = entry->block_logical_adr;
            addr_vec_index++;
        }
    }

    // separate block id vector according to the goodness
    bool flag_for_pushdown[block_logical_addr_count];
    int pushdown_block_num = 0;
    for (int i = 0; i < block_logical_addr_count; i++) {
        if (calculate_goodness_for_spatio_temporal(meta_storage, block_logical_addr_vec[i], predicate) < 0.3) {
            flag_for_pushdown[i] = true;
            pushdown_block_num++;
        } else {
            flag_for_pushdown[i] = false;
        }
    }

    int *blocks_for_pushdown = malloc(pushdown_block_num * sizeof(int));
    int pushdown_index = 0;
    int host_block_num = block_logical_addr_count - pushdown_block_num;
    int *blocks_for_host = malloc(host_block_num * sizeof(int));
    int host_index = 0;
    for (int i = 0; i < block_logical_addr_count; i++) {
        if (flag_for_pushdown[i]) {
            blocks_for_pushdown[pushdown_index] = block_logical_addr_vec[i];
            pushdown_index++;
        } else {
            blocks_for_host[host_index] = block_logical_addr_vec[i];
            host_index++;
        }
    }

    // run query
    printf("[isp adaptive] host block num: %d, device block num: %d\n", host_block_num, pushdown_block_num);
    int result_count1 = run_spatio_temporal_query_in_host(predicate, data_storage, host_block_num, blocks_for_host);
    int result_count2 = run_spatio_temporal_query_device_fpga(predicate, data_storage, meta_storage, pushdown_block_num, blocks_for_pushdown, enable_estimated_result_size);

    free(blocks_for_pushdown);
    free(blocks_for_host);
    return result_count1 + result_count2;
}


int id_temporal_query_with_full_pushdown(struct simple_query_engine *engine, struct id_temporal_predicate *predicate, bool enable_host_index) {
    struct index_entry_storage *index_storage = &engine->index_storage;
    struct traj_storage *data_storage = &engine->data_storage;
    struct seg_meta_section_entry_storage *meta_storage = &engine->seg_meta_storage;

    // calculate the matched block num
    int block_logical_addr_count = 0;
    for (int i = 0; i <= index_storage->current_index; i++) {
        struct index_entry *entry = index_storage->index_entry_base[i];
        if (enable_host_index) {
            if (check_oid_exist(entry->oid_array, entry->oid_array_size, predicate->oid) > 0
                && predicate->time_min <= entry->time_max
                && predicate->time_max >= entry->time_min) {
                block_logical_addr_count++;
            }
        } else {
            block_logical_addr_count++;
        }
    }
    if (block_logical_addr_count == 0) {
        return 0;
    }

    // get the match block id vector
    int block_logical_addr_vec[block_logical_addr_count];
    int addr_vec_index = 0;
    for (int i = 0; i <= index_storage->current_index; i++) {
        struct index_entry *entry = index_storage->index_entry_base[i];
        if (enable_host_index) {
            if (check_oid_exist(entry->oid_array, entry->oid_array_size, predicate->oid) > 0
                && predicate->time_min <= entry->time_max
                && predicate->time_max >= entry->time_min) {
                block_logical_addr_vec[addr_vec_index] = entry->block_logical_adr;
                addr_vec_index++;
            }
        } else {
            block_logical_addr_vec[addr_vec_index] = entry->block_logical_adr;
            addr_vec_index++;
        }
    }
    printf("[isp cpu] id temporal read block num: %d\n", block_logical_addr_count);
    // run query
    int result_count = run_id_temporal_query_device(predicate, data_storage, meta_storage, block_logical_addr_count,
                                                        block_logical_addr_vec, enable_estimated_result_size);
    return result_count;
}

int run_id_temporal_query_device(struct id_temporal_predicate *predicate, struct traj_storage *data_storage,
                                 struct seg_meta_section_entry_storage *meta_storage, int block_logical_addr_count,
                                 int *block_logical_addr_vec, bool enable_result_estimation) {
    int result_count = 0;

    clock_t start, end, pure_read;
    pure_read = 0;
    clock_t start_all, end_all;

    int total_result_block_num = 0;
    start_all = clock();
    fill_points_buffer(points_buffer_size);
    for (int i = 0; i < block_logical_addr_count; i += 256) {
        int block_addr_vec_size = block_logical_addr_count - i > 256 ? 256 : block_logical_addr_count - i;
        int estimated_result_size = block_addr_vec_size * 0x1000;
        if (enable_result_estimation) {
            estimated_result_size = estimate_id_temporal_result_size_for_blocks(meta_storage, predicate,
                                                                                    &block_logical_addr_vec[i],
                                                                                    block_addr_vec_size);
        }
        //printf("estimated result size: %d\n", estimated_result_size);
        if (estimated_result_size == 0) {
            continue;
        }
        if (estimated_result_size % 0x1000 != 0) {
            estimated_result_size = ((estimated_result_size / 0x1000) + 1) * 0x1000;
        }
        int estimated_result_block_num = estimated_result_size / 0x1000;
        //int estimated_result_block_num = 1;
        total_result_block_num += estimated_result_block_num;

        void* result_buffer = malloc(estimated_result_size);

        int block_meta_vec_size = calculate_aggregate_block_vec_size(&block_logical_addr_vec[i], block_addr_vec_size);
        struct continuous_block_meta block_meta_vec[block_meta_vec_size];
        aggregate_blocks(&block_logical_addr_vec[i], block_addr_vec_size, block_meta_vec, block_meta_vec_size);

        struct isp_descriptor isp_desc;
        struct lba lba_vec[block_meta_vec_size];
        assemble_lba_vec_via_block_meta(lba_vec, DATA_FILE_OFFSET, block_meta_vec, block_meta_vec_size);
        assemble_isp_desc_for_id_temporal(&isp_desc, predicate, estimated_result_block_num, lba_vec, block_meta_vec_size);

        start = clock();
        do_isp_for_trajectory_data(data_storage, result_buffer, estimated_result_size, &isp_desc, 0);
        end = clock();
        pure_read += (end - start);

        for (int block_count = 0; block_count < estimated_result_block_num; block_count++) {
            int count = id_temporal_query_isp_output_block(result_buffer + block_count * 4096, predicate);
            result_count += count;
        }

        free(result_buffer);

        //printf("pure read time [isp]: %f\n",(double)(end-start));
    }
    free_points_buffer(points_buffer_size);
    end_all = clock();
    printf("[isp cpu] id temporal estimated result block number: %d\n", total_result_block_num);
    printf("[isp cpu] id temporal pure read time: %f\n",(double)pure_read);
    printf("[isp cpu] id temporal query time (including estimation): %f\n", (double)(end_all - start_all));


    return result_count;
}

int id_temporal_query_with_full_pushdown_fpga(struct simple_query_engine *engine, struct id_temporal_predicate *predicate, bool enable_host_index) {

    struct index_entry_storage *index_storage = &engine->index_storage;
    struct traj_storage *data_storage = &engine->data_storage;
    struct seg_meta_section_entry_storage *meta_storage = &engine->seg_meta_storage;

    // calculate the matched block num
    int block_logical_addr_count = 0;
    for (int i = 0; i <= index_storage->current_index; i++) {
        struct index_entry *entry = index_storage->index_entry_base[i];
        if (enable_host_index) {
            if (check_oid_exist(entry->oid_array, entry->oid_array_size, predicate->oid) > 0
                && predicate->time_min <= entry->time_max
                && predicate->time_max >= entry->time_min) {
                block_logical_addr_count++;
            }
        } else {
            block_logical_addr_count++;
        }
    }
    if (block_logical_addr_count == 0) {
        return 0;
    }

    // get the match block id vector
    int block_logical_addr_vec[block_logical_addr_count];
    int addr_vec_index = 0;
    for (int i = 0; i <= index_storage->current_index; i++) {
        struct index_entry *entry = index_storage->index_entry_base[i];
        if (enable_host_index) {
            if (check_oid_exist(entry->oid_array, entry->oid_array_size, predicate->oid) > 0
                && predicate->time_min <= entry->time_max
                && predicate->time_max >= entry->time_min) {
                block_logical_addr_vec[addr_vec_index] = entry->block_logical_adr;
                addr_vec_index++;
            }
        } else {
            block_logical_addr_vec[addr_vec_index] = entry->block_logical_adr;
            addr_vec_index++;
        }
    }

    printf("[isp fpga] id temporal read block num: %d\n", block_logical_addr_count);
    // run query
    int result_count = run_id_temporal_query_device_fpga(predicate, data_storage, meta_storage, block_logical_addr_count,
                                                             block_logical_addr_vec, enable_estimated_result_size);
    return result_count;
}

int
run_id_temporal_query_device_fpga(struct id_temporal_predicate *predicate, struct traj_storage *data_storage,
                                      struct seg_meta_section_entry_storage *meta_storage, int block_logical_addr_count,
                                      int *block_logical_addr_vec, bool enable_result_estimation) {
    int result_count = 0;

    clock_t start, end, pure_read;
    pure_read = 0;
    clock_t start_all, end_all;
    int total_result_block_num = 0;
    start_all = clock();
    fill_points_buffer(points_buffer_size);
    for (int i = 0; i < block_logical_addr_count; i += 256) {
        int block_addr_vec_size = block_logical_addr_count - i > 256 ? 256 : block_logical_addr_count - i;
        int estimated_result_size = block_addr_vec_size * 0x1000;
        if (enable_result_estimation) {
            estimated_result_size = estimate_id_temporal_result_size_for_blocks(meta_storage, predicate,
                                                                                    &block_logical_addr_vec[i],
                                                                                    block_addr_vec_size);
        }
        //printf("estimated result size: %d\n", estimated_result_size);
        if (estimated_result_size == 0) {
            continue;
        }
        if (estimated_result_size % 0x1000 != 0) {
            estimated_result_size = ((estimated_result_size / 0x1000) + 1) * 0x1000;
        }
        int estimated_result_block_num = estimated_result_size / 0x1000;
        //int estimated_result_block_num = 1;
        total_result_block_num += estimated_result_block_num;


        void* result_buffer = malloc(estimated_result_size);

        int block_meta_vec_size = calculate_aggregate_block_vec_size(&block_logical_addr_vec[i], block_addr_vec_size);
        struct continuous_block_meta block_meta_vec[block_meta_vec_size];
        aggregate_blocks(&block_logical_addr_vec[i], block_addr_vec_size, block_meta_vec, block_meta_vec_size);

        struct isp_descriptor isp_desc;
        struct lba lba_vec[block_meta_vec_size];
        assemble_lba_vec_via_block_meta(lba_vec, DATA_FILE_OFFSET, block_meta_vec, block_meta_vec_size);
        assemble_isp_desc_for_id_temporal(&isp_desc, predicate, estimated_result_block_num, lba_vec, block_meta_vec_size);

        start = clock();
        do_isp_for_trajectory_data(data_storage, result_buffer, estimated_result_size, &isp_desc, 1);
        end = clock();
        pure_read += (end - start);
        result_count += id_temporal_query_isp_fpga_output_blocks(result_buffer, predicate);

        free(result_buffer);


    }
    free_points_buffer(points_buffer_size);
    end_all = clock();
    printf("[isp fpga] id temporal estimated result block number: %d\n", total_result_block_num);
    printf("[isp fpga] id temporal pure read time: %f\n",(double)pure_read);
    printf("[isp fpga] id temporal query time (including estimation): %f\n", (double)(end_all - start_all));

    return result_count;
}

int id_temporal_query_without_pushdown(struct simple_query_engine *engine, struct id_temporal_predicate *predicate, bool enable_host_index) {

    struct index_entry_storage *index_storage = &engine->index_storage;
    struct traj_storage *data_storage = &engine->data_storage;
    struct seg_meta_section_entry_storage *meta_storage = &engine->seg_meta_storage;

    // calculate the matched block num
    // calculate the matched block num
    int block_logical_addr_count = 0;
    for (int i = 0; i <= index_storage->current_index; i++) {
        struct index_entry *entry = index_storage->index_entry_base[i];
        if (enable_host_index) {
            if (check_oid_exist(entry->oid_array, entry->oid_array_size, predicate->oid) > 0
                && predicate->time_min <= entry->time_max
                && predicate->time_max >= entry->time_min) {
                block_logical_addr_count++;
            }
        } else {
            block_logical_addr_count++;
        }
    }
    if (block_logical_addr_count == 0) {
        return 0;
    }

    // get the match block id vector
    int block_logical_addr_vec[block_logical_addr_count];
    int addr_vec_index = 0;
    for (int i = 0; i <= index_storage->current_index; i++) {
        struct index_entry *entry = index_storage->index_entry_base[i];
        if (enable_host_index) {
            if (check_oid_exist(entry->oid_array, entry->oid_array_size, predicate->oid) > 0
                && predicate->time_min <= entry->time_max
                && predicate->time_max >= entry->time_min) {
                block_logical_addr_vec[addr_vec_index] = entry->block_logical_adr;
                addr_vec_index++;
            }
        } else {
            block_logical_addr_vec[addr_vec_index] = entry->block_logical_adr;
            addr_vec_index++;
        }
    }
    printf("[host] id temporal read block num: %d\n", block_logical_addr_count);
    // run query
    int result_count = run_id_temporal_query_in_host(predicate, data_storage, block_logical_addr_count,
                                                     block_logical_addr_vec);

    return result_count;
}

int
run_id_temporal_query_in_host(struct id_temporal_predicate *predicate, struct traj_storage *data_storage,
                                  int block_logical_addr_count, int *block_logical_addr_vec) {
    clock_t start, start_config, end;
    clock_t start_read, end_read, pure_read;
    clock_t start_computation, end_computation, pure_computation;
    pure_computation = 0;
    pure_read = 0;
    start_config = clock();
    int result_count = 0;
    int aggregated_block_vec_size = calculate_aggregate_block_vec_size(block_logical_addr_vec, block_logical_addr_count);
    struct continuous_block_meta aggregated_block_vec[aggregated_block_vec_size];
    aggregate_blocks(block_logical_addr_vec, block_logical_addr_count, aggregated_block_vec, aggregated_block_vec_size);

    fill_points_buffer(points_buffer_size);

    start = clock();


    // split aggregated block if its size is larger than 256
    for (int i = 0; i < aggregated_block_vec_size; i++) {
        struct continuous_block_meta block_meta = aggregated_block_vec[i];
        int block_count = block_meta.block_count;
        int block_start = block_meta.block_start;
        for (int j = 0; j < block_count; j+=256) {
            int current_block_count = block_count - j > 256 ? 256 : block_count - j;
            int current_block_start = block_start + j;
            char *buffer = malloc(current_block_count * TRAJ_BLOCK_SIZE);
            start_read = clock();
            fetch_continuous_traj_data_block(data_storage, current_block_start, current_block_count, buffer);
            end_read = clock();
            pure_read += (end_read - start_read);
            for (int index = 0; index < current_block_count; index++) {
                start_computation = clock();
                int count = id_temporal_query_raw_trajectory_block_without_meta_filtering(buffer + index * TRAJ_BLOCK_SIZE, predicate);
                //int count = id_temporal_query_raw_trajectory_block(buffer + index * TRAJ_BLOCK_SIZE, predicate);
                result_count += count;
                end_computation = clock();
                pure_computation += (end_computation - start_computation);
            }
            free(buffer);
        }

    }



    end = clock();
    free_points_buffer(points_buffer_size);
    printf("[host] pure read time: %f\n",(double)(pure_read));
    printf("[host] pure computation time: %f\n",(double)(pure_computation));
    printf("[host] pure read and computation time: %f\n",(double)(end-start));
    printf("[host] query time: %f\n",(double)(end-start_config));
    return result_count;
}