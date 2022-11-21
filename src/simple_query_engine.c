//
// Created by yangguo on 11/16/22.
//

#include "simple_query_engine.h"
#include "porto_dataset_reader.h"
#include "traj_block_format.h"

#include <stdlib.h>

#define TRAJ_BLOCK_SIZE 4096    // 4 KB

static int check_oid_exist(const int *oids, int array_size, int checked_oid) {
    for (int i = 0; i < array_size; i++) {
        if (oids[i] == checked_oid) {
            return 1;
        }
    }
    return -1;
}

void init_query_engine(struct simple_query_engine *engine) {
    init_index_entry_storage(&(engine->index_storage));
    init_traj_storage(&(engine->data_storage));
}

void free_query_engine(struct simple_query_engine *engine) {
    free_index_entry_storage(&(engine->index_storage));
    free_traj_storage(&(engine->data_storage));
}

void ingest_data_via_time_partition(struct simple_query_engine *engine, FILE *fp, int block_num) {
    struct traj_storage data_storage = engine->data_storage;
    struct index_entry_storage index_storage = engine->index_storage;
    // trajectory block info
    int points_num = calculate_points_num_via_block_size(TRAJ_BLOCK_SIZE, 8);

    for (int i = 0; i < block_num; i++) {
        struct traj_point **points = allocate_points_memory(points_num);
        read_points_from_csv(fp,points, i*points_num, points_num);
        // convert and put this data to traj storage
        void *data = malloc(TRAJ_BLOCK_SIZE);
        convert_struct_to_serialized_traj(points, data, points_num);
        struct address_pair data_addresses = append_traj_block_to_storage(&data_storage, data);
        // update index
        struct index_entry *entry = malloc(sizeof(struct index_entry));
        init_index_entry(entry);
        fill_index_entry(entry, points, points_num, data_addresses.physical_ptr, data_addresses.logical_adr);
        append_index_entry_to_storage(&index_storage, entry);
    }

}

void id_temporal_query(struct simple_query_engine *engine, struct id_temporal_predicate *predicate) {
    // check the index
    struct index_entry_storage index_storage = engine->index_storage;
    struct traj_storage data_storage = engine->data_storage;
    for (int i = 0; i <= index_storage.current_index; i++) {
        struct index_entry *entry = index_storage.index_entry_base[i];
        if (check_oid_exist(entry->oid_array, entry->oid_array_size, predicate->oid) > 0
        && predicate->time_min <= entry->time_max
        && predicate->time_max >= entry->time_min) {

        }
    }

}

void spatio_temporal_query(struct simple_query_engine *engine, struct spatio_temporal_range_predicate *predicate) {

}


