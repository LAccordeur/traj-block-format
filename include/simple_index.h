//
// Created by yangguo on 11/16/22.
//

#ifndef TRAJ_BLOCK_FORMAT_SIMPLE_INDEX_H
#define TRAJ_BLOCK_FORMAT_SIMPLE_INDEX_H
#include "traj_block_format.h"

struct index_entry {
    int *oid_array;
    int oid_array_size;
    int lon_min;
    int lon_max;
    int lat_min;
    int lat_max;
    int time_min;
    int time_max;
    void *block_physical_ptr;
    int block_logical_adr;
};

struct index_entry_storage {
    struct index_entry **index_entry_base;
    int current_index;
    int total_size;
};

void init_index_entry_storage(struct index_entry_storage *storage);

void append_index_entry_to_storage(struct index_entry_storage *storage, struct index_entry *entry);

void free_index_entry_storage(struct index_entry_storage *storage);

void init_index_entry(struct index_entry *entry);

void free_index_entry(struct index_entry *entry);

void fill_index_entry(struct index_entry *entry, struct traj_point **points, int points_num, void* block_physical_ptr, int block_logical_adr);

#endif //TRAJ_BLOCK_FORMAT_SIMPLE_INDEX_H
