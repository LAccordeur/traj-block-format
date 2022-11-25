//
// Created by yangguo on 11/16/22.
//

#ifndef TRAJ_BLOCK_FORMAT_SIMPLE_INDEX_H
#define TRAJ_BLOCK_FORMAT_SIMPLE_INDEX_H
#include "traj_block_format.h"

struct index_entry {
    int oid_array_size;
    int *oid_array;
    int lon_min;
    int lon_max;
    int lat_min;
    int lat_max;
    int time_min;
    int time_max;
    int block_logical_adr;
    void *block_physical_ptr;
};

struct index_entry_storage {
    struct index_entry **index_entry_base;
    int current_index;
    int total_size;
};

struct serialized_index_storage {
    void **index_block_base;
    int current_index;
    int total_size;
};

void serialize_index_entry(struct index_entry *source, void *destination);

void deserialize_index_entry(void* source, struct index_entry *destination);

void init_serialized_index_storage(struct serialized_index_storage *storage);

void free_serialized_index_storage(struct serialized_index_storage *storage);

void append_serialized_index_block_to_storage(struct serialized_index_storage *storage, void *block);

void serialize_index_entry_storage(struct index_entry_storage *entry_storage, struct serialized_index_storage *serialized_storage);

void deserialize_index_entry_storage(struct serialized_index_storage *serialized_storage, struct index_entry_storage *entry_storage);

void init_index_entry_storage(struct index_entry_storage *storage);

void append_index_entry_to_storage(struct index_entry_storage *storage, struct index_entry *entry);

void free_index_entry_storage(struct index_entry_storage *storage);

void init_index_entry(struct index_entry *entry);

void free_index_entry(struct index_entry *entry);

void fill_index_entry(struct index_entry *entry, struct traj_point **points, int points_num, void* block_physical_ptr, int block_logical_adr);

#endif //TRAJ_BLOCK_FORMAT_SIMPLE_INDEX_H
