//
// Created by yangguo on 11/16/22.
//

#include "simple_index.h"
#include <stdlib.h>
#include <limits.h>
#include "hashset_int.h"

void init_index_entry(struct index_entry *entry) {
    entry->oid_array_size = -1;
    entry->oid_array = NULL;
    entry->lon_min = INT_MAX;
    entry->lon_max = 0;
    entry->lat_min = INT_MAX;
    entry->lat_max = 0;
    entry->time_min = INT_MAX;
    entry->time_max = 0;
    entry->block_logical_adr = -1;
    entry->block_physical_ptr = NULL;
}

void free_index_entry(struct index_entry *entry) {
    if (entry->block_physical_ptr != NULL) {
        free(entry->block_physical_ptr);
        entry->block_physical_ptr = NULL;
        free(entry->oid_array);
        entry->oid_array = NULL;
    }
    entry->block_logical_adr = -1;
}

void fill_index_entry(struct index_entry *entry, struct traj_point **points, int points_num, void* block_physical_ptr, int block_logical_adr) {

    struct hashset_int *oids = NULL;    // calculate the unique num of oid

    struct traj_point *point;
    for (int i = 0; i < points_num; i++) {
        point = points[i];
        if (point->normalized_longitude > entry->lon_max) {
            entry->lon_max = point->normalized_longitude;
        }
        if (point->normalized_longitude < entry->lon_min) {
            entry->lon_min = point->normalized_longitude;
        }
        if (point->normalized_latitude > entry->lat_max) {
            entry->lat_max = point->normalized_latitude;
        }
        if (point->normalized_latitude < entry->lat_min) {
            entry->lat_min = point->normalized_latitude;
        }
        if (point->timestamp_sec > entry->time_max) {
            entry->time_max = point->timestamp_sec;
        }
        if (point->timestamp_sec < entry->time_min) {
            entry->time_min = point->timestamp_sec;
        }
        add_key(&oids, point->oid, 0);
    }
    entry->block_physical_ptr = block_physical_ptr;
    entry->block_logical_adr = block_logical_adr;

    int unique_oid_num = hash_count(&oids);
    entry->oid_array = (int *) malloc(unique_oid_num * sizeof(int));
    entry->oid_array_size = unique_oid_num;
    get_key_set(&oids, entry->oid_array);
}

void init_index_entry_storage(struct index_entry_storage *storage) {
    storage->current_index = 0;
    storage->total_size = 1000;
    storage->index_entry_base = malloc(1000 * sizeof(struct index_entry));
}

void free_index_entry_storage(struct index_entry_storage *storage) {
    for (int i = 0 ; i <= storage->current_index; i++) {
        free_index_entry(storage->index_entry_base[i]);
        free(storage->index_entry_base[i]);
        storage->index_entry_base[i] = NULL;
    }
}

void append_index_entry_to_storage(struct index_entry_storage *storage, struct index_entry *entry) {
    if (storage->current_index < storage->total_size) {
        (storage->index_entry_base)[storage->current_index] = entry;
        storage->current_index++;
    } else {
        // we need to extend array
        int new_total_size = storage->total_size * 2;
        struct index_entry **tmp_base = malloc(new_total_size * sizeof(void*));
        for (int i = 0; i < storage->total_size; i++) {
            tmp_base[i] = (storage->index_entry_base)[i];
        }
        free(storage->index_entry_base);
        storage->index_entry_base = tmp_base;

        (storage->index_entry_base)[storage->current_index] = entry;
        storage->current_index++;
    }
}
