//
// Created by yangguo on 11/16/22.
//
#include "simple_storage.h"
#include "log.h"
#include <stdlib.h>

void init_traj_storage(struct traj_storage *storage) {
    storage->current_index = -1;
    storage->total_size = 1000; // initial capacity is 1000
    storage->traj_blocks_base = malloc(1000 * sizeof(void*));
}

struct address_pair append_traj_block_to_storage(struct traj_storage *storage, void *block_ptr) {
    storage->current_index++;

    struct address_pair addresses;
    addresses.physical_ptr = block_ptr;
    addresses.logical_adr = storage->current_index;
    if (storage->current_index < storage->total_size) {
        (storage->traj_blocks_base)[storage->current_index] = block_ptr;
        //storage->current_index++;
    } else {
        // we need to extend array
        int new_total_size = storage->total_size * 2;
        debug_print("extend data storage from size %d to size %d\n", storage->total_size, new_total_size);
        void **tmp_base = malloc(new_total_size * sizeof(void*));
        for (int i = 0; i < storage->total_size; i++) {
            tmp_base[i] = (storage->traj_blocks_base)[i];
        }
        free(storage->traj_blocks_base);
        storage->traj_blocks_base = tmp_base;

        (storage->traj_blocks_base)[storage->current_index] = block_ptr;
        //storage->current_index++;
        storage->total_size = new_total_size;
    }
    return addresses;
}

void* fetch_traj_data_via_logical_pointer(struct traj_storage *storage, int logical_pointer) {

}

void free_traj_storage(struct traj_storage *storage) {
    for (int i = 0; i <= storage->current_index; i++) {
        void* block_ptr = (storage->traj_blocks_base)[i];
        free(block_ptr);
        (storage->traj_blocks_base)[i] = NULL;
    }
    free(storage->traj_blocks_base);
}
