//
// Created by yangguo on 11/16/22.
//

#ifndef TRAJ_BLOCK_FORMAT_SIMPLE_STORAGE_H
#define TRAJ_BLOCK_FORMAT_SIMPLE_STORAGE_H

struct traj_storage {
    void **traj_blocks_base;
    int current_index;
    int total_size;
};

struct address_pair{
    int logical_adr;
    void* physical_ptr;
};

void init_traj_storage(struct traj_storage *storage);

struct address_pair append_traj_block_to_storage(struct traj_storage *storage, void *block_ptr);

void free_traj_storage(struct traj_storage *storage);

void* fetch_traj_data_via_logical_pointer(struct traj_storage *storage, int logical_pointer);

#endif //TRAJ_BLOCK_FORMAT_SIMPLE_STORAGE_H
