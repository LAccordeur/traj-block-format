//
// Created by yangguo on 11/29/22.
//
// we maintain a copy of seg meta to estimate the size of return result

#ifndef TRAJ_BLOCK_FORMAT_SEG_META_STORE_H
#define TRAJ_BLOCK_FORMAT_SEG_META_STORE_H

#include "simple_query_engine.h"

struct seg_meta_section_entry{
    int block_logical_adr;
    void *seg_meta_section;
    int seg_meta_count;
};

struct seg_meta_section_entry_storage {
    struct seg_meta_section_entry **base;
    int current_index;
    int array_size;
};

struct serialized_seg_meta_section_entry_storage {
    void** base;
    int current_index;
    int array_size;
};

void serialize_seg_meta_section_entry(struct seg_meta_section_entry *source, void* destination);

void deserialize_seg_meta_section_entry(void* source, struct seg_meta_section_entry *destination);

void init_serialized_seg_meta_section_entry_storage(struct serialized_seg_meta_section_entry_storage *storage);

void free_serialized_seg_meta_section_entry_storage(struct serialized_seg_meta_section_entry_storage *storage);

void append_serialized_seg_meta_block_to_storage(struct serialized_seg_meta_section_entry_storage *storage, void *block);

void serialize_seg_meta_section_entry_storage(struct seg_meta_section_entry_storage *storage, struct serialized_seg_meta_section_entry_storage *serialized_storage);

void deserialize_seg_meta_section_entry_storage(struct serialized_seg_meta_section_entry_storage *serialized_storage, struct seg_meta_section_entry_storage *storage);

void init_seg_meta_entry_storage(struct seg_meta_section_entry_storage *storage);

void append_to_seg_meta_entry_storage(struct seg_meta_section_entry_storage *storage, struct seg_meta_section_entry *entry);

void free_seg_meta_entry_storage(struct seg_meta_section_entry_storage *storage);

int estimate_id_temporal_result_size(struct seg_meta_section_entry_storage *storage, struct id_temporal_predicate *predicate);

int estimate_spatio_temporal_result_size(struct seg_meta_section_entry_storage *storage, struct spatio_temporal_range_predicate *predicate);

#endif //TRAJ_BLOCK_FORMAT_SEG_META_STORE_H
