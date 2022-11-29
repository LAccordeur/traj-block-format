//
// Created by yangguo on 11/29/22.
//

#include <malloc.h>
#include <string.h>
#include "seg_meta_store.h"
#include "log.h"
#include "traj_block_format.h"
#include "common_util.h"
#include "config.h"

static const int BLOCK_LOGICAL_ADR_SIZE = size_of_attribute(struct seg_meta_section_entry, block_logical_adr);
static const int SEG_META_COUNT_SIZE = size_of_attribute(struct seg_meta_section_entry, seg_meta_count);

static const int BLOCK_LOGICAL_ADR_OFFSET = 0;
static const int SEG_META_COUNT_OFFSET = BLOCK_LOGICAL_ADR_OFFSET + BLOCK_LOGICAL_ADR_SIZE;
static const int SEG_META_SECTION_OFFSET = SEG_META_COUNT_OFFSET + SEG_META_COUNT_SIZE;

static void parse_seg_meta_section(void *metadata, struct seg_meta *meta_array, int array_size) {
    char* m = metadata;
    int seg_meta_size = get_seg_meta_size();
    for (int i= 0; i < array_size; i++) {
        char* offset = m + i * seg_meta_size;
        deserialize_seg_meta(offset, meta_array + i);
    }
}

static int calculate_seg_meta_section_entry_space(int seg_meta_count) {
    return BLOCK_LOGICAL_ADR_SIZE + SEG_META_COUNT_SIZE + get_seg_meta_size() * seg_meta_count;
}

void serialize_seg_meta_section_entry(struct seg_meta_section_entry *source, void* destination) {
    char *d = destination;
    int seg_meta_section_size = get_seg_meta_size() * source->seg_meta_count;
    memcpy(d + BLOCK_LOGICAL_ADR_OFFSET, &(source->block_logical_adr), BLOCK_LOGICAL_ADR_SIZE);
    memcpy(d + SEG_META_COUNT_OFFSET, &(source->seg_meta_count), SEG_META_COUNT_SIZE);
    memcpy(d + SEG_META_SECTION_OFFSET, source->seg_meta_section, seg_meta_section_size);
}

void deserialize_seg_meta_section_entry(void* source, struct seg_meta_section_entry *destination) {
    char *s = source;
    int seg_meta_section_size = get_seg_meta_size() * destination->seg_meta_count;
    memcpy(&(destination->block_logical_adr), s + BLOCK_LOGICAL_ADR_OFFSET, BLOCK_LOGICAL_ADR_SIZE);
    memcpy(&(destination->seg_meta_count), s + SEG_META_COUNT_OFFSET, SEG_META_COUNT_SIZE);
    void *seg_meta_ptr = malloc(seg_meta_section_size);
    memcpy(seg_meta_ptr, s + SEG_META_SECTION_OFFSET, seg_meta_section_size);
    destination->seg_meta_section = seg_meta_ptr;
}

void init_serialized_seg_meta_section_entry_storage(struct serialized_seg_meta_section_entry_storage *storage) {
    storage->current_index = -1;
    storage->array_size = 1000;
    storage->base = (void**) malloc(1000 * sizeof(void*));
}

void free_serialized_seg_meta_section_entry_storage(struct serialized_seg_meta_section_entry_storage *storage) {
    for (int i = 0; i <= storage->current_index; i++) {
        free(storage->base[i]);
        storage->base[i] = NULL;
    }
    free(storage->base);
    storage->base = NULL;
}

void append_serialized_seg_meta_block_to_storage(struct serialized_seg_meta_section_entry_storage *storage, void *block) {
    storage->current_index++;

    if (storage->current_index < storage->array_size) {
        (storage->base)[storage->current_index] = block;
    } else {
        // we need to extend array
        int new_total_size = storage->array_size * 2;
        debug_print("[append_serialized_seg_meta_block_to_storage] extend serialized seg_meta storage from size %d to size %d\n", storage->array_size, new_total_size);
        void **tmp_base = malloc(new_total_size * sizeof(void*));
        for (int i = 0; i < storage->array_size; i++) {
            tmp_base[i] = (storage->base)[i];
        }
        free(storage->base);
        storage->base = tmp_base;

        (storage->base)[storage->current_index] = block;
        storage->array_size = new_total_size;
    }
}

void serialize_seg_meta_section_entry_storage(struct seg_meta_section_entry_storage *storage, struct serialized_seg_meta_section_entry_storage *serialized_storage) {
    int current_offset_in_block = 4;    // the first 4 bytes are used to record the total num of entries
    void *meta_block = malloc(INDEX_BLOCK_SIZE);
    memset((char*)meta_block, 0, INDEX_BLOCK_SIZE);

    int total_serialized_count = 0;
    int count = 0;
    for (int i = 0; i <= storage->current_index; i++) {
        struct seg_meta_section_entry *entry = storage->base[i];
        int serialized_space = calculate_seg_meta_section_entry_space(entry->seg_meta_count);
        if (current_offset_in_block + serialized_space <= INDEX_BLOCK_SIZE) {
            serialize_seg_meta_section_entry(entry, (char*)meta_block + current_offset_in_block);
            current_offset_in_block += serialized_space;
            count++;
        } else {
            // this serialized block is full, so we add it to the storage and create a new block
            append_serialized_seg_meta_block_to_storage(serialized_storage, meta_block);
            memcpy(meta_block, &count, 4);
            total_serialized_count += count;
            meta_block = malloc(INDEX_BLOCK_SIZE);
            current_offset_in_block = 4;
            count = 0;
            memset((char*)meta_block, 0, INDEX_BLOCK_SIZE);
            serialize_seg_meta_section_entry(entry, (char*)meta_block + current_offset_in_block);
            current_offset_in_block += serialized_space;
            count++;
        }
    }

    // handle the last block that not full
    append_serialized_seg_meta_block_to_storage(serialized_storage, meta_block);
    memcpy(meta_block, &count, 4);
    total_serialized_count += count;
    debug_print("[serialize_seg_meta_section_entry_storage] expected serialized count: %d\n", storage->current_index + 1);
    debug_print("[serialize_seg_meta_section_entry_storage] total serialized count: %d\n", total_serialized_count);

}

void deserialize_seg_meta_section_entry_storage(struct serialized_seg_meta_section_entry_storage *serialized_storage, struct seg_meta_section_entry_storage *storage) {

    int total_deserialized_count = 0;

    int serialized_space = 0;
    for (int i = 0; i <= serialized_storage->current_index; i++) {
        char *block_base = serialized_storage->base[i];
        int count = 0;
        int current_offset_in_block = 4;
        memcpy(&count, block_base, 4);
        total_deserialized_count += count;
        for (int j = 0; j < count; j++) {
            struct seg_meta_section_entry *entry = malloc(sizeof(struct seg_meta_section_entry));
            deserialize_seg_meta_section_entry(block_base + current_offset_in_block, entry);
            append_to_seg_meta_entry_storage(storage, entry);
            serialized_space = calculate_seg_meta_section_entry_space(entry->seg_meta_count);
            current_offset_in_block += serialized_space;
        }
    }
    debug_print("[deserialize_seg_meta_section_entry_storage] total deserialized count: %d\n", total_deserialized_count);

}


void init_seg_meta_entry_storage(struct seg_meta_section_entry_storage *storage) {
    storage->current_index = -1;
    storage->array_size = 1000;
    storage->base = (struct seg_meta_section_entry**) malloc(1000 * sizeof(struct seg_meta_section_entry*));
}

void append_to_seg_meta_entry_storage(struct seg_meta_section_entry_storage *storage, struct seg_meta_section_entry *entry) {
    storage->current_index++;

    if (storage->current_index < storage->array_size) {
        (storage->base)[storage->current_index] = entry;
    } else {
        // we need to extend array
        int new_total_size = storage->array_size * 2;
        debug_print("[append_to_seg_meta_entry_storage] extend seg meta entry storage from size %d to size %d\n", storage->array_size, new_total_size);
        struct seg_meta_section_entry **tmp_base = malloc(new_total_size * sizeof(struct seg_meta_section_entry*));
        for (int i = 0; i < storage->array_size; i++) {
            tmp_base[i] = (storage->base)[i];
        }
        free(storage->base);
        storage->base = tmp_base;

        (storage->base)[storage->current_index] = entry;
        storage->array_size = new_total_size;
    }
}

void free_seg_meta_entry_storage(struct seg_meta_section_entry_storage *storage) {
    for (int i = 0; i <= storage->current_index; i++) {
        free(storage->base[i]->seg_meta_section);
        storage->base[i]->seg_meta_section = NULL;
        free(storage->base[i]);
        storage->base[i] = NULL;
    }
    free(storage->base);
}

int estimate_id_temporal_result_size(struct seg_meta_section_entry_storage *storage, struct id_temporal_predicate *predicate) {
    int result_size = 0;
    for (int i = 0; i <= storage->current_index; i++) {
        struct seg_meta_section_entry *entry = storage->base[storage->current_index];
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
        struct seg_meta_section_entry *entry = storage->base[storage->current_index];
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

