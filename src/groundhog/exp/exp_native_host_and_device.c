//
// Created by yangguo on 23-2-7.
//
// compare performance between host computation and in-storage-computing
// to get the cross-point which tell us when in-storage-computing is better

#include <stdio.h>
#include "groundhog/simple_query_engine.h"
#include "groundhog/query_workload_reader.h"
#include "time.h"

static void ingest_and_flush_data() {
    init_and_mk_fs_for_traj(false);

    int data_block_num = 1024;

    FILE *data_fp = fopen("/home/yangguo/Dataset/trajectory/porto_data_v2.csv", "r");
    // ingest data
    struct simple_query_engine query_engine;
    struct my_file data_file = {NULL, DATA_FILENAME, "w", SPDK_FS_MODE};
    struct my_file index_file = {NULL, INDEX_FILENAME, "w", SPDK_FS_MODE};
    struct my_file meta_file = {NULL, SEG_META_FILENAME, "w", SPDK_FS_MODE};
    init_query_engine_with_persistence(&query_engine, &data_file, &index_file, &meta_file);
    ingest_and_flush_data_via_time_partition(&query_engine, data_fp, data_block_num);

    // print temporal meta information for each block
    struct index_entry_storage *index_storage = &query_engine.index_storage;
    for (int i = 0; i <= index_storage->current_index; i++) {
        struct index_entry *entry = index_storage->index_entry_base[i];
        printf("block pointer: [%d], time min: %d, time max: %d\n", entry->block_logical_adr, entry->time_min, entry->time_max);
    }

    // save filesystem meta
    spdk_flush_static_fs_meta_for_traj();

}

static void exp_native_spatio_temporal_host(struct spatio_temporal_range_predicate *predicate) {
    init_and_mk_fs_for_traj(true);
    print_spdk_static_fs_meta_for_traj();

    struct simple_query_engine rebuild_engine;
    struct my_file data_file_rebuild = {NULL, DATA_FILENAME, "r", SPDK_FS_MODE};
    struct my_file index_file_rebuild = {NULL, INDEX_FILENAME, "r", SPDK_FS_MODE};
    struct my_file meta_file_rebuild = {NULL, SEG_META_FILENAME, "r", SPDK_FS_MODE};
    init_query_engine_with_persistence(&rebuild_engine, &data_file_rebuild, &index_file_rebuild, &meta_file_rebuild);


    rebuild_query_engine_from_file(&rebuild_engine);
    int engine_result = spatio_temporal_query_without_pushdown(&rebuild_engine, predicate);
    printf("engine result: %d\n", engine_result);

    free_query_engine(&rebuild_engine);
}

static void exp_native_spatio_temporal_armcpu_full_pushdown(struct spatio_temporal_range_predicate *predicate) {
    init_and_mk_fs_for_traj(true);
    print_spdk_static_fs_meta_for_traj();

    struct simple_query_engine rebuild_engine;
    struct my_file data_file_rebuild = {NULL, DATA_FILENAME, "r", SPDK_FS_MODE};
    struct my_file index_file_rebuild = {NULL, INDEX_FILENAME, "r", SPDK_FS_MODE};
    struct my_file meta_file_rebuild = {NULL, SEG_META_FILENAME, "r", SPDK_FS_MODE};
    init_query_engine_with_persistence(&rebuild_engine, &data_file_rebuild, &index_file_rebuild, &meta_file_rebuild);


    rebuild_query_engine_from_file(&rebuild_engine);
    int engine_result = spatio_temporal_query_with_full_pushdown(&rebuild_engine, predicate);
    printf("engine result: %d\n", engine_result);
    free_query_engine(&rebuild_engine);

}


int main(void) {
    ingest_and_flush_data();
    struct spatio_temporal_range_predicate predicate = {0, 2147483647, 0, 2147483647, 1372636853, 1372757330};

    clock_t start, end;
    start = clock();
    exp_native_spatio_temporal_armcpu_full_pushdown(&predicate);
    end = clock();
    printf("time: %f\n",(double)(end-start));

}