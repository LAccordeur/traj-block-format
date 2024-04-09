//
// Created by yangguo on 23-6-6.
//

#include <stdio.h>
#include "groundhog/simple_query_engine.h"
#include "groundhog/query_workload_reader.h"
#include "time.h"
#include "groundhog/config.h"
#include "groundhog/normalization_util.h"

static bool enable_host_index = false;

static void ingest_and_flush_synthetic_data_large(int data_block_num_each_run, int run_num) {
    init_and_mk_fs_for_traj(false);

    // ingest data
    struct simple_query_engine query_engine;
    struct my_file data_file = {NULL, DATA_FILENAME, "w", SPDK_FS_MODE};
    struct my_file index_file = {NULL, INDEX_FILENAME, "w", SPDK_FS_MODE};
    struct my_file meta_file = {NULL, SEG_META_FILENAME, "w", SPDK_FS_MODE};
    init_query_engine_with_persistence(&query_engine, &data_file, &index_file, &meta_file);

    for (int i = 0; i < run_num; i++) {
        printf("the %d run of data ingestion\n", i);
        init_query_engine_with_persistence(&query_engine, &data_file, &index_file, &meta_file);
        ingest_and_flush_synthetic_data_via_time_partition_with_block_index(&query_engine, i * data_block_num_each_run, data_block_num_each_run);
        free_query_engine(&query_engine);
    }


    print_spdk_static_fs_meta_for_traj();
    // save filesystem meta
    spdk_flush_static_fs_meta_for_traj();
}

static void ingest_and_flush_osm_data_zcurve_large(int data_block_num_each_run, int run_num) {
    init_and_mk_fs_for_traj(false);

    FILE *data_fp = fopen("/home/yangguo/Dataset/osm/osm_points_v1.csv", "r");
    // ingest data
    struct simple_query_engine query_engine;
    struct my_file data_file = {NULL, DATA_FILENAME, "w", SPDK_FS_MODE};
    struct my_file index_file = {NULL, INDEX_FILENAME, "w", SPDK_FS_MODE};
    struct my_file meta_file = {NULL, SEG_META_FILENAME, "w", SPDK_FS_MODE};
    init_query_engine_with_persistence(&query_engine, &data_file, &index_file, &meta_file);

    for (int i = 0; i < run_num; i++) {
        printf("the %d run of data ingestion\n", i);
        init_query_engine_with_persistence(&query_engine, &data_file, &index_file, &meta_file);
        ingest_and_flush_osm_data_via_zcurve_partition_with_block_index(&query_engine, data_fp, i * data_block_num_each_run, data_block_num_each_run);
        free_query_engine(&query_engine);
    }


    print_spdk_static_fs_meta_for_traj();
    // save filesystem meta
    spdk_flush_static_fs_meta_for_traj();
}


static void print_large_file_info() {
    init_and_mk_fs_for_traj(true);
    print_spdk_static_fs_meta_for_traj();

    struct simple_query_engine rebuild_engine;
    struct my_file data_file_rebuild = {NULL, DATA_FILENAME, "r", SPDK_FS_MODE};
    struct my_file index_file_rebuild = {NULL, INDEX_FILENAME, "r", SPDK_FS_MODE};
    struct my_file meta_file_rebuild = {NULL, SEG_META_FILENAME, "r", SPDK_FS_MODE};
    init_query_engine_with_persistence(&rebuild_engine, &data_file_rebuild, &index_file_rebuild, &meta_file_rebuild);


    rebuild_query_engine_from_file(&rebuild_engine);

    // print temporal meta information for each block
    struct index_entry_storage *index_storage = &rebuild_engine.index_storage;
    for (int i = 0; i <= index_storage->current_index; i++) {
        struct index_entry *entry = index_storage->index_entry_base[i];
        printf("block pointer: [%d], time min: %d, time max: %d\n", entry->block_logical_adr, entry->time_min, entry->time_max);
    }
    free_query_engine(&rebuild_engine);
}

int main(void)  {
    //ingest_and_flush_synthetic_data_large(2, 4);
    //print_large_file_info();

    ingest_and_flush_osm_data_zcurve_large(442000, 24);
}
