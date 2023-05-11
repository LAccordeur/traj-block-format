//
// Created by yangguo on 23-5-4.
//

#include <stdio.h>
#include "groundhog/simple_query_engine.h"
#include "groundhog/query_workload_reader.h"
#include "time.h"
#include <stdlib.h>


static void ingest_and_flush_synthetic_data(int data_block_num) {
    init_and_mk_fs_for_traj(false);


    // ingest data
    struct simple_query_engine query_engine;
    struct my_file data_file = {NULL, DATA_FILENAME, "w", SPDK_FS_MODE};
    struct my_file index_file = {NULL, INDEX_FILENAME, "w", SPDK_FS_MODE};
    struct my_file meta_file = {NULL, SEG_META_FILENAME, "w", SPDK_FS_MODE};
    init_query_engine_with_persistence(&query_engine, &data_file, &index_file, &meta_file);
    ingest_and_flush_synthetic_data_via_time_partition(&query_engine, data_block_num);

    // print temporal meta information for each block
    struct index_entry_storage *index_storage = &query_engine.index_storage;
    for (int i = 0; i <= index_storage->current_index; i++) {
        struct index_entry *entry = index_storage->index_entry_base[i];
        printf("block pointer: [%d], time min: %d, time max: %d\n", entry->block_logical_adr, entry->time_min, entry->time_max);
    }

    // save filesystem meta
    spdk_flush_static_fs_meta_for_traj();

}

static void exp_read_data_host(struct spatio_temporal_range_predicate *predicate) {
    init_and_mk_fs_for_traj(true);
    print_spdk_static_fs_meta_for_traj();

    struct simple_query_engine rebuild_engine;
    struct my_file data_file_rebuild = {NULL, DATA_FILENAME, "r", SPDK_FS_MODE};
    struct my_file index_file_rebuild = {NULL, INDEX_FILENAME, "r", SPDK_FS_MODE};
    struct my_file meta_file_rebuild = {NULL, SEG_META_FILENAME, "r", SPDK_FS_MODE};
    init_query_engine_with_persistence(&rebuild_engine, &data_file_rebuild, &index_file_rebuild, &meta_file_rebuild);


    clock_t start, end;

    start = clock();
    //fetch_continuous_traj_data_block(rebuild_engine.data_storage, 0, 256, );
    end = clock();
    printf("[host] read time: %f second\n", (double) (end - start));


    free_query_engine(&rebuild_engine);
}

void exp_computation() {

    srand(time(NULL));   // Initialization, should only be called once.


    int record_num = 10000000;
    int result_count = 0;
    struct traj_point *points = malloc(sizeof(struct traj_point) * record_num);
    for (int i = 0; i < record_num; i++) {
        int random_value = rand() % record_num;
        points[i].normalized_latitude = random_value;
        points[i].normalized_longitude = random_value;
        points[i].timestamp_sec = random_value;
        points[i].oid = random_value;
        //printf("value: %d\n", random_value);
    }
    struct spatio_temporal_range_predicate *predicate = malloc(sizeof(*predicate));
    int predicate_value = 1000000*10;
    predicate->lat_max = predicate_value;
    predicate->lat_min = 0;
    predicate->lon_max = predicate_value;
    predicate->lon_min = 0;
    predicate->time_max = predicate_value;
    predicate->time_min = 0;

    clock_t start, stop;

    void* output_buffer = malloc(4096);
    struct traj_point* traj_output = output_buffer;
    //struct traj_point *point = &points[0];
    start = clock();
    for (int k = 0; k < record_num; k++) {
        struct traj_point *point = &points[k];
        if (predicate->lon_min <= point->normalized_longitude
            && predicate->lon_max >= point->normalized_longitude
            && predicate->lat_min <= point->normalized_latitude
            && predicate->lat_max >= point->normalized_latitude
            && predicate->time_min <= point->timestamp_sec
            && predicate->time_max >= point->timestamp_sec) {
            result_count++;
            traj_output[0].oid = point->oid;
            traj_output[0].normalized_longitude = point->normalized_longitude;
            traj_output[0].normalized_latitude = point->normalized_latitude;
            traj_output[0].timestamp_sec = point->timestamp_sec;
        }

        /*if (predicate->lon_min <= point->normalized_longitude
            && predicate->lon_max >= point->normalized_longitude
            ) {
            result_count++;
        }*/
    }
    stop = clock();
    printf("result count: %d, total computation time: %f\n", result_count, (double)(stop - start));
}

int main(void) {
    exp_computation();
    return 0;
}