//
// Created by yangguo on 11/22/22.
//

#include "gtest/gtest.h"

extern "C" {
#include "simple_query_engine.h"
#include "normalization_util.h"
}

static void print_index_entry(struct index_entry *entry) {
    printf("oid_array_size: %d\n", entry->oid_array_size);
    printf("oid_array: %p\n", entry->oid_array);
    printf("lon_min: %d\n", entry->lon_min);
    printf("lon_max: %d\n", entry->lon_max);
    printf("lat_min: %d\n", entry->lat_min);
    printf("lat_max: %d\n", entry->lat_max);
    printf("time_min: %d\n", entry->time_min);
    printf("time_max: %d\n", entry->time_max);
    printf("block_logical_adr: %d\n", entry->block_logical_adr);
    printf("block_physical_ptr: %p\n\n", entry->block_physical_ptr);
}

TEST(queryenginetest, init) {
    struct simple_query_engine query_engine;
    init_query_engine(&query_engine);
    free_query_engine(&query_engine);
}

TEST(queryenginetest, ingest) {

    FILE *fp = fopen("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v2.csv", "r");

    struct simple_query_engine query_engine;
    init_query_engine(&query_engine);
    ingest_data_via_time_partition(&query_engine, fp, 2000);

    for (int i = 0; i <= query_engine.index_storage.current_index; i++) {
        printf("i: %d\n", i);
        struct index_entry *entry = query_engine.index_storage.index_entry_base[i];
        print_index_entry(entry);

    }

    free_query_engine(&query_engine);

}

TEST(queryengine, idtemporal) {
    FILE *fp = fopen("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v2.csv", "r");

    // ingest data
    struct simple_query_engine query_engine;
    init_query_engine(&query_engine);
    ingest_data_via_time_partition(&query_engine, fp, 2000);

    // query
    struct id_temporal_predicate predicate = {20000380, 1372636850, 1372636859};
    int result_count = id_temporal_query(&query_engine, &predicate);
    printf("result count: %d\n", result_count);

    free_query_engine(&query_engine);
}

TEST(queryengine, spatiotemporal) {
    FILE *fp = fopen("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v2.csv", "r");

    // ingest data
    struct simple_query_engine query_engine;
    init_query_engine(&query_engine);
    ingest_data_via_time_partition(&query_engine, fp, 2000);

    // query
    struct spatio_temporal_range_predicate predicate = {normalize_longitude(-8.610291), normalize_longitude(-8.610291),
                                                        normalize_latitude(41.140746), normalize_latitude(41.140746),
                                                        1372636850, 1372636859};
    int result_count = spatio_temporal_query(&query_engine, &predicate);
    printf("result count: %d\n", result_count);

    free_query_engine(&query_engine);
}

