//
// Created by yangguo on 11/28/22.
//

#include <stdio.h>
#include "simple_query_engine.h"
#include "query_verification_util.h"
#include "query_workload_reader.h"
#include "log.h"

static void verify_id_temporal_query_exe() {
    FILE *data_fp = fopen("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v2.csv", "r");
    FILE *query_fp = fopen("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/query-on-10w/porto_10w_id_24h.query", "r");

    // ingest data
    struct simple_query_engine query_engine;
    init_query_engine(&query_engine);
    ingest_data_via_time_partition(&query_engine, data_fp, 500);

    // read queries
    struct id_temporal_predicate **predicates = allocate_id_temporal_predicate_mem(50);
    read_id_temporal_queries_from_csv(query_fp, predicates, 50);

    // query
    for (int i = 0; i < 50; i++) {
        int engine_result = id_temporal_query(&query_engine, predicates[i]);
        int verification_result = verify_id_temporal_query(data_fp, predicates[i], 500);
        printf("engine result: %d, verification result: %d\n", engine_result, verification_result);
        if (engine_result != verification_result) {
            debug_print("not matched %d, %d\n", engine_result, verification_result);
        }
    }

    free_query_engine(&query_engine);
}

static void verify_spatio_temporal_query_exe() {
    FILE *data_fp = fopen("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v2.csv", "r");
    FILE *query_fp = fopen("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/query-on-10w/porto_10w_1h_01.query", "r");

    // ingest data
    struct simple_query_engine query_engine;
    init_query_engine(&query_engine);
    ingest_data_via_time_partition(&query_engine, data_fp, 500);

    // read queries
    struct spatio_temporal_range_predicate **predicates = allocate_spatio_temporal_predicate_mem(50);
    read_spatio_temporal_queries_from_csv(query_fp, predicates, 50);

    // query
    for (int i = 0; i < 50; i++) {
        int engine_result = spatio_temporal_query(&query_engine, predicates[i]);
        int verification_result = verify_spatio_temporal_query(data_fp, predicates[i], 500);
        printf("engine result: %d, verification result: %d\n", engine_result, verification_result);
        if (engine_result != verification_result) {
            debug_print("not matched %d, %d\n", engine_result, verification_result);
        }
    }

    int count = calculate_total_num_of_points_in_storage(&query_engine.data_storage);
    printf("total number in storage: %d\n", count);
    free_query_engine(&query_engine);

}


int main(void) {
    verify_spatio_temporal_query_exe();
}
