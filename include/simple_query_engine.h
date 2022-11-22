//
// Created by yangguo on 11/16/22.
//

#ifndef TRAJ_BLOCK_FORMAT_SIMPLE_QUERY_ENGINE_H
#define TRAJ_BLOCK_FORMAT_SIMPLE_QUERY_ENGINE_H

#include <stdio.h>

#include "simple_index.h"
#include "simple_storage.h"

struct simple_query_engine {
    struct traj_storage data_storage;
    struct index_entry_storage index_storage;
};

struct id_temporal_predicate {
    int oid;
    int time_min;
    int time_max;
};

struct spatio_temporal_range_predicate {
    int lon_min;    // normalized value, same below
    int lon_max;
    int lat_min;
    int lat_max;
    int time_min;
    int time_max;
};

void init_query_engine(struct simple_query_engine *engine);

void free_query_engine(struct simple_query_engine *engine);

void ingest_data_via_time_partition(struct simple_query_engine *engine, FILE *fp, int block_num);

int id_temporal_query(struct simple_query_engine *engine, struct id_temporal_predicate *predicate);

int spatio_temporal_query(struct simple_query_engine *engine, struct spatio_temporal_range_predicate *predicate);

#endif //TRAJ_BLOCK_FORMAT_SIMPLE_QUERY_ENGINE_H
