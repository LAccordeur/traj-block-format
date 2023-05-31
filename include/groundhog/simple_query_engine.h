//
// Created by yangguo on 11/16/22.
//

#ifndef TRAJ_BLOCK_FORMAT_SIMPLE_QUERY_ENGINE_H
#define TRAJ_BLOCK_FORMAT_SIMPLE_QUERY_ENGINE_H

#include <stdio.h>

#include "simple_index.h"
#include "simple_storage.h"
#include "seg_meta_store.h"

struct simple_query_engine {
    struct traj_storage data_storage;
    struct index_entry_storage index_storage;
    struct seg_meta_section_entry_storage seg_meta_storage;
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

/**
 * represent a continuous block region
 */
struct continuous_block_meta {
    int block_start;
    int block_count;
};

void init_query_engine(struct simple_query_engine *engine);

void init_query_engine_with_persistence(struct simple_query_engine *engine, struct my_file *data_file, struct my_file *index_file, struct my_file *meta_file);

void free_query_engine(struct simple_query_engine *engine);

void ingest_data_via_time_partition(struct simple_query_engine *engine, FILE *fp, int block_num);

void ingest_synthetic_data_via_time_partition(struct simple_query_engine *engine, int block_num);

void ingest_and_flush_data_via_time_partition(struct simple_query_engine *engine, FILE *fp, int block_num);

void ingest_and_flush_data_via_zcurve_partition(struct simple_query_engine *engine, FILE *fp, int block_num);

void ingest_and_flush_synthetic_data_via_time_partition(struct simple_query_engine *engine, int block_num);

void rebuild_query_engine_from_file(struct simple_query_engine *engine);

int id_temporal_query(struct simple_query_engine *engine, struct id_temporal_predicate *predicate);

int spatio_temporal_query(struct simple_query_engine *engine, struct spatio_temporal_range_predicate *predicate);

int id_temporal_query_isp(struct simple_query_engine *engine, struct id_temporal_predicate *predicate);

int spatio_temporal_query_isp(struct simple_query_engine *engine, struct spatio_temporal_range_predicate *predicate);

int estimate_id_temporal_result_size(struct seg_meta_section_entry_storage *storage, struct id_temporal_predicate *predicate);

int estimate_spatio_temporal_result_size(struct seg_meta_section_entry_storage *storage, struct spatio_temporal_range_predicate *predicate);


int calculate_aggregate_block_vec_size(int *block_addr_vec, int block_addr_vec_size);
/**
 *
 * Given a vector of block address @param block_addr_vec , aggregate them to continuous ones
 * Example:
 * 1, 2, 5, 6, 8 -> [1, 2], [5, 6], [8]
 * @param block_addr_vec
 * @param block_addr_vec_size
 * @param block_meta_vec
 * @param block_meta_vec_size  use function calculate_aggregate_block_vec_size() to calculate the value of @param block_meta_vec_size before call this function
 */
void aggregate_blocks(int *block_addr_vec, int block_addr_vec_size, struct continuous_block_meta *block_meta_vec, int block_meta_vec_size);

void print_aggregated_blocks_meta(struct continuous_block_meta *block_meta_vec, int block_meta_vec_size);

int spatio_temporal_query_without_pushdown(struct simple_query_engine *engine, struct spatio_temporal_range_predicate *predicate, bool enable_host_index);

int spatio_temporal_query_without_pushdown_batch(struct simple_query_engine *engine, struct spatio_temporal_range_predicate *predicate, bool enable_host_index);

int spatio_temporal_query_with_full_pushdown(struct simple_query_engine *engine, struct spatio_temporal_range_predicate *predicate, bool enable_host_index);

int spatio_temporal_query_with_full_pushdown_batch(struct simple_query_engine *engine, struct spatio_temporal_range_predicate *predicate, bool enable_host_index);

int spatio_temporal_query_with_full_pushdown_fpga(struct simple_query_engine *engine, struct spatio_temporal_range_predicate *predicate, bool enable_host_index);

int spatio_temporal_query_with_full_pushdown_fpga_batch(struct simple_query_engine *engine, struct spatio_temporal_range_predicate *predicate, bool enable_host_index);

int spatio_temporal_query_with_adaptive_pushdown(struct simple_query_engine *engine, struct spatio_temporal_range_predicate *predicate, bool enable_host_index);

int id_temporal_query_with_full_pushdown(struct simple_query_engine *engine, struct id_temporal_predicate *predicate, bool enable_host_index);

int id_temporal_query_with_full_pushdown_fpga(struct simple_query_engine *engine, struct id_temporal_predicate *predicate, bool enable_host_index);

int id_temporal_query_without_pushdown(struct simple_query_engine *engine, struct id_temporal_predicate *predicate, bool enable_host_index);

int id_temporal_query_without_pushdown_batch(struct simple_query_engine *engine, struct id_temporal_predicate *predicate, bool enable_host_index);

int id_temporal_query_with_full_pushdown_batch(struct simple_query_engine *engine, struct id_temporal_predicate *predicate, bool enable_host_index);

int id_temporal_query_with_full_pushdown_fpga_batch(struct simple_query_engine *engine, struct id_temporal_predicate *predicate, bool enable_host_index);
#endif //TRAJ_BLOCK_FORMAT_SIMPLE_QUERY_ENGINE_H
