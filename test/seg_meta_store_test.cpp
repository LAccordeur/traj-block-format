//
// Created by yangguo on 11/29/22.
//

#include "gtest/gtest.h"

extern "C" {
#include "seg_meta_store.h"
#include "config.h"
#include "traj_block_format.h"
#include "porto_dataset_reader.h"
}



TEST(seg_meta_store_test, init) {
    struct seg_meta_section_entry_storage storage;
    init_seg_meta_entry_storage(&storage);
    free_seg_meta_entry_storage(&storage);
}

TEST(seg_meta_store_test, create) {
    struct seg_meta_section_entry_storage storage;
    init_seg_meta_entry_storage(&storage);

    FILE *data_fp = fopen("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v2.csv", "r");

    // ingest data
    struct simple_query_engine query_engine;
    init_query_engine(&query_engine);

    struct traj_storage *data_storage = &query_engine.data_storage;
    struct index_entry_storage *index_storage = &query_engine.index_storage;
    // trajectory block info
    int points_num = calculate_points_num_via_block_size(TRAJ_BLOCK_SIZE, SPLIT_SEGMENT_NUM);

    for (int i = 0; i < 500; i++) {
        struct traj_point **points = allocate_points_memory(points_num);
        read_points_from_csv(data_fp,points, i*points_num, points_num);
        // convert and put this data to traj storage
        void *data = malloc(TRAJ_BLOCK_SIZE);
        do_self_contained_traj_block(points, points_num, data, TRAJ_BLOCK_SIZE);
        struct address_pair data_addresses = append_traj_block_to_storage(data_storage, data);

        // update seg_meta
        struct seg_meta_section_entry *entry = (struct seg_meta_section_entry *)malloc(sizeof(struct seg_meta_section_entry));
        struct traj_block_header header;
        parse_traj_block_for_header(data, &header);
        int meta_section_size = get_seg_meta_section_size(data);
        void* meta_section = malloc(meta_section_size);
        extract_seg_meta_section(data, meta_section);
        entry->seg_meta_count = header.seg_count;
        entry->seg_meta_section = meta_section;
        entry->block_logical_adr = data_addresses.logical_adr;
        append_to_seg_meta_entry_storage(&storage, entry);

        free_points_memory(points, points_num);
    }

    struct id_temporal_predicate predicate;
    predicate.oid = 20000367;
    predicate.time_min = 1372637312;
    predicate.time_max = 1372640912;
    int result_size = estimate_id_temporal_result_size(&storage, &predicate);
    printf("result size: %d\n", result_size);


    free_seg_meta_entry_storage(&storage);
}
