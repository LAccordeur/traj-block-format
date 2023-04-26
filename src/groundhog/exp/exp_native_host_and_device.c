//
// Created by yangguo on 23-2-7.
//
// compare performance between host computation and in-storage-computing
// to get the cross-point which tell us when in-storage-computing is better

#include <stdio.h>
#include "groundhog/simple_query_engine.h"
#include "groundhog/query_workload_reader.h"
#include "time.h"

static void ingest_and_flush_porto_data(int data_block_num) {
    init_and_mk_fs_for_traj(false);

    //int data_block_num = 1024;

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
        printf("block pointer: [%d], time min: %d, time max: %d, lon min: %d, lon max: %d, lat min: %d, lat max: %d\n", entry->block_logical_adr, entry->time_min, entry->time_max, entry->lon_min, entry->lon_max, entry->lat_min, entry->lat_max);
    }

    // save filesystem meta
    spdk_flush_static_fs_meta_for_traj();

}

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

static void exp_native_spatio_temporal_host(struct spatio_temporal_range_predicate *predicate) {
    init_and_mk_fs_for_traj(true);
    print_spdk_static_fs_meta_for_traj();

    struct simple_query_engine rebuild_engine;
    struct my_file data_file_rebuild = {NULL, DATA_FILENAME, "r", SPDK_FS_MODE};
    struct my_file index_file_rebuild = {NULL, INDEX_FILENAME, "r", SPDK_FS_MODE};
    struct my_file meta_file_rebuild = {NULL, SEG_META_FILENAME, "r", SPDK_FS_MODE};
    init_query_engine_with_persistence(&rebuild_engine, &data_file_rebuild, &index_file_rebuild, &meta_file_rebuild);


    rebuild_query_engine_from_file(&rebuild_engine);
    clock_t start, end;
    start = clock();
    int engine_result = spatio_temporal_query_without_pushdown(&rebuild_engine, predicate, true);
    end = clock();
    printf("[host] query time (includin index lookup): %f\n", (double )(end - start));
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
    clock_t start, end;
    start = clock();
    int engine_result = spatio_temporal_query_with_full_pushdown(&rebuild_engine, predicate, true);
    end = clock();
    printf("[isp cpu] query time (including index lookup): %f\n", (double )(end - start));
    printf("[isp cpu] engine result: %d\n", engine_result);
    free_query_engine(&rebuild_engine);

}

static void exp_native_spatio_temporal_fpga_full_pushdown(struct spatio_temporal_range_predicate *predicate) {

    init_and_mk_fs_for_traj(true);
    print_spdk_static_fs_meta_for_traj();

    struct simple_query_engine rebuild_engine;
    struct my_file data_file_rebuild = {NULL, DATA_FILENAME, "r", SPDK_FS_MODE};
    struct my_file index_file_rebuild = {NULL, INDEX_FILENAME, "r", SPDK_FS_MODE};
    struct my_file meta_file_rebuild = {NULL, SEG_META_FILENAME, "r", SPDK_FS_MODE};
    init_query_engine_with_persistence(&rebuild_engine, &data_file_rebuild, &index_file_rebuild, &meta_file_rebuild);


    rebuild_query_engine_from_file(&rebuild_engine);
    clock_t start, end;
    start = clock();
    int engine_result = spatio_temporal_query_with_full_pushdown_fpga(&rebuild_engine, predicate, true);
    end = clock();
    printf("[isp fpga] query time (includin index lookup): %f\n", (double )(end - start));
    printf("[isp fpga] engine result: %d\n", engine_result);
    free_query_engine(&rebuild_engine);
}



static void exp_native_spatio_temporal_host_v1(struct spatio_temporal_range_predicate *predicate, struct simple_query_engine *rebuild_engine) {

    clock_t start, end;
    start = clock();
    int engine_result = spatio_temporal_query_without_pushdown(rebuild_engine, predicate, false);
    end = clock();
    printf("[host] query time (includin index lookup): %f\n", (double )(end - start));
    printf("engine result: %d\n", engine_result);


}

static void exp_native_spatio_temporal_armcpu_full_pushdown_v1(struct spatio_temporal_range_predicate *predicate, struct simple_query_engine *rebuild_engine) {

    clock_t start, end;
    start = clock();
    int engine_result = spatio_temporal_query_with_full_pushdown(rebuild_engine, predicate, false);
    end = clock();
    printf("[isp cpu] query time (including index lookup): %f\n", (double )(end - start));
    printf("[isp cpu] engine result: %d\n", engine_result);


}

static void exp_native_spatio_temporal_fpga_full_pushdown_v1(struct spatio_temporal_range_predicate *predicate, struct simple_query_engine *rebuild_engine) {


    clock_t start, end;
    start = clock();
    int engine_result = spatio_temporal_query_with_full_pushdown_fpga(rebuild_engine, predicate, false);
    end = clock();
    printf("[isp fpga] query time (includin index lookup): %f\n", (double )(end - start));
    printf("[isp fpga] engine result: %d\n", engine_result);

}

static void run_on_porto_data() {
    //ingest_and_flush_data();
    //struct spatio_temporal_range_predicate predicate = {0, 2147483647, 0, 2147483647, 1372636853, 1372757330};
    struct spatio_temporal_range_predicate predicate = {7983124, 8000193, 12214847, 12221445, 0, 1380292968};

    clock_t start, end;
    start = clock();
    exp_native_spatio_temporal_fpga_full_pushdown(&predicate);
    end = clock();
    printf("total time: %f\n",(double)(end-start));
}

static void run_on_synthetic_data() {
    //ingest_and_flush_synthetic_data(4096);

    // predicate for 512 MB data
    // 1 blocks
    //struct spatio_temporal_range_predicate predicate = {30801685, 30801919, 30801685, 30801919, 30801685, 30801919};

    // 13 blocks
    //struct spatio_temporal_range_predicate predicate = {30798877, 30801919, 30798877, 30801919, 30798877, 30801919};

    // 131 blocks
    //struct spatio_temporal_range_predicate predicate = {30771134, 30801919, 30771134, 30801919, 30771134, 30801919};

    // 1310 blocks
    //struct spatio_temporal_range_predicate predicate = {30494069, 30801919, 30494069, 30801919, 30494069, 30801919};

    // 13107 blocks
    //struct spatio_temporal_range_predicate predicate = {27751774, 30801919, 27751774, 30801919, 27751774, 30801919};

    // 26214 blocks
    //struct spatio_temporal_range_predicate predicate = {24641629, 30801919, 24641629, 30801919, 24641629, 30801919};

    // 52428 blocks
    //struct spatio_temporal_range_predicate predicate = {18481339, 30801919, 18481339, 30801919, 18481339, 30801919};

    // 78643 blocks
    //struct spatio_temporal_range_predicate predicate = {12320814, 30801919, 12320814, 30801919, 12320814, 30801919};

    // 104857 blocks
    //struct spatio_temporal_range_predicate predicate = {6160524, 30801919, 6160524, 30801919, 6160524, 30801919};

    // 131072 blocks
    //struct spatio_temporal_range_predicate predicate = {0, 30801919, 0, 30801919, 0, 30801919};


    // predicate for 4GB data
    // 1 blocks
    struct spatio_temporal_range_predicate predicate_4g_0 = {246415125, 246415359, 246415125, 246415359, 246415125, 246415359};

    // 104 blocks
    struct spatio_temporal_range_predicate predicate_4g_1 = {246390919, 246415359, 246390919, 246415359, 246390919, 246415359};

    // 1048 blocks
    struct spatio_temporal_range_predicate predicate_4g_2 = {246169079, 246415359, 246169079, 246415359, 246169079, 246415359};

    // 10485 blocks
    struct spatio_temporal_range_predicate predicate_4g_3 = {243951384, 246415359, 243951384, 246415359, 243951384, 246415359};

    // 104857 blocks
    struct spatio_temporal_range_predicate predicate_4g_4 = {221773964, 246415359, 221773964, 246415359, 221773964, 246415359};

    // 209715 blocks
    struct spatio_temporal_range_predicate predicate_4g_5 = {197132334, 246415359, 197132334, 246415359, 197132334, 246415359};

    // 419430 blocks
    struct spatio_temporal_range_predicate predicate_4g_6 = {147849309, 246415359, 147849309, 246415359, 147849309, 246415359};

    // 629145 blocks
    struct spatio_temporal_range_predicate predicate_4g_7 = {98566284, 246415359, 98566284, 246415359, 98566284, 246415359};

    // 838860 blocks
    struct spatio_temporal_range_predicate predicate_4g_8 = {49283259, 246415359, 49283259, 246415359, 49283259, 246415359};

    // 1048576 blocks
    struct spatio_temporal_range_predicate predicate_4g_9 = {0, 246415359, 0, 246415359, 0, 246415359};


    clock_t start, end;
    start = clock();
    //exp_native_spatio_temporal_host(&predicate_4g_0);
    //exp_native_spatio_temporal_armcpu_full_pushdown(&predicate_4g_0);
    exp_native_spatio_temporal_fpga_full_pushdown(&predicate_4g_0);
    end = clock();
    printf("total time: %f\n",(double)(end-start));
    printf("---------------------------------\n");
}

static void run_on_synthetic_data_batch() {
    init_and_mk_fs_for_traj(true);
    print_spdk_static_fs_meta_for_traj();

    struct simple_query_engine rebuild_engine;
    struct my_file data_file_rebuild = {NULL, DATA_FILENAME, "r", SPDK_FS_MODE};
    struct my_file index_file_rebuild = {NULL, INDEX_FILENAME, "r", SPDK_FS_MODE};
    struct my_file meta_file_rebuild = {NULL, SEG_META_FILENAME, "r", SPDK_FS_MODE};
    init_query_engine_with_persistence(&rebuild_engine, &data_file_rebuild, &index_file_rebuild, &meta_file_rebuild);


    rebuild_query_engine_from_file(&rebuild_engine);

    //ingest_and_flush_synthetic_data(4096);

    // predicate for 512 MB data
    // 1 blocks
    struct spatio_temporal_range_predicate predicate_512mb_0 = {30801685, 30801919, 30801685, 30801919, 30801685, 30801919};

    // 13 blocks
    struct spatio_temporal_range_predicate predicate_512mb_1 = {30798877, 30801919, 30798877, 30801919, 30798877, 30801919};

    // 131 blocks
    struct spatio_temporal_range_predicate predicate_512mb_2 = {30771134, 30801919, 30771134, 30801919, 30771134, 30801919};

    // 1310 blocks
    struct spatio_temporal_range_predicate predicate_512mb_3 = {30494069, 30801919, 30494069, 30801919, 30494069, 30801919};

    // 13107 blocks
    struct spatio_temporal_range_predicate predicate_512mb_4 = {27751774, 30801919, 27751774, 30801919, 27751774, 30801919};

    // 26214 blocks
    struct spatio_temporal_range_predicate predicate_512mb_5 = {24641629, 30801919, 24641629, 30801919, 24641629, 30801919};

    // 52428 blocks
    struct spatio_temporal_range_predicate predicate_512mb_6 = {18481339, 30801919, 18481339, 30801919, 18481339, 30801919};

    // 78643 blocks
    struct spatio_temporal_range_predicate predicate_512mb_7 = {12320814, 30801919, 12320814, 30801919, 12320814, 30801919};

    // 104857 blocks
    struct spatio_temporal_range_predicate predicate_512mb_8 = {6160524, 30801919, 6160524, 30801919, 6160524, 30801919};

    // 131072 blocks
    struct spatio_temporal_range_predicate predicate_512mb_9 = {0, 30801919, 0, 30801919, 0, 30801919};


    // predicate for 4GB data
    // 1 blocks
    struct spatio_temporal_range_predicate predicate_4g_0 = {246415125, 246415359, 246415125, 246415359, 246415125, 246415359};

    // 104 blocks
    struct spatio_temporal_range_predicate predicate_4g_1 = {246390919, 246415359, 246390919, 246415359, 246390919, 246415359};

    // 1048 blocks
    struct spatio_temporal_range_predicate predicate_4g_2 = {246169079, 246415359, 246169079, 246415359, 246169079, 246415359};

    // 10485 blocks
    struct spatio_temporal_range_predicate predicate_4g_3 = {243951384, 246415359, 243951384, 246415359, 243951384, 246415359};

    // 104857 blocks
    struct spatio_temporal_range_predicate predicate_4g_4 = {221773964, 246415359, 221773964, 246415359, 221773964, 246415359};

    // 209715 blocks
    struct spatio_temporal_range_predicate predicate_4g_5 = {197132334, 246415359, 197132334, 246415359, 197132334, 246415359};

    // 419430 blocks
    struct spatio_temporal_range_predicate predicate_4g_6 = {147849309, 246415359, 147849309, 246415359, 147849309, 246415359};

    // 629145 blocks
    struct spatio_temporal_range_predicate predicate_4g_7 = {98566284, 246415359, 98566284, 246415359, 98566284, 246415359};

    // 838860 blocks
    struct spatio_temporal_range_predicate predicate_4g_8 = {49283259, 246415359, 49283259, 246415359, 49283259, 246415359};

    // 1048576 blocks
    struct spatio_temporal_range_predicate predicate_4g_9 = {0, 246415359, 0, 246415359, 0, 246415359};


    clock_t start, end;
    start = clock();
    exp_native_spatio_temporal_host_v1(&predicate_4g_0, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_armcpu_full_pushdown_v1(&predicate_4g_0, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_fpga_full_pushdown_v1(&predicate_4g_0, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_host_v1(&predicate_4g_1, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_armcpu_full_pushdown_v1(&predicate_4g_1, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_fpga_full_pushdown_v1(&predicate_4g_1, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_host_v1(&predicate_4g_2, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_armcpu_full_pushdown_v1(&predicate_4g_2, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_fpga_full_pushdown_v1(&predicate_4g_2, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_host_v1(&predicate_4g_3, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_armcpu_full_pushdown_v1(&predicate_4g_3, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_fpga_full_pushdown_v1(&predicate_4g_3, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_host_v1(&predicate_4g_4, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_armcpu_full_pushdown_v1(&predicate_4g_4, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_fpga_full_pushdown_v1(&predicate_4g_4, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_host_v1(&predicate_4g_5, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_armcpu_full_pushdown_v1(&predicate_4g_5, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_fpga_full_pushdown_v1(&predicate_4g_5, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_host_v1(&predicate_4g_6, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_armcpu_full_pushdown_v1(&predicate_4g_6, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_fpga_full_pushdown_v1(&predicate_4g_6, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_host_v1(&predicate_4g_7, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_armcpu_full_pushdown_v1(&predicate_4g_7, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_fpga_full_pushdown_v1(&predicate_4g_7, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_host_v1(&predicate_4g_8, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_armcpu_full_pushdown_v1(&predicate_4g_8, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_fpga_full_pushdown_v1(&predicate_4g_8, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_host_v1(&predicate_4g_9, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_armcpu_full_pushdown_v1(&predicate_4g_9, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_fpga_full_pushdown_v1(&predicate_4g_9, &rebuild_engine);

    /*exp_native_spatio_temporal_host_v1(&predicate_512mb_0, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_armcpu_full_pushdown_v1(&predicate_512mb_0, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_fpga_full_pushdown_v1(&predicate_512mb_0, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_host_v1(&predicate_512mb_1, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_armcpu_full_pushdown_v1(&predicate_512mb_1, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_fpga_full_pushdown_v1(&predicate_512mb_1, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_host_v1(&predicate_512mb_2, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_armcpu_full_pushdown_v1(&predicate_512mb_2, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_fpga_full_pushdown_v1(&predicate_512mb_2, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_host_v1(&predicate_512mb_3, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_armcpu_full_pushdown_v1(&predicate_512mb_3, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_fpga_full_pushdown_v1(&predicate_512mb_3, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_host_v1(&predicate_512mb_4, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_armcpu_full_pushdown_v1(&predicate_512mb_4, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_fpga_full_pushdown_v1(&predicate_512mb_4, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_host_v1(&predicate_512mb_5, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_armcpu_full_pushdown_v1(&predicate_512mb_5, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_fpga_full_pushdown_v1(&predicate_512mb_5, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_host_v1(&predicate_512mb_6, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_armcpu_full_pushdown_v1(&predicate_512mb_6, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_fpga_full_pushdown_v1(&predicate_512mb_6, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_host_v1(&predicate_512mb_7, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_armcpu_full_pushdown_v1(&predicate_512mb_7, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_fpga_full_pushdown_v1(&predicate_512mb_7, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_host_v1(&predicate_512mb_8, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_armcpu_full_pushdown_v1(&predicate_512mb_8, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_fpga_full_pushdown_v1(&predicate_512mb_8, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_host_v1(&predicate_512mb_9, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_armcpu_full_pushdown_v1(&predicate_512mb_9, &rebuild_engine);
    printf("---------------------------------\n");
    exp_native_spatio_temporal_fpga_full_pushdown_v1(&predicate_512mb_9, &rebuild_engine);*/
    end = clock();
    printf("total time: %f\n",(double)(end-start));

    free_query_engine(&rebuild_engine);
}


int main(void) {
    // block pointer: [25599], time min: 6015765, time max: 6015999
    // block pointer: [131071], time min: 30801685, time max: 30801919
    // block pointer: [262143], time min: 61603605, time max: 61603839
    // block pointer: [1048575], time min: 246415125, time max: 246415359
    // 4G: 1048576  2G: 524288  1G: 262144  512MB: 131072   256MB: 65536
    // ingest_and_flush_synthetic_data(65536);
    // run_on_synthetic_data();

    //ingest_and_flush_porto_data(65536);

    run_on_porto_data();

}