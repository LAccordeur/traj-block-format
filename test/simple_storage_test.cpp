//
// Created by yangguo on 11/22/22.
//

#include "gtest/gtest.h"

extern "C" {
#include "simple_storage.h"
}

TEST(storage, test) {

    struct traj_storage storage;
    init_traj_storage(&storage);

    for (int i = 0; i < 8002; i++) {
        void * ptr = NULL;
        struct address_pair result = append_traj_block_to_storage(&storage, ptr);
        printf("physical: %p, logical: %d\n", result.physical_ptr, result.logical_adr);
    }


    free_traj_storage(&storage);
}
