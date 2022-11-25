cmake_minimum_required(VERSION 3.23)
project(traj_block_format C)

set(CMAKE_C_STANDARD 99)

include_directories("${PROJECT_BINARY_DIR}"
        "${PROJECT_SOURCE_DIR}/include"
        "${PROJECT_SOURCE_DIR}/lib"
        )

# testing section
enable_testing()
add_subdirectory(test)

add_library(traj_block_format
        src/library.c
        src/normalization_util.c
        src/traj_block_format.c
        src/porto_dataset_reader.c
        src/traj_processing.c
        src/simple_index.c
        src/simple_storage.c
        src/simple_query_engine.c
        src/hashset_int.c
        src/query_verification_util.c
        include/normalization_util.h
        include/traj_block_format.h
        include/porto_dataset_reader.h
        include/traj_processing.h
        include/simple_index.h
        include/simple_storage.h
        include/simple_query_engine.h
        include/hashset_int.h
        include/query_verification_util.h
        include/common_util.h
        include/log.h
        )

add_executable(uthashtest src/uthashtest.c)

add_executable(mymain src/mymain.c)
target_link_libraries(mymain ${PROJECT_SOURCE_DIR}/cmake-build-debug/libtraj_block_format.a)

