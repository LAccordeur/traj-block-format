project(traj_block_format)

add_subdirectory(googletest)

include_directories(${gtest_SOURCE_DIR}/include
        ${gtest_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/../include
        ${CMAKE_SOURCE_DIR}/../lib
        )

add_executable(mytest mytest.cpp)
target_link_libraries(mytest gtest gtest_main)
target_link_libraries(mytest ${CMAKE_SOURCE_DIR}/cmake-build-debug/libtraj_block_format.a)


add_executable(traj_block_format_test traj_block_format_test.cpp)
target_link_libraries(traj_block_format_test gtest gtest_main)
target_link_libraries(traj_block_format_test ${CMAKE_SOURCE_DIR}/cmake-build-debug/libtraj_block_format.a)

add_executable(porto_dataset_read_test porto_dataset_read_test.cpp)
target_link_libraries(porto_dataset_read_test gtest gtest_main)
target_link_libraries(porto_dataset_read_test ${CMAKE_SOURCE_DIR}/cmake-build-debug/libtraj_block_format.a)

add_executable(traj_processing_test traj_processing_test.cpp)
target_link_libraries(traj_processing_test gtest gtest_main)
target_link_libraries(traj_processing_test ${CMAKE_SOURCE_DIR}/cmake-build-debug/libtraj_block_format.a)

