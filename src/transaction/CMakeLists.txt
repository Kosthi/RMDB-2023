set(SOURCES concurrency/lock_manager.cpp transaction_manager.cpp)
add_library(transaction STATIC ${SOURCES})
target_link_libraries(transaction system recovery pthread)

add_executable(lock_manager_test lock_manager_test.cpp)
target_link_libraries(lock_manager_test transaction execution gtest_main)