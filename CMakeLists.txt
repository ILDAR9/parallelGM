cmake_minimum_required(VERSION 3.6)
project(GM_multy)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmodule/)
find_package(TBB REQUIRED)
add_definitions(${TBB_DEFINITIONS})

include_directories(${TBB_INCLUDE_DIRS})

set(SOURCE_FILES
    code/main/main.cpp
#    code/main/tbb_study.cpp
    code/source/edge.cpp
    code/source/graph.cpp)


include_directories(${PROJECT_SOURCE_DIR}/code/include)
link_directories(${PROJECT_SOURCE_DIR}/code/lib)
message("Binary tree path: ${PROJECT_SOURCE_DIR}")

add_executable(GM_multy ${SOURCE_FILES})

target_link_libraries(GM_multy ${TBB_LIBRARIES})