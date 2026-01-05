cmake_minimum_required(VERSION 3.5)
project(it2s-rsock)

SET(BUILD_SHARED_LIBS ON)
SET(CMAKE_INSTALL_PREFIX "/usr")
add_subdirectory(src)
add_subdirectory(test)
