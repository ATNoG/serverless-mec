include_directories(../include)
set(raw_test raw_test.c)
add_executable(raw_test ${raw_test} ../src/rsock.c ../src/utils.c)
