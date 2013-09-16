cmake_minimum_required (VERSION 2.6)
project (sophia C)

option (sophia_build_tests "Build sophia tests" ON)

find_package (Threads)
include_directories (db/)
add_library (sophia STATIC
	db/cat.c
	db/crc.c
	db/cursor.c
	db/e.c
	db/file.c
	db/gc.c
	db/i.c
	db/merge.c
	db/recover.c
	db/rep.c
	db/sp.c
	db/util.c)
target_link_libraries (sophia
	${CAKE_THREAD_LIBS_INIT})

if (sophia_build_tests)
	enable_testing ()
	add_subdirectory (test)
endif ()