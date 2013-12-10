# Copyright (C) 2013 LuaDist.
# Created by Michal Kottman
# Redistribution and use of this file is allowed according to the terms of the MIT license.
# For details see the COPYRIGHT file distributed with LuaDist.
# Please note that the package source code is licensed under its own license.

# Project
project ( sophia C )
cmake_minimum_required ( VERSION 2.8 )
include ( cmake/dist.cmake )

# link to pthread
find_package ( Threads )
if ( Threads_FOUND )
  if ( CMAKE_USE_PTHREADS_INIT )
    add_definitions ( -D_REENTRANT=1 -DTHREADSAFE=1 )
    set ( EXTRA_LIBS ${CMAKE_THREAD_LIBS_INIT} )
  endif ( )
endif ( )

# sophia library
include_directories ( db )
add_library ( sophia 
	db/file.c
	db/util.c
	db/gc.c
	db/rep.c
	db/cat.c
	db/sp.c
	db/i.c
	db/e.c
	db/cursor.c
	db/recover.c
	db/merge.c
	db/crc.c
)
if ( CMAKE_USE_PTHREADS_INIT )
  target_link_libraries ( sophia ${EXTRA_LIBS} )
endif ( )
install_library ( sophia )
