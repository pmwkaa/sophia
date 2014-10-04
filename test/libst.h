#ifndef LIBST_H_
#define LIBST_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#define t(expr) ({ \
	if (! (expr)) { \
		printf(": fail (%s:%d) %s\n", __FILE__, __LINE__, #expr); \
		fflush(NULL); \
		abort(); \
	} \
	cx->suite->stat_stmt++; \
})

#include "st.h"
#include "st_scene.h"

#endif
