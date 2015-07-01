#ifndef ST_ASSERT_H_
#define ST_ASSERT_H_

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
	st_r.stat_stmt++; \
})

#endif
