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
		fprintf(st_r.output, ": fail (%s:%d) %s\n", __FILE__, __LINE__, #expr); \
		fflush(st_r.output); \
		abort(); \
	} \
	st_r.stat_stmt++; \
})

#endif
