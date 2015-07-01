#ifndef ST_API_H_
#define ST_API_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#define st_callpointer(function, ...) ({ \
	void *o = function(__VA_ARGS__); \
	t( o != NULL ); \
	o; \
})

#define st_callint(function, ...) ({ \
	int rc  = function(__VA_ARGS__); \
	t( rc == 0 ); \
	rc; \
})

#define st_env(...)     st_callpointer(sp_env, __VA_ARGS__)
#define st_ctl(...)     st_callpointer(sp_ctl, __VA_ARGS__)
#define st_async(...)   st_callpointer(sp_async, __VA_ARGS__)
#define st_object(...)  st_callpointer(sp_object, __VA_ARGS__)
#define st_open(...)    st_callint(sp_open, __VA_ARGS__)
#define st_destroy(...) st_callint(sp_destroy, __VA_ARGS__)
#define st_error(...)   st_callint(sp_error, __VA_ARGS__)
#define st_set(...)     st_callint(sp_set, __VA_ARGS__)
#define st_delete(...)  st_callint(sp_delete, __VA_ARGS__)
#define st_get(...)     st_callpointer(sp_get, __VA_ARGS__)
#define st_poll(...)    st_callpointer(sp_poll, __VA_ARGS__)
#define st_drop(...)    st_callint(sp_drop, __VA_ARGS__)
#define st_begin(...)   st_callpointer(sp_begin, __VA_ARGS__)
#define st_prepare(...) st_callint(sp_prepare, __VA_ARGS__)
#define st_commit(...)  st_callint(sp_commit, __VA_ARGS__)
#define st_cursor(...)  st_callpointer(sp_cursor, __VA_ARGS__)
#define st_type(...)    st_callpointer(sp_type, __VA_ARGS__)

#endif
