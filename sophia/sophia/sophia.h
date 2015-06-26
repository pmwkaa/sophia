#ifndef SOPHIA_H_
#define SOPHIA_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>
#include <stdint.h>

#if __GNUC__ >= 4
#  define SP_API __attribute__((visibility("default")))
#else
#  define SP_API
#endif

SP_API void    *sp_env(void);
SP_API void    *sp_object(void*);
SP_API int      sp_open(void*);
SP_API int      sp_drop(void*);
SP_API int      sp_destroy(void*);
SP_API int      sp_error(void*);
SP_API void    *sp_asynchronous(void*);
SP_API void    *sp_poll(void*);
SP_API int      sp_setobject(void*, char*, void*);
SP_API int      sp_setstring(void*, char*, void*, int);
SP_API int      sp_setint(void*, char*, int64_t);
SP_API void    *sp_getobject(void*, char*);
SP_API void    *sp_getstring(void*, char*, int*);
SP_API int64_t  sp_getint(void*, char*);
SP_API int      sp_set(void*, void*);
SP_API int      sp_update(void*, void*);
SP_API int      sp_delete(void*, void*);
SP_API void    *sp_get(void*, void*);
SP_API void    *sp_cursor(void*, void*);
SP_API void    *sp_begin(void*);
SP_API int      sp_prepare(void*);
SP_API int      sp_commit(void*);

#ifdef __cplusplus
}
#endif

#endif
