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

SP_API void *sp_env(void);
SP_API void *sp_storage(void*, ...);
SP_API void *sp_ctl(void*, ...);
SP_API void *sp_object(void*, ...);
SP_API int   sp_open(void*, ...);
SP_API int   sp_destroy(void*, ...);
SP_API int   sp_set(void*, ...);
SP_API void *sp_get(void*, ...);
SP_API int   sp_delete(void*, ...);
SP_API void *sp_begin(void*, ...);
SP_API int   sp_commit(void*, ...);
SP_API int   sp_rollback(void*, ...);
SP_API void *sp_cursor(void*, ...);
SP_API void *sp_backup(void*, ...);
SP_API void *sp_copy(void*, ...);
SP_API void *sp_type(void*, ...);
SP_API void *sp_error(void*, ...);

#ifdef __cplusplus
}
#endif

#endif
