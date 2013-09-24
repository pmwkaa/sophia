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

typedef void *(*spallocf)(void *ptr, size_t size, void *arg);
typedef int (*spcmpf)(char *a, size_t asz, char *b, size_t bsz, void *arg);

typedef enum {
	/* env related */
	SPDIR,     /* uint32_t, char* */
	SPALLOC,   /* spallocf, void* */
	SPCMP,     /* spcmpf, void* */
	SPPAGE,    /* uint32_t */
	SPGC,      /* int */
	SPGCF,     /* double */
	SPGROW,    /* uint32_t, double */
	SPMERGE,   /* int */
	SPMERGEWM, /* uint32_t */
	/* db related */
	SPMERGEFORCE,
	/* unrelated */
	SPVERSION  /* uint32_t*, uint32_t* */
} spopt;

typedef enum {
	SPO_RDONLY = 1,
	SPO_RDWR   = 2,
	SPO_CREAT  = 4,
	SPO_SYNC   = 8
} spflags;

typedef enum {
	SPGT,
	SPGTE,
	SPLT,
	SPLTE
} sporder;

typedef struct {
	uint32_t epoch;
	uint64_t psn;
	uint32_t repn;
	uint32_t repndb;
	uint32_t repnxfer;
	uint32_t catn;
	uint32_t indexn;
	uint32_t indexpages;
} spstat;

SP_API void *sp_env(void);
SP_API void *sp_open(void *env);
SP_API int sp_ctl(void*, spopt, ...);
SP_API int sp_destroy(void *ptr);
SP_API int sp_begin(void *db);
SP_API int sp_commit(void *db);
SP_API int sp_rollback(void *db);
SP_API int sp_set(void *db, const void *k, size_t ksize, const void *v, size_t vsize);
SP_API int sp_delete(void *db, const void *k, size_t ksize);
SP_API int sp_get(void *db, const void *k, size_t ksize, void **v, size_t *vsize);
SP_API void *sp_cursor(void *db, sporder, const void *k, size_t ksize);
SP_API int sp_fetch(void *cur);
SP_API const char *sp_key(void *cur);
SP_API size_t sp_keysize(void *cur);
SP_API const char *sp_value(void *cur);
SP_API size_t sp_valuesize(void *cur);
SP_API char *sp_error(void *ptr);
SP_API void sp_stat(void *ptr, spstat*);

#ifdef __cplusplus
}
#endif

#endif
