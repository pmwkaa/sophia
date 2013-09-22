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

enum {
	SPO_RDONLY = 1,
	SPO_RDWR   = 2,
	SPO_CREAT  = 4,
	SPO_SYNC   = 8
};

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

void *sp_env(void);
void *sp_open(void *env);
int sp_ctl(void*, spopt, ...);
int sp_destroy(void *ptr);
int sp_begin(void *db);
int sp_commit(void *db);
int sp_rollback(void *db);
int sp_set(void *db, const void *k, size_t ksize, const void *v, size_t vsize);
int sp_delete(void *db, const void *k, size_t ksize);
int sp_get(void *db, const void *k, size_t ksize, void **v, size_t *vsize);
void *sp_cursor(void *db, sporder, const void *k, size_t ksize);
int sp_fetch(void *cur);
const char *sp_key(void *cur);
size_t sp_keysize(void *cur);
const char *sp_value(void *cur);
size_t sp_valuesize(void *cur);
char *sp_error(void *ptr);
void sp_stat(void *ptr, spstat*);

#ifdef __cplusplus
}
#endif

#endif
