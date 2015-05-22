#ifndef SX_H_
#define SX_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sxmanager sxmanager;
typedef struct sxindex sxindex;
typedef struct sx sx;

typedef enum {
	SXUNDEF,
	SXREADY,
	SXCOMMIT,
	SXPREPARE,
	SXROLLBACK,
	SXLOCK
} sxstate;

typedef sxstate (*sxpreparef)(sx*, sv*, void*, void*);

struct sxindex {
	srrb i;
	uint32_t count;
	uint32_t dsn;
	srscheme *scheme;
	void *ptr;
};

struct sx {
	uint32_t id;
	sxstate s;
	uint64_t vlsn;
	svlog log;
	srlist deadlock;
	sxmanager *manager;
	srrbnode node;
};

struct sxmanager {
	srspinlock lockupd;
	srspinlock lock;
	srrb i;
	uint32_t count;
	sra *asxv;
	sr *r;
};

int       sx_init(sxmanager*, sr*, sra*);
int       sx_free(sxmanager*);
int       sx_indexinit(sxindex*, void*);
int       sx_indexset(sxindex*, uint32_t, srscheme*);
int       sx_indexfree(sxindex*, sxmanager*);
sx       *sx_find(sxmanager*, uint32_t);
sxstate   sx_begin(sxmanager*, sx*, uint64_t);
void      sx_gc(sx*);
sxstate   sx_prepare(sx*, sxpreparef, void*);
sxstate   sx_commit(sx*);
sxstate   sx_rollback(sx*);
int       sx_set(sx*, sxindex*, svv*);
int       sx_get(sx*, sxindex*, sv*, sv*);
uint32_t  sx_min(sxmanager*);
uint32_t  sx_max(sxmanager*);
uint64_t  sx_vlsn(sxmanager*);
sxstate   sx_setstmt(sxmanager*, sxindex*, sv*);
sxstate   sx_getstmt(sxmanager*, sxindex*);

#endif
