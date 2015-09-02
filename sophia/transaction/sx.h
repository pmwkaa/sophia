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

struct sxindex {
	ssrb i;
	uint32_t dsn;
	srscheme *scheme;
	void *ptr;
	sr *r;
	sslist link;
};

struct sx {
	uint32_t id;
	uint64_t csn;
	uint64_t vlsn;
	int complete;
	sxstate s;
	svlog log;
	sslist deadlock;
	sxmanager *manager;
	ssrbnode node;
};

struct sxmanager {
	ssspinlock lock;
	sslist indexes;
	ssrb i;
	uint64_t csn;
	uint32_t count;
	ssa *asxv;
	ssa *a;
	srseq *seq;
};

int       sx_managerinit(sxmanager*, srseq*, ssa*, ssa*);
int       sx_managerfree(sxmanager*);
int       sx_indexinit(sxindex*, sxmanager*, sr*, void*);
int       sx_indexset(sxindex*, uint32_t, srscheme*);
int       sx_indexfree(sxindex*, sxmanager*);
sx       *sx_find(sxmanager*, uint32_t);
void      sx_init(sxmanager*, sx*);
sxstate   sx_begin(sxmanager*, sx*, uint64_t);
void      sx_gc(sx*);
sxstate   sx_prepare(sx*);
sxstate   sx_complete(sx*);
sxstate   sx_commit(sx*);
sxstate   sx_rollback(sx*, sr*);
int       sx_set(sx*, sxindex*, svv*);
int       sx_get(sx*, sxindex*, sv*, sv*);
uint32_t  sx_min(sxmanager*);
uint32_t  sx_max(sxmanager*);
uint64_t  sx_vlsn(sxmanager*);

sxstate   sx_getstmt(sxmanager*, sxindex*);

#endif
