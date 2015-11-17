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

typedef enum {
	SXRO,
	SXRW
} sxtype;

struct sxindex {
	ssrb      i;
	uint32_t  dsn;
	void     *ptr;
	sr       *r;
	sslist    link;
};

typedef int (*sxpreparef)(sx*, sv*, void*, void*);

struct sx {
	sxtype     type;
	sxstate    state;
	uint64_t   id;
	uint64_t   vlsn;
	uint64_t   csn;
	int        log_read;
	svlog      log;
	sslist     deadlock;
	ssrbnode   node;
	sxmanager *manager;
};

struct sxmanager {
	ssspinlock  lock;
	sslist      indexes;
	ssrb        i;
	uint32_t    count_rd;
	uint32_t    count_rw;
	uint32_t    count_gc;
	uint64_t    csn;
	sxv        *gc;
	ssa        *asxv;
	sr         *r;
};

int       sx_managerinit(sxmanager*, sr*, ssa*);
int       sx_managerfree(sxmanager*);
int       sx_indexinit(sxindex*, sxmanager*, sr*, void*);
int       sx_indexset(sxindex*, uint32_t);
int       sx_indexfree(sxindex*, sxmanager*);
sx       *sx_find(sxmanager*, uint64_t);
void      sx_init(sxmanager*, sx*);
sxstate   sx_begin(sxmanager*, sx*, sxtype, uint64_t);
void      sx_gc(sx*);
sxstate   sx_prepare(sx*, sxpreparef, void*);
sxstate   sx_commit(sx*);
sxstate   sx_rollback(sx*);
int       sx_set(sx*, sxindex*, svv*);
int       sx_get(sx*, sxindex*, sv*, sv*);
uint64_t  sx_min(sxmanager*);
uint64_t  sx_max(sxmanager*);
uint64_t  sx_vlsn(sxmanager*);
sxstate   sx_set_autocommit(sxmanager*, sxindex*, sx*, svv*);
sxstate   sx_get_autocommit(sxmanager*, sxindex*);

#endif
