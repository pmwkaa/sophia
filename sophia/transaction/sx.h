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
	SX_UNDEF,
	SX_ROLLBACK,
	SX_LOCK,
	SX_READY,
	SX_COMMIT,
	SX_PREPARE
} sxstate;

typedef enum {
	SX_SERIALIZABLE,
	SX_BATCH
} sxisolation;

typedef enum {
	SX_RO,
	SX_RW
} sxtype;

struct sxindex {
	ssrb      i;
	uint32_t  dsn;
	so       *object;
	sr       *r;
	sslist    link;
};

typedef int (*sxpreparef)(sx*, sv*, so*, void*);

struct sx {
	sxtype       type;
	sxisolation  isolation;
	sxstate      state;
	uint64_t     id;
	uint64_t     vlsn;
	uint64_t     csn;
	int          log_read;
	svlog       *log;
	sslist       deadlock;
	ssrbnode     node;
	sxmanager   *manager;
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
	sxvpool     pool;
	srseq      *seq;
};

int       sx_managerinit(sxmanager*, srseq*, ssa*);
int       sx_managerfree(sxmanager*);
int       sx_indexinit(sxindex*, sxmanager*, sr*, so*);
int       sx_indexset(sxindex*, uint32_t);
int       sx_indexfree(sxindex*, sxmanager*);
sx       *sx_find(sxmanager*, uint64_t);
void      sx_init(sxmanager*, sx*, svlog*);
sxstate   sx_begin(sxmanager*, sx*, sxtype, svlog*, uint64_t);
void      sx_gc(sx*);
sxstate   sx_prepare(sx*, sxpreparef, void*);
sxstate   sx_commit(sx*);
sxstate   sx_rollback(sx*);
int       sx_isolation(sx*, char*, int);
int       sx_set(sx*, sxindex*, svv*);
int       sx_get(sx*, sxindex*, sv*, sv*);
uint64_t  sx_vlsn(sxmanager*);
sxstate   sx_set_autocommit(sxmanager*, sxindex*, sx*, svlog*, svv*);
sxstate   sx_get_autocommit(sxmanager*, sxindex*);

#endif
