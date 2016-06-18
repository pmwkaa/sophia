#ifndef SC_H_
#define SC_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct scdb scdb;
typedef struct sctask sctask;
typedef struct sc sc;

enum {
	SC_QBRANCH  = 0,
	SC_QGC      = 1,
	SC_QEXPIRE  = 2,
	SC_QLRU     = 3,
	SC_QBACKUP  = 4,
	SC_QMAX
};

struct scdb {
	uint32_t  workers[SC_QMAX];
	si       *index;
	/* state */
	uint64_t  checkpoint_lsn_last;
	uint64_t  checkpoint_lsn;
	uint32_t  checkpoint;
	uint32_t  age;
	uint64_t  age_time;
	uint32_t  expire;
	uint64_t  expire_time;
	uint64_t  anticache_asn;
	uint64_t  anticache_asn_last;
	uint64_t  anticache_storage;
	uint64_t  anticache_limit;
	uint64_t  anticache_time;
	uint64_t  anticache;
	uint64_t  snapshot_ssn;
	uint64_t  snapshot_ssn_last;
	uint64_t  snapshot_time;
	uint64_t  snapshot;
	uint64_t  gc_time;
	uint32_t  gc;
	uint64_t  lru_time;
	uint32_t  lru;
};

struct sctask {
	scdb      *db;
	scworker  *w;
	uint64_t   vlsn;
	uint64_t   time;
	int        rotate;
	int        gc;
	siplan     plan;
};

struct sc {
	ssmutex       lock;
	uint32_t      prio[SC_QMAX];
	/* backup state */
	uint32_t      backup_bsn;
	uint32_t      backup_bsn_last;
	uint32_t      backup_bsn_last_complete;
	uint32_t      backup;
	char         *backup_path;
	/* index */
	int           rotate;
	int           rr;
	int           count;
	scdb         *i;
	/* pools */
	ssthreadpool  tp;
	scworkerpool  wp;
	slpool       *lp;
	sr           *r;
};

int sc_init(sc*, sr*, slpool*);
int sc_set(sc*, uint32_t);
int sc_setbackup(sc*, char*);
int sc_run(sc*, ssthreadf, void*, int);
int sc_shutdown(sc*);

static inline void
sc_register(sc *s, si *index)
{
	int pos = index->scheme.id;
	assert(pos < s->count);
	s->i[pos].index = index;
}

static inline scdb*
sc_of(sc *s, si *index)
{
	int pos = index->scheme.id;
	assert(pos < s->count);
	return &s->i[pos];
}

#endif
