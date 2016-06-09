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
	uint32_t workers[SC_QMAX];
	si *index;
	uint32_t active;
};

struct sctask {
	siplan plan;
	scdb  *db;
	int    on_backup;
	int    rotate;
	int    gc;
};

struct sc {
	ssmutex       lock;
	uint64_t      checkpoint_lsn_last;
	uint64_t      checkpoint_lsn;
	uint32_t      checkpoint;
	uint32_t      age;
	uint64_t      age_time;
	uint32_t      expire;
	uint64_t      expire_time;
	uint64_t      anticache_asn;
	uint64_t      anticache_asn_last;
	uint64_t      anticache_storage;
	uint64_t      anticache_time;
	uint64_t      anticache_limit;
	uint64_t      anticache;
	uint64_t      snapshot_ssn;
	uint64_t      snapshot_ssn_last;
	uint64_t      snapshot_time;
	uint64_t      snapshot;
	uint64_t      gc_time;
	uint32_t      gc;
	uint64_t      lru_time;
	uint32_t      lru;
	uint32_t      backup_bsn;
	uint32_t      backup_bsn_last;
	uint32_t      backup_bsn_last_complete;
	uint32_t      backup_events;
	uint32_t      backup;
	int           rotate;
	int           rr;
	int           count;
	scdb         *i;
	ssthreadpool  tp;
	scworkerpool  wp;
	slpool       *lp;
	char         *backup_path;
	sstrigger    *on_event;
	sr           *r;
};

int sc_init(sc*, sr*, sstrigger*, slpool*);
int sc_set(sc*, uint32_t, uint64_t, char*);
int sc_create(sc*, ssthreadf, void*, int);
int sc_shutdown(sc*);

static inline void
sc_register(sc *s, si *index)
{
	int pos = index->scheme.id;
	assert(pos < s->count);
	s->i[pos].index = index;
}

static inline void
sc_start(sc *s, int task)
{
	int i = 0;
	while (i < s->count) {
		s->i[i].active |= task;
		i++;
	}
}

static inline int
sc_end(sc *s, scdb *db, int task)
{
	db->active &= ~task;
	int complete = 1;
	int i = 0;
	while (i < s->count) {
		if (s->i[i].active & task)
			complete = 0;
		i++;
	}
	return complete;
}

#endif
