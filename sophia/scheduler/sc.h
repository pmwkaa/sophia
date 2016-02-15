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
	SC_QLRU     = 2,
	SC_QBACKUP  = 3,
	SC_QNONE
};

struct scdb {
	uint32_t workers[SC_QNONE];
	si *index;
	so *db;
};

struct sctask {
	siplan plan;
	scdb  *db;
	si    *shutdown;
	int    rotate;
	int    read;
	int    gc;
	int    checkpoint_complete;
	int    anticache_complete;
	int    snapshot_complete;
	int    backup_complete;
};

struct sc {
	ssmutex        lock;
	uint64_t       checkpoint_lsn_last;
	uint64_t       checkpoint_lsn;
	uint32_t       checkpoint;
	uint32_t       age;
	uint64_t       age_time;
	uint64_t       anticache_asn;
	uint64_t       anticache_asn_last;
	uint64_t       anticache_storage;
	uint64_t       anticache_time;
	uint64_t       anticache_limit;
	uint64_t       anticache;
	uint64_t       snapshot_ssn;
	uint64_t       snapshot_ssn_last;
	uint64_t       snapshot_time;
	uint64_t       snapshot;
	uint64_t       gc_time;
	uint32_t       gc;
	uint64_t       lru_time;
	uint32_t       lru;
	uint32_t       backup_bsn;
	uint32_t       backup_bsn_last;
	uint32_t       backup_bsn_last_complete;
	uint32_t       backup_events;
	uint32_t       backup;
	int            shutdown_pending;
	int            rotate;
	int            read;
	int            rr;
	int            count;
	scdb         **i;
	solist         shutdown;
	ssthreadpool   tp;
	scworkerpool   wp;
	screadpool     rp;
	slpool        *lp;
	char          *backup_path;
	sstrigger     *on_event;
	sr            *r;
};

int sc_init(sc*, sr*, sstrigger*, slpool*);
int sc_set(sc *s, uint64_t, char*);
int sc_create(sc *s, ssthreadf, void*, int);
int sc_shutdown(sc*);
int sc_add(sc*, so*, si*);
int sc_del(sc*, so*, int);

#endif
