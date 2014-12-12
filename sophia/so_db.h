#ifndef SO_DB_H_
#define SO_DB_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sodbctl sodbctl;
typedef struct sodb sodb;

struct sodbctl {
	void         *parent;
	char         *name;
	uint32_t      id;
	srcomparator  cmp;
	char         *path;
	uint32_t      created;
	uint32_t      sync;
	siprofiler    rtp;
} srpacked;

struct sodb {
	soobj o;
	sostatus status;
	sodbctl ctl;
	soobjindex tx;
	soobjindex cursor;
	sm mvcc;
	sdc dc;
	siconf indexconf;
	si index;
	sr r;
	so *e;
} srpacked;

static inline int
so_dbactive(sodb *o) {
	return so_statusactive(&o->status);
}

soobj    *so_dbnew(so*, char*);
soobj    *so_dbmatch(so*, char*);
soobj    *so_dbmatch_id(so*, uint32_t);
int       so_dbmalfunction(sodb *o);
uint64_t  so_dbvlsn(sodb*);

#endif
