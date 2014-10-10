#ifndef SO_DB_H_
#define SO_DB_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sodb sodb;

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
	slconf lpconf;
	slpool lp;
	soworkers workers;
	srinjection ei;
	srerror error;
	sr r;
	so *e;
};

static inline int
so_dbactive(sodb *o) {
	return so_statusactive(o->status);
}

soobj *so_dbnew(so*, char*);
soobj *so_dbmatch(so*, char*);

#endif
