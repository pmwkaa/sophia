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
	somode mode;
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
	sr r;
	so *e;
};

static inline int
so_dbactive(sodb *o) {
	return o->mode != SO_OFFLINE &&
	       o->mode != SO_SHUTDOWN;
}

soobj *so_dbnew(so*, char*);
soobj *so_dbmatch(so*, char*);

#endif
