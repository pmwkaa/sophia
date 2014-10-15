#ifndef SO_H_
#define SO_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct so so;

struct so {
	soobj o;
	srmutex apilock;
	soobjindex db;
	soobjindex ctlcursor;
	sostatus status;
	soctl ctl;
	srseq seq;
	srpager pager;
	sra a;
	sra a_db;
	sra a_v;
	sra a_cursor;
	sra a_ctlcursor;
	sra a_tx;
	srerror error;
	sr r;
};

static inline int
so_active(so *o) {
	return so_statusactive(&o->status);
}

static inline void
so_apilock(soobj *o) {
	sr_mutexlock(&((so*)o)->apilock);
}

static inline void
so_apiunlock(soobj *o) {
	sr_mutexunlock(&((so*)o)->apilock);
}

soobj *so_new(void);

#endif
