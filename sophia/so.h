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
	soobjindex tx;
	soobjindex ctlcursor;
	soobjindex snapshot;
	sostatus status;
	soctl ctl;
	srseq seq;
	srquota quota;
	srpager pager;
	sra a;
	sra a_db;
	sra a_v;
	sra a_cursor;
	sra a_cursorcache;
	sra a_ctlcursor;
	sra a_logcursor;
	sra a_snapshot;
	sra a_tx;
	sra a_sxv;
	seconf seconf;
	se se;
	slconf lpconf;
	slpool lp;
	sxmanager xm;
	soscheduler sched;
	srerror error;
	srinjection ei;
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
