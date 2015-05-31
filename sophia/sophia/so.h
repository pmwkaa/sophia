#ifndef SO_H_
#define SO_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct soasync soasync;
typedef struct so so;

struct soasync {
	srobj o;
};

struct so {
	srobj o;
	ssmutex apilock;
	ssspinlock reqlock;
	ssspinlock dblock;
	srobjlist db;
	srobjlist db_shutdown;
	srobjlist tx;
	srobjlist req;
	srobjlist reqready;
	srobjlist snapshot;
	srobjlist ctlcursor;
	sostatus status;
	soctl ctl;
	soasync async;
	srseq seq;
	ssquota quota;
	sspager pagersx;
	sspager pager;
	ssa a;
	ssa a_db;
	ssa a_v;
	ssa a_cursor;
	ssa a_cachebranch;
	ssa a_cache;
	ssa a_ctlcursor;
	ssa a_snapshot;
	ssa a_tx;
	ssa a_sxv;
	ssa a_req;
	sicachepool cachepool;
	seconf seconf;
	se se;
	slconf lpconf;
	slpool lp;
	sxmanager xm;
	soscheduler sched;
	ssinjection ei;
	srerror error;
	sr r;
};

static inline int
so_active(so *o) {
	return so_statusactive(&o->status);
}

static inline void
so_apilock(srobj *o) {
	ss_mutexlock(&((so*)o)->apilock);
}

static inline void
so_apiunlock(srobj *o) {
	ss_mutexunlock(&((so*)o)->apilock);
}

static inline so*
so_of(srobj *o) {
	return (so*)o->env;
}

srobj *so_new(void);

#endif
