#ifndef SE_H_
#define SE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct se se;

struct se {
	so          o;
	sestatus    status;
	ssmutex     apilock;
	ssmutex     reqlock;
	sscond      reqcond;
	ssspinlock  dblock;
	solist      db;
	solist      db_shutdown;
	solist      cursor;
	solist      tx;
	solist      req;
	solist      reqactive;
	solist      reqready;
	solist      snapshot;
	solist      metacursor;
	srseq       seq;
	semeta      meta;
	ssquota     quota;
	sspager     pager;
	ssa         a_oom;
	ssa         a;
	ssa         a_db;
	ssa         a_v;
	ssa         a_cursor;
	ssa         a_cachebranch;
	ssa         a_cache;
	ssa         a_metacursor;
	ssa         a_metav;
	ssa         a_snapshot;
	ssa         a_snapshotcursor;
	ssa         a_batch;
	ssa         a_tx;
	ssa         a_sxv;
	ssa         a_req;
	sicachepool cachepool;
	syconf      repconf;
	sy          rep;
	slconf      lpconf;
	slpool      lp;
	sxmanager   xm;
	sescheduler sched;
	srerror     error;
	srstat      stat;
	ssinjection ei;
	sr          r;
};

static inline int
se_active(se *e) {
	return se_statusactive(&e->status);
}

static inline void
se_apilock(so *o) {
	ss_mutexlock(&((se*)o)->apilock);
}

static inline void
se_apiunlock(so *o) {
	ss_mutexunlock(&((se*)o)->apilock);
}

static inline se *se_of(so *o) {
	return (se*)o->env;
}

so *se_new(void);

#endif
