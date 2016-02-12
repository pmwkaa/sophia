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
	srstatus    status;
	ssmutex     apilock;
	solist      db;
	solist      cursor;
	solist      viewdb;
	solist      view;
	solist      tx;
	solist      confcursor;
	srseq       seq;
	seconf      conf;
	ssquota     quota;
	sspager     pager;
	ssvfs       vfs;
	ssa         a_oom;
	ssa         a;
	ssa         a_ref;
	ssa         a_document;
	ssa         a_cursor;
	ssa         a_viewdb;
	ssa         a_view;
	ssa         a_cachebranch;
	ssa         a_cache;
	ssa         a_confcursor;
	ssa         a_confkv;
	ssa         a_tx;
	ssa         a_sxv;
	sicachepool cachepool;
	syconf      repconf;
	sy          rep;
	slconf      lpconf;
	slpool      lp;
	sxmanager   xm;
	sc          scheduler;
	srerror     error;
	srstat      stat;
	ssinjection ei;
	sr          r;
};

static inline int
se_active(se *e) {
	return sr_statusactive(&e->status);
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

so  *se_new(void);
int  se_service_threads(se*, int);
int  se_service(so*);

#endif
