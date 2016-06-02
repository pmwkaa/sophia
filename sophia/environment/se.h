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
	sopool      document;
	sopool      cursor;
	sopool      tx;
	sopool      confcursor;
	sopool      confcursor_kv;
	sopool      view;
	sopool      viewdb;
	solist      db;
	srseq       seq;
	seconf      conf;
	srquota     quota;
	ssvfs       vfs;
	ssa         a_oom;
	ssa         a;
	ssa         a_ref;
	sicachepool cachepool;
	syconf      repconf;
	sy          rep;
	slconf      lpconf;
	slpool      lp;
	sxmanager   xm;
	sc          scheduler;
	srlog       log;
	srerror     error;
	srstat      stat;
	sflimit     limit;
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
