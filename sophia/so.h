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
	soobjindex db;
	soobjindex ctlcursor;
	sostatus status;
	soctl ctl;
	sra a;
	srseq seq;
	srmutex apilock;
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
