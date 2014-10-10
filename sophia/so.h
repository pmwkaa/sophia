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
	volatile sostatus status;
	soctl ctl;
	sra a;
	srseq seq;
	srerror error;
	sr r;
};

static inline int
so_active(so *o) {
	return so_statusactive(o->status);
}

soobj *so_new(void);

#endif
