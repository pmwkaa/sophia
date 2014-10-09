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

typedef enum {
	SO_OFFLINE,
	SO_ONLINE,
	SO_RECOVER,
	SO_SHUTDOWN
} somode;

struct so {
	soobj o;
	soobjindex db;
	soobjindex ctlcursor;
	volatile somode mode;
	soctl ctl;
	sra a;
	srseq seq;
	sr r;
};

static inline int
so_active(so *o) {
	return o->mode != SO_OFFLINE &&
	       o->mode != SO_SHUTDOWN;
}

soobj *so_new(void);

#endif
