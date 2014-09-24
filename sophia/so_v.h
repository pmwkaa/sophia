#ifndef SO_V_H_
#define SO_V_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sov sov;

struct sov {
	soobj o;
	uint8_t allocated;
	svlocal lv;
	sv v;
	so *e;
} srpacked;

static inline int
so_vhas(sov *v) {
	return v->v.v != NULL;
}

soobj *so_vnew(so*);
soobj *so_vdup(so*, sv*);
soobj *so_vinit(sov*, so*);
soobj *so_vrelease(sov*);
soobj *so_vput(sov*, sv*);

#endif
