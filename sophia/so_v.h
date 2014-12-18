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

#define SO_VALLOCATED 1
#define SO_VRO        2
#define SO_VIMMUTABLE 4

struct sov {
	soobj o;
	uint8_t flags;
	svlocal lv;
	sv v;
	srorder order;
	void *log;
	soobj *parent;
	so *e;
} srpacked;

static inline int
so_vhas(sov *v) {
	return v->v.v != NULL;
}

soobj *so_vnew(so*, soobj*);
soobj *so_vdup(so*, soobj*, sv*);
soobj *so_vinit(sov*, so*, soobj*);
soobj *so_vrelease(sov*);
soobj *so_vput(sov*, sv*);
int    so_vimmutable(sov*);

#endif
