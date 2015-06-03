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

struct sov {
	srobj o;
	uint8_t flags;
	sv v;
	ssorder order;
	sfv keyv[8];
	int keyc;
	uint16_t keysize;
	void *value;
	uint32_t valuesize;
	void *raw;
	uint32_t rawsize;
	void *prefix;
	uint16_t prefixsize;
	void *log;
	srobj *parent;
} sspacked;

static inline int
so_vhas(sov *v) {
	return v->v.v != NULL;
}

srobj *so_vnew(so*, srobj*);
srobj *so_vdup(so*, srobj*, sv*);
srobj *so_vinit(sov*, so*, srobj*);
srobj *so_vrelease(sov*);
srobj *so_vput(sov*, sv*);

#endif
