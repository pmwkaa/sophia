#ifndef SV_REF_H_
#define SV_REF_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct svref svref;

struct svref {
	svv      *v;
	svref    *next;
	uint8_t  flags;
	ssrbnode node;
} sspacked;

extern svif sv_refif;

static inline svref*
sv_refnew(sr *r, svv *v)
{
	svref *ref = ss_malloc(r->aref, sizeof(svref));
	if (ssunlikely(ref == NULL))
		return NULL;
	ref->v = v;
	ref->next = NULL;
	ref->flags = 0;
	memset(&ref->node, 0, sizeof(ref->node));
	return ref;
}

static inline void
sv_reffree(sr *r, svref *v)
{
	while (v) {
		svref *n = v->next;
		sv_vunref(r, v->v);
		ss_free(r->aref, v);
		v = n;
	}
}

static inline svref*
sv_refvisible(svref *v, uint64_t vlsn) {
	while (v && v->v->lsn > vlsn)
		v = v->next;
	return v;
}

#endif
