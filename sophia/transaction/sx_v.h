#ifndef SX_V_H_
#define SX_V_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sxv sxv;

struct sxv {
	uint32_t id, lo;
	uint64_t csn;
	void *index;
	svv *v;
	sxv *next;
	sxv *prev;
	sxv *gc;
	ssrbnode node;
} sspacked;

extern svif sx_vif;

static inline sxv*
sx_valloc(ssa *asxv, svv *v)
{
	sxv *vv = ss_malloc(asxv, sizeof(sxv));
	if (ssunlikely(vv == NULL))
		return NULL;
	vv->index = NULL;
	vv->id    = 0;
	vv->lo    = 0;
	vv->csn   = 0;
	vv->v     = v;
	vv->next  = NULL;
	vv->prev  = NULL;
	vv->gc    = NULL;
	memset(&vv->node, 0, sizeof(vv->node));
	return vv;
}

static inline void
sx_vfree(ssa *a, ssa *asxv, sxv *v)
{
	sv_vfree(a, v->v);
	ss_free(asxv, v);
}

static inline void
sx_vfreeall(ssa *a, ssa *asxv, sxv *v)
{
	while (v) {
		sxv *next = v->next;
		sv_vfree(a, v->v);
		ss_free(asxv, v);
		v = next;
	}
}

static inline sxv*
sx_vmatch(sxv *head, uint32_t id) {
	sxv *c = head;
	while (c) {
		if (c->id == id)
			break;
		c = c->next;
	}
	return c;
}

static inline void
sx_vreplace(sxv *v, sxv *n) {
	if (v->prev)
		v->prev->next = n;
	if (v->next)
		v->next->prev = n;
	n->next = v->next;
	n->prev = v->prev;
}

static inline void
sx_vlink(sxv *head, sxv *v) {
	sxv *c = head;
	while (c->next)
		c = c->next;
	c->next = v;
	v->prev = c;
	v->next = NULL;
}

static inline void
sx_vunlink(sxv *v) {
	if (v->prev)
		v->prev->next = v->next;
	if (v->next)
		v->next->prev = v->prev;
	v->prev = NULL;
	v->next = NULL;
}

static inline void
sx_vcommit(sxv *v, uint32_t csn)
{
	v->id  = UINT32_MAX;
	v->lo  = UINT32_MAX;
	v->csn = csn;
}

static inline int
sx_vcommitted(sxv *v)
{
	return v->id == UINT32_MAX && v->lo == UINT32_MAX;
}

#endif
