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
	void *index;
	svv *v;
	sxv *next;
	sxv *prev;
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
	vv->v     = v;
	vv->next  = NULL;
	vv->prev  = NULL;
	memset(&vv->node, 0, sizeof(vv->node));
	return vv;
}

static inline void
sx_vfree(ssa *a, ssa *asxv, sxv *v)
{
	ss_free(a, v->v);
	ss_free(asxv, v);
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
sx_vabortwaiters(sxv *v) {
	sxv *c = v->next;
	while (c) {
		c->v->flags |= SVABORT;
		c = c->next;
	}
}

#endif
