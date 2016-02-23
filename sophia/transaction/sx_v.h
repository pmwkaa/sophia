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
typedef struct sxvpool sxvpool;

struct sxv {
	uint64_t id;
	uint32_t lo;
	uint64_t csn;
	void *index;
	svv *v;
	sxv *next;
	sxv *prev;
	sxv *gc;
	ssrbnode node;
} sspacked;

struct sxvpool {
	sxv *head;
	int n;
	sr *r;
};

static inline void
sx_vpool_init(sxvpool *p, sr *r)
{
	p->head = NULL;
	p->n = 0;
	p->r = r;
}

static inline void
sx_vpool_free(sxvpool *p)
{
	sxv *n, *c = p->head;
	while (c) {
		n = c->next;
		ss_free(p->r->a, c);
		c = n;
	}
}

static inline sxv*
sx_vpool_pop(sxvpool *p)
{
	if (ssunlikely(p->n == 0))
		return NULL;
	sxv *v = p->head;
	p->head = v->next;
	p->n--;
	return v;
}

static inline void
sx_vpool_push(sxvpool *p, sxv *v)
{
	v->v    = NULL;
	v->next = NULL;
	v->prev = NULL;
	v->next = p->head;
	p->head = v;
	p->n++;
}

static inline sxv*
sx_valloc(sxvpool *p, svv *ref)
{
	sxv *v = sx_vpool_pop(p);
	if (ssunlikely(v == NULL)) {
		v = ss_malloc(p->r->a, sizeof(sxv));
		if (ssunlikely(v == NULL))
			return NULL;
	}
	v->index = NULL;
	v->id    = 0;
	v->lo    = 0;
	v->csn   = 0;
	v->v     = ref;
	v->next  = NULL;
	v->prev  = NULL;
	v->gc    = NULL;
	memset(&v->node, 0, sizeof(v->node));
	return v;
}

static inline void
sx_vfree(sxvpool *p, sxv *v)
{
	sv_vunref(p->r, v->v);
	sx_vpool_push(p, v);
}

static inline void
sx_vfreeall(sxvpool *p, sxv *v)
{
	while (v) {
		sxv *next = v->next;
		sx_vfree(p, v);
		v = next;
	}
}

static inline sxv*
sx_vmatch(sxv *head, uint64_t id)
{
	sxv *c = head;
	while (c) {
		if (c->id == id)
			break;
		c = c->next;
	}
	return c;
}

static inline void
sx_vreplace(sxv *v, sxv *n)
{
	if (v->prev)
		v->prev->next = n;
	if (v->next)
		v->next->prev = n;
	n->next = v->next;
	n->prev = v->prev;
}

static inline void
sx_vlink(sxv *head, sxv *v)
{
	sxv *c = head;
	while (c->next)
		c = c->next;
	c->next = v;
	v->prev = c;
	v->next = NULL;
}

static inline void
sx_vunlink(sxv *v)
{
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
	v->id  = UINT64_MAX;
	v->lo  = UINT32_MAX;
	v->csn = csn;
}

static inline int
sx_vcommitted(sxv *v)
{
	return v->id == UINT64_MAX && v->lo == UINT32_MAX;
}

static inline void
sx_vabort(sxv *v)
{
	v->v->flags |= SVCONFLICT;
}

static inline void
sx_vabort_all(sxv *v)
{
	while (v) {
		sx_vabort(v);
		v = v->next;
	}
}

static inline int
sx_vaborted(sxv *v)
{
	return v->v->flags & SVCONFLICT;
}

extern svif sx_vif;

#endif
