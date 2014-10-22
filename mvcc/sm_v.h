#ifndef SM_V_H_
#define SM_V_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct smv smv;

struct smv {
	uint32_t id, lo;
	svv *v;
	smv *next;
	smv *prev;
	srrbnode node;
} srpacked;

extern svif sm_vif;

static inline smv*
sm_valloc(sra *asmv, svv *v)
{
	smv *mv = sr_malloc(asmv, sizeof(smv));
	if (srunlikely(mv == NULL))
		return NULL;
	mv->id   = 0;
	mv->lo   = 0;
	mv->v    = v;
	mv->next = NULL;
	mv->prev = NULL;
	memset(&mv->node, 0, sizeof(mv->node));
	return mv;
}

static inline void
sm_vfree(sra *a, sra *asmv, smv *v)
{
	sr_free(a, v->v);
	sr_free(asmv, v);
}

static inline smv*
sm_vmatch(smv *head, uint32_t id) {
	smv *c = head;
	while (c) {
		if (c->id == id)
			break;
		c = c->next;
	}
	return c;
}

static inline void
sm_vreplace(smv *v, smv *n) {
	if (v->prev)
		v->prev->next = n;
	if (v->next)
		v->next->prev = n;
	n->next = v->next;
	n->prev = v->prev;
}

static inline void
sm_vlink(smv *head, smv *v) {
	smv *c = head;
	while (c->next)
		c = c->next;
	c->next = v;
	v->prev = c;
	v->next = NULL;
}

static inline void
sm_vunlink(smv *v) {
	if (v->prev)
		v->prev->next = v->next;
	if (v->next)
		v->next->prev = v->prev;
	v->prev = NULL;
	v->next = NULL;
}

static inline void
sm_vabortwaiters(smv *v) {
	smv *c = v->next;
	while (c) {
		c->v->flags |= SVABORT;
		c = c->next;
	}
}

#endif
