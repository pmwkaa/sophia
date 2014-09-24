#ifndef SM_V_H_
#define SM_V_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

static inline svv*
sm_vmatch(svv *head, uint32_t id) {
	register svv *c = head;
	while (c) {
		if (c->id.tx.id == id)
			break;
		c = c->next;
	}
	return c;
}

static inline void
sm_vreplace(svv *v, svv *n) {
	if (v->prev)
		v->prev->next = n;
	if (v->next)
		v->next->prev = n;
	n->next = v->next;
	n->prev = v->prev;
}

static inline void
sm_vlink(svv *head, svv *v) {
	register svv *c = head;
	while (c->next)
		c = c->next;
	c->next = v;
	v->prev = c;
}

static inline void
sm_vunlink(svv *v) {
	if (v->prev)
		v->prev->next = v->next;
	if (v->next)
		v->next->prev = v->prev;
	v->prev = NULL;
	v->next = NULL;
}

static inline void
sm_vabortwaiters(svv *v) {
	register svv *c = v->next;
	while (c) {
		c->flags |= SVABORT;
		c = c->next;
	}
}

#endif
