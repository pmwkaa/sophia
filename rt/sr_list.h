#ifndef SR_LIST_H_
#define SR_LIST_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srlist srlist;

struct srlist {
	srlist *next, *prev;
};

static inline void
sr_listinit(srlist *h) {
	h->next = h->prev = h;
}

static inline void
sr_listappend(srlist *h, srlist *n) {
	n->next = h;
	n->prev = h->prev;
	n->prev->next = n;
	n->next->prev = n;
}

static inline void
sr_listunlink(srlist *n) {
	n->prev->next = n->next;
	n->next->prev = n->prev;
}

static inline void
sr_listpush(srlist *h, srlist *n) {
	n->next = h->next;
	n->prev = h;
	n->prev->next = n;
	n->next->prev = n;
}

static inline srlist*
sr_listpop(srlist *h) {
	register srlist *pop = h->next;
	sr_listunlink(pop);
	return pop;
}

static inline int
sr_listempty(srlist *l) {
	return l->next == l && l->prev == l;
}

static inline void
sr_listmerge(srlist *a, srlist *b) {
	if (srunlikely(sr_listempty(b)))
		return;
	register srlist *first = b->next;
	register srlist *last = b->prev;
	first->prev = a->prev;
	a->prev->next = first;
	last->next = a;
	a->prev = last;
}

static inline void
sr_listreplace(srlist *o, srlist *n) {
	n->next = o->next;
	n->next->prev = n;
	n->prev = o->prev;
	n->prev->next = n;
}

#define sr_listlast(H, N) ((H) == (N))

#define sr_listforeach(H, I) \
	for (I = (H)->next; I != H; I = (I)->next)

#define sr_listforeach_continue(H, I) \
	for (; I != H; I = (I)->next)

#define sr_listforeach_safe(H, I, N) \
	for (I = (H)->next; I != H && (N = I->next); I = N)

#define sr_listforeach_reverse(H, I) \
	for (I = (H)->prev; I != H; I = (I)->prev)

#endif
