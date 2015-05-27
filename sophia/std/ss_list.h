#ifndef SS_LIST_H_
#define SS_LIST_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sslist sslist;

struct sslist {
	sslist *next, *prev;
};

static inline void
ss_listinit(sslist *h) {
	h->next = h->prev = h;
}

static inline void
ss_listappend(sslist *h, sslist *n) {
	n->next = h;
	n->prev = h->prev;
	n->prev->next = n;
	n->next->prev = n;
}

static inline void
ss_listunlink(sslist *n) {
	n->prev->next = n->next;
	n->next->prev = n->prev;
}

static inline void
ss_listpush(sslist *h, sslist *n) {
	n->next = h->next;
	n->prev = h;
	n->prev->next = n;
	n->next->prev = n;
}

static inline sslist*
ss_listpop(sslist *h) {
	register sslist *pop = h->next;
	ss_listunlink(pop);
	return pop;
}

static inline int
ss_listempty(sslist *l) {
	return l->next == l && l->prev == l;
}

static inline void
ss_listmerge(sslist *a, sslist *b) {
	if (ssunlikely(ss_listempty(b)))
		return;
	register sslist *first = b->next;
	register sslist *last = b->prev;
	first->prev = a->prev;
	a->prev->next = first;
	last->next = a;
	a->prev = last;
}

static inline void
ss_listreplace(sslist *o, sslist *n) {
	n->next = o->next;
	n->next->prev = n;
	n->prev = o->prev;
	n->prev->next = n;
}

#define ss_listlast(H, N) ((H) == (N))

#define ss_listforeach(H, I) \
	for (I = (H)->next; I != H; I = (I)->next)

#define ss_listforeach_continue(H, I) \
	for (; I != H; I = (I)->next)

#define ss_listforeach_safe(H, I, N) \
	for (I = (H)->next; I != H && (N = I->next); I = N)

#define ss_listforeach_reverse(H, I) \
	for (I = (H)->prev; I != H; I = (I)->prev)

#endif
