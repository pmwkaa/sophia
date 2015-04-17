#ifndef SR_RB_H_
#define SR_RB_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srrbnode srrbnode;
typedef struct srrb  srrb;

struct srrbnode {
	srrbnode *p, *l, *r;
	uint8_t color;
} srpacked;

struct srrb {
	srrbnode *root;
} srpacked;

static inline void
sr_rbinit(srrb *t) {
	t->root = NULL;
}

static inline void
sr_rbinitnode(srrbnode *n) {
	n->color = 2;
	n->p = NULL;
	n->l = NULL;
	n->r = NULL;
}

#define sr_rbget(name, compare) \
\
static inline int \
name(srrb *t, \
     srkey *cmp srunused, \
     void *key srunused, int keysize srunused, \
     srrbnode **match) \
{ \
	srrbnode *n = t->root; \
	*match = NULL; \
	int rc = 0; \
	while (n) { \
		*match = n; \
		switch ((rc = (compare))) { \
		case  0: return 0; \
		case -1: n = n->r; \
			break; \
		case  1: n = n->l; \
			break; \
		} \
	} \
	return rc; \
}

#define sr_rbtruncate(name, executable) \
\
static inline void \
name(srrbnode *n, void *arg) \
{ \
	if (n->l) \
		name(n->l, arg); \
	if (n->r) \
		name(n->r, arg); \
	executable; \
}

srrbnode *sr_rbmin(srrb*);
srrbnode *sr_rbmax(srrb*);
srrbnode *sr_rbnext(srrb*, srrbnode*);
srrbnode *sr_rbprev(srrb*, srrbnode*);

void sr_rbset(srrb*, srrbnode*, int, srrbnode*);
void sr_rbreplace(srrb*, srrbnode*, srrbnode*);
void sr_rbremove(srrb*, srrbnode*);

#endif
