#ifndef SS_RB_H_
#define SS_RB_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssrbnode ssrbnode;
typedef struct ssrb  ssrb;

struct ssrbnode {
	ssrbnode *p, *l, *r;
	uint8_t color;
} sspacked;

struct ssrb {
	ssrbnode *root;
} sspacked;

static inline void
ss_rbinit(ssrb *t) {
	t->root = NULL;
}

static inline void
ss_rbinitnode(ssrbnode *n) {
	n->color = 2;
	n->p = NULL;
	n->l = NULL;
	n->r = NULL;
}

#define ss_rbget(name, compare) \
\
static inline int \
name(ssrb *t, \
     void *scheme ssunused, \
     void *key ssunused, int keysize ssunused, \
     ssrbnode **match) \
{ \
	ssrbnode *n = t->root; \
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

#define ss_rbtruncate(name, executable) \
\
static inline void \
name(ssrbnode *n, void *arg) \
{ \
	if (n->l) \
		name(n->l, arg); \
	if (n->r) \
		name(n->r, arg); \
	executable; \
}

ssrbnode *ss_rbmin(ssrb*);
ssrbnode *ss_rbmax(ssrb*);
ssrbnode *ss_rbnext(ssrb*, ssrbnode*);
ssrbnode *ss_rbprev(ssrb*, ssrbnode*);

void ss_rbset(ssrb*, ssrbnode*, int, ssrbnode*);
void ss_rbreplace(ssrb*, ssrbnode*, ssrbnode*);
void ss_rbremove(ssrb*, ssrbnode*);

#endif
