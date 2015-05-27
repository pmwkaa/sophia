#ifndef SI_BRANCH_H_
#define SI_BRANCH_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sibranch sibranch;

struct sibranch {
	sdid id;
	sdindex index;
	sibranch *next;
};

static inline void
si_branchinit(sibranch *b) {
	memset(&b->id, 0, sizeof(b->id));
	sd_indexinit(&b->index);
	b->next = NULL;
}

static inline sibranch*
si_branchnew(sr *r)
{
	sibranch *b = (sibranch*)ss_malloc(r->a, sizeof(sibranch));
	if (ssunlikely(b == NULL)) {
		sr_malfunction(r->e, "%s", "memory allocation failed");
		return NULL;
	}
	si_branchinit(b);
	return b;
}

static inline void
si_branchset(sibranch *b, sdindex *i)
{
	b->id = i->h->id;
	b->index = *i;
}

static inline void
si_branchfree(sibranch *b, sr *r)
{
	sd_indexfree(&b->index, r);
	ss_free(r->a, b);
}

#endif
