#ifndef SI_TRACK_H_
#define SI_TRACK_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sitrack sitrack;

struct sitrack {
	srrb i;
	int count;
	uint32_t nsn;
	uint64_t lsn;
};

static inline void
si_trackinit(sitrack *t) {
	sr_rbinit(&t->i);
	t->count = 0;
	t->nsn = 0;
	t->lsn = 0;
}

sr_rbtruncate(si_tracktruncate,
              si_nodefree_all(srcast(n, sinode, node), (sr*)arg))

static inline void
si_trackfree(sitrack *t, sr *r) {
	if (t->i.root)
		si_tracktruncate(t->i.root, r);
}

static inline void
si_tracklsn(sitrack *t, sinode *n)
{
	sdindexheader *h = n->index.h;
	if (h->lsnmin > t->lsn)
		t->lsn = h->lsnmin;
	if (h->lsnmax > t->lsn)
		t->lsn = h->lsnmax;
}

static inline void
si_tracknsn(sitrack *t, uint32_t nsn)
{
	if (t->nsn < nsn)
		t->nsn = nsn;
}

sr_rbget(si_trackmatch,
         sr_cmpu32((char*)&(srcast(n, sinode, node))->id.id, sizeof(uint32_t),
                   (char*)key, sizeof(uint32_t), NULL))

static inline void
si_trackset(sitrack *t, sinode *n)
{
	srrbnode *p = NULL;
	int rc = si_trackmatch(&t->i, NULL, (char*)&n->id, sizeof(n->id), &p);
	assert(! (rc == 0 && p));
	sr_rbset(&t->i, p, rc, &n->node);
	t->count++;
}

static inline sinode*
si_trackget(sitrack *t, uint32_t id)
{
	srrbnode *p = NULL;
	int rc = si_trackmatch(&t->i, NULL, (char*)&id, sizeof(id), &p);
	if (rc == 0 && p)
		return srcast(p, sinode, node);
	return NULL;
}

static inline void
si_trackreplace(sitrack *t, sinode *o, sinode *n)
{
	sr_rbreplace(&t->i, &o->node, &n->node);
}

static inline void
si_trackremove(sitrack *t, sinode *n)
{
	sr_rbremove(&t->i, &n->node);
	t->count--;
}

#endif
