#ifndef SI_CACHE_H_
#define SI_CACHE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sicachebranch sicachebranch;
typedef struct sicache sicache;

struct sicachebranch {
	sibranch *branch;
	sdindexpage *ref;
	sriter i;
	srbuf buf;
	int iterate;
	sicachebranch *next;
} srpacked;

struct sicache {
	sra *ac;
	sicachebranch *path;
	sicachebranch *branch;
	uint32_t count;
	uint32_t nodeid;
	sinode *node;
};

static inline void
si_cacheinit(sicache *c, sra *ac)
{
	c->path   = NULL;
	c->branch = NULL;
	c->count  = 0;
	c->node   = NULL;
	c->nodeid = 0;
	c->ac     = ac;
}

static inline void
si_cachefree(sicache *c, sr *r)
{
	sicachebranch *next;
	sicachebranch *cb = c->path;
	while (cb) {
		next = cb->next;
		sr_buffree(&cb->buf, r->a);
		sr_free(c->ac, cb);
		cb = next;
	}
}

static inline void
si_cachereset(sicache *c)
{
	sicachebranch *cb = c->path;
	while (cb) {
		sr_bufreset(&cb->buf);
		cb->branch = NULL;
		cb->ref = NULL;
		cb->iterate = 0;
		cb = cb->next;
	}
	c->branch = NULL;
	c->node   = NULL;
	c->nodeid = 0;
	c->count  = 0;
}

static inline sicachebranch*
si_cacheadd(sicache *c, sibranch *b)
{
	sicachebranch *nb = sr_malloc(c->ac, sizeof(sicachebranch));
	if (srunlikely(nb == NULL))
		return NULL;
	nb->branch  = b;
	nb->ref     = NULL;
	nb->iterate = 0;
	nb->next    = NULL;
	sr_bufinit(&nb->buf);
	return nb;
}

static inline int
si_cachevalidate(sicache *c, sinode *n)
{
	if (srlikely(c->node == n && c->nodeid == n->self.id.id))
	{
		if (srlikely(n->branch_count == c->count)) {
			c->branch = c->path;
			return 0;
		}
		assert(n->branch_count > c->count);
		/* c b a */
		/* e d c b a */
		sicachebranch *head = NULL;
		sicachebranch *last = NULL;
		sicachebranch *cb = c->path;
		sibranch *b = n->branch;
		while (b) {
			if (cb->branch == b) {
				assert(last != NULL);
				last->next = cb;
				break;
			}
			sicachebranch *nb = si_cacheadd(c, b);
			if (srunlikely(nb == NULL))
				return -1;
			if (! head)
				head = nb;
			if (last)
				last->next = nb;
			last = nb;
			b = b->next;
		}
		c->path   = head;
		c->count  = n->branch_count;
		c->branch = c->path;
		return 0;
	}
	sicachebranch *last = c->path;
	sicachebranch *cb = last;
	sibranch *b = n->branch;
	while (cb && b) {
		cb->branch = b;
		cb->ref = NULL;
		sr_bufreset(&cb->buf);
		last = cb;
		cb = cb->next;
		b  = b->next;
	}
	while (b) {
		cb = si_cacheadd(c, b);
		if (srunlikely(cb == NULL))
			return -1;
		if (last)
			last->next = cb;
		last = cb;
		if (c->path == NULL)
			c->path = cb;
		b = b->next;
	}
	c->count  = n->branch_count;
	c->node   = n;
	c->nodeid = n->self.id.id;
	c->branch = c->path;
	return 0;
}

static inline sicachebranch*
si_cachefollow(sicache *c)
{
	sicachebranch *b = c->branch;
	c->branch = c->branch->next;
	return b;
}

#endif
