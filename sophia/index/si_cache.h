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
typedef struct sicachepool sicachepool;

struct sicachebranch {
	sibranch *branch;
	sdindexpage *ref;
	sdpage page;
	ssiter i;
	ssiter page_iter;
	ssiter index_iter;
	ssbuf buf_a;
	ssbuf buf_b;
	int open;
	sicachebranch *next;
} sspacked;

struct sicache {
	sicachebranch *path;
	sicachebranch *branch;
	uint32_t count;
	uint64_t nsn;
	sinode *node;
	sicache *next;
	sicachepool *pool;
};

struct sicachepool {
	sicache *head;
	int n;
	sr *r;
};

static inline void
si_cacheinit(sicache *c, sicachepool *pool)
{
	c->path   = NULL;
	c->branch = NULL;
	c->count  = 0;
	c->node   = NULL;
	c->nsn    = 0;
	c->next   = NULL;
	c->pool   = pool;
}

static inline void
si_cachefree(sicache *c)
{
	ssa *a = c->pool->r->a;
	sicachebranch *next;
	sicachebranch *cb = c->path;
	while (cb) {
		next = cb->next;
		ss_buffree(&cb->buf_a, a);
		ss_buffree(&cb->buf_b, a);
		ss_free(a, cb);
		cb = next;
	}
}

static inline void
si_cachereset(sicache *c)
{
	sicachebranch *cb = c->path;
	while (cb) {
		ss_bufreset(&cb->buf_a);
		ss_bufreset(&cb->buf_b);
		cb->branch = NULL;
		cb->ref = NULL;
		ss_iterclose(sd_read, &cb->i);
		cb->open = 0;
		cb = cb->next;
	}
	c->branch = NULL;
	c->node   = NULL;
	c->nsn    = 0;
	c->count  = 0;
}

static inline sicachebranch*
si_cacheadd(sicache *c, sibranch *b)
{
	sicachebranch *nb =
		ss_malloc(c->pool->r->a, sizeof(sicachebranch));
	if (ssunlikely(nb == NULL))
		return NULL;
	nb->branch  = b;
	nb->ref     = NULL;
	memset(&nb->i, 0, sizeof(nb->i));
	ss_iterinit(sd_read, &nb->i);
	nb->open    = 0;
	nb->next    = NULL;
	ss_bufinit(&nb->buf_a);
	ss_bufinit(&nb->buf_b);
	return nb;
}

static inline int
si_cachevalidate(sicache *c, sinode *n)
{
	if (sslikely(c->node == n && c->nsn == n->self.id.id))
	{
		if (sslikely(n->branch_count == c->count)) {
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
			if (ssunlikely(nb == NULL))
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
		cb->open = 0;
		ss_iterclose(sd_read, &cb->i);
		ss_bufreset(&cb->buf_a);
		ss_bufreset(&cb->buf_b);
		last = cb;
		cb = cb->next;
		b  = b->next;
	}
	while (b) {
		cb = si_cacheadd(c, b);
		if (ssunlikely(cb == NULL))
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
	c->nsn    = n->self.id.id;
	c->branch = c->path;
	return 0;
}

static inline sicachebranch*
si_cachefollow(sicache *c, sibranch *seek)
{
	while (c->branch) {
		sicachebranch *cb = c->branch;
		c->branch = c->branch->next;
		if (sslikely(cb->branch == seek))
			return cb;
	}
	return NULL;
}

static inline void
si_cachepool_init(sicachepool *p, sr *r)
{
	p->head = NULL;
	p->n    = 0;
	p->r    = r;
}

static inline void
si_cachepool_free(sicachepool *p)
{
	sicache *next;
	sicache *c = p->head;
	while (c) {
		next = c->next;
		si_cachefree(c);
		ss_free(p->r->a, c);
		c = next;
	}
}

static inline sicache*
si_cachepool_pop(sicachepool *p)
{
	sicache *c;
	if (sslikely(p->n > 0)) {
		c = p->head;
		p->head = c->next;
		p->n--;
		si_cachereset(c);
		c->pool = p;
		return c;
	}
	c = ss_malloc(p->r->a, sizeof(sicache));
	if (ssunlikely(c == NULL))
		return NULL;
	si_cacheinit(c, p);
	return c;
}

static inline void
si_cachepool_push(sicache *c)
{
	sicachepool *p = c->pool;
	c->next = p->head;
	p->head = c;
	p->n++;
}

#endif
