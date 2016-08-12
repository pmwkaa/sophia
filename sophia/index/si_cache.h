#ifndef SI_CACHE_H_
#define SI_CACHE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sicache sicache;
typedef struct sicachepool sicachepool;

struct sicache {
	uint64_t     nsn;
	int          open;
	sinode      *node;
	sdindexpage *ref;
	sdpage       page;
	ssiter       i;
	ssiter       page_iter;
	ssiter       index_iter;
	ssbuf        buf_a;
	ssbuf        buf_b;
	sicache     *next;
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
	c->node = NULL;
	c->nsn  = 0;
	c->next = NULL;
	c->pool = pool;
	c->open = 0;
	memset(&c->i, 0, sizeof(c->i));
	ss_iterinit(sd_read, &c->i);
	ss_bufinit(&c->buf_a);
	ss_bufinit(&c->buf_b);
}

static inline void
si_cachefree(sicache *c)
{
	ss_buffree(&c->buf_a, c->pool->r->a);
	ss_buffree(&c->buf_b, c->pool->r->a);
}

static inline void
si_cachereset(sicache *c)
{
	ss_bufreset(&c->buf_a);
	ss_bufreset(&c->buf_b);
	ss_iterclose(sd_read, &c->i);
	c->ref    = NULL;
	c->open   = 0;
	c->node   = NULL;
	c->nsn    = 0;
}

static inline int
si_cachevalidate(sicache *c, sinode *n)
{
	if (sslikely(c->node == n && c->nsn == n->id))
		return 0;
	ss_iterclose(sd_read, &c->i);
	ss_bufreset(&c->buf_a);
	ss_bufreset(&c->buf_b);
	c->ref  = NULL;
	c->open = 0;
	c->node = n;
	c->nsn  = n->id;
	return 0;
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
