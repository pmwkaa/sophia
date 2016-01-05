
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>

typedef struct sssachunk sssachunk;
typedef struct sssa sssa;

struct sssachunk {
	sssachunk *next;
} sspacked;

struct sssa {
	uint32_t chunk_max;
	uint32_t chunk_used;
	uint32_t chunk_size;
	uint32_t chunk_q;
	sssachunk *chunk;
	sspage *pu;
	sspager *pager;
	ssspinlock lock;
} sspacked;

void ss_slaba_info(ssa *a, uint32_t *pools, uint32_t *pool_size)
{
	sssa *s = (sssa*)a->priv;
	ss_spinlock(&s->lock);
	*pools = s->pager->pools;
	*pool_size = s->pager->pool_size;
	ss_spinunlock(&s->lock);
}

static inline int
ss_sagrow(sssa *s)
{
	register sspage *page = ss_pagerpop(s->pager);
	if (ssunlikely(page == NULL))
		return -1;
	page->next = s->pu;
	s->pu = page;
	s->chunk_used = 0;
	return 0;
}

static inline int
ss_slabaclose(ssa *a)
{
	sssa *s = (sssa*)a->priv;
	sspage *p_next, *p;
	p = s->pu;
	while (p) {
		p_next = p->next;
		ss_pagerpush(s->pager, p);
		p = p_next;
	}
	ss_spinlockfree(&s->lock);
	return 0;
}

static inline int
ss_slabaopen(ssa *a, va_list args)
{
	assert(sizeof(sssa) <= sizeof(a->priv));
	sssa *s = (sssa*)a->priv;
	memset(s, 0, sizeof(*s));

	s->pager      = va_arg(args, sspager*);
	s->chunk_size = ss_align(32, va_arg(args, uint32_t));
	s->chunk_max  = (s->pager->page_size - sizeof(sspage) - 32) /
	                 s->chunk_size;
	s->chunk_used = 0;
	s->chunk_q    = 0;
	s->chunk      = NULL;
	s->pu         = NULL;
	ss_spinlockinit(&s->lock);
	int rc = ss_sagrow(s);
	if (ssunlikely(rc == -1)) {
		ss_slabaclose(a);
		return -1;
	}
	return 0;
}

sshot static inline void*
ss_slabamalloc(ssa *a, int size ssunused)
{
	sssa *s = (sssa*)a->priv;
	if (sslikely(s->chunk)) {
		register sssachunk *c = s->chunk;
		s->chunk_q--;
		s->chunk = c->next;
		c->next = NULL;
		return (char*)c + sizeof(sssachunk);
	}
	if (ssunlikely(s->chunk_used == s->chunk_max)) {
		if (ssunlikely(ss_sagrow(s) == -1))
			return NULL;
	}

	register sssachunk *ptr =
		(sssachunk*)(ss_align(32,
			(char*)s->pu + sizeof(sspage) +
			       s->chunk_used * s->chunk_size));

	assert(ss_align(32, ptr) == (uintptr_t)ptr);
	s->chunk_used++;
	ptr->next = NULL;
	return (char*)ptr + sizeof(sssachunk);
}

sshot static inline void
ss_slabafree(ssa *a, void *ptr)
{
	sssa *s = (sssa*)a->priv;
	register sssachunk *c =
		(sssachunk*)((char*)ptr - sizeof(sssachunk));
	c->next = s->chunk;
	s->chunk = c;
	s->chunk_q++;
}

sshot static inline int
ss_slabaensure(ssa *a, int n, int size ssunused)
{
	sssa *s = (sssa*)a->priv;
	if (sslikely(s->chunk_q >= (uint32_t)n))
		return 0;
	// XXX
	return 0;
}

sshot static inline void*
ss_slabamalloc_lock(ssa *a, int size)
{
	sssa *s = (sssa*)a->priv;
	ss_spinlock(&s->lock);
	void *ptr = ss_slabamalloc(a, size);
	ss_spinunlock(&s->lock);
	return ptr;
}

sshot static inline void
ss_slabafree_lock(ssa *a, void *ptr)
{
	sssa *s = (sssa*)a->priv;
	ss_spinlock(&s->lock);
	ss_slabafree(a, ptr);
	ss_spinunlock(&s->lock);
}

ssaif ss_slaba =
{
	.open    = ss_slabaopen,
	.close   = ss_slabaclose,
	.malloc  = ss_slabamalloc,
	.realloc = NULL,
	.ensure  = ss_slabaensure,
	.free    = ss_slabafree
};

ssaif ss_slaba_lock =
{
	.open    = ss_slabaopen,
	.close   = ss_slabaclose,
	.malloc  = ss_slabamalloc_lock,
	.realloc = NULL,
	.free    = ss_slabafree_lock
};
