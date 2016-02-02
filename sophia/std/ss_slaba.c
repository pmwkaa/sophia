
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
};

struct sssa {
	uint32_t chunk_max;
	uint32_t chunk_count;
	uint32_t chunk_used;
	uint32_t chunk_size;
	sssachunk *chunk;
	sspage *pu;
	sspager *pager;
} sspacked;

static inline int
ss_sagrow(sssa *s)
{
	sspage *page = ss_pagerpop(s->pager);
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
	return 0;
}

static inline int
ss_slabaopen(ssa *a, va_list args) {
	assert(sizeof(sssa) <= sizeof(a->priv));
	sssa *s = (sssa*)a->priv;
	memset(s, 0, sizeof(*s));
	s->pager       = va_arg(args, sspager*);
	s->chunk_size  = va_arg(args, uint32_t);
	s->chunk_count = 0;
	s->chunk_max   =
		(s->pager->page_size - sizeof(sspage)) /
	     (sizeof(sssachunk) + s->chunk_size);
	s->chunk_used  = 0;
	s->chunk       = NULL;
	s->pu          = NULL;
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
		sssachunk *c = s->chunk;
		s->chunk = c->next;
		s->chunk_count++;
		c->next = NULL;
		return (char*)c + sizeof(sssachunk);
	}
	if (ssunlikely(s->chunk_used == s->chunk_max)) {
		if (ssunlikely(ss_sagrow(s) == -1))
			return NULL;
	}
	int off = sizeof(sspage) +
		s->chunk_used * (sizeof(sssachunk) + s->chunk_size);
	sssachunk *n = (sssachunk*)((char*)s->pu + off);
	s->chunk_used++;
	s->chunk_count++;
	n->next = NULL;
	return (char*)n + sizeof(sssachunk);
}

sshot static inline void
ss_slabafree(ssa *a, void *ptr)
{
	sssa *s = (sssa*)a->priv;
	sssachunk *c = (sssachunk*)((char*)ptr - sizeof(sssachunk));
	c->next = s->chunk;
	s->chunk = c;
	s->chunk_count--;
}

ssaif ss_slaba =
{
	.open    = ss_slabaopen,
	.close   = ss_slabaclose,
	.malloc  = ss_slabamalloc,
	.realloc = NULL,
	.free    = ss_slabafree
};
