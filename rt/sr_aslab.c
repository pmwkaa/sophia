
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>

typedef struct srsachunk srsachunk;
typedef struct srsa srsa;

struct srsachunk {
	srsachunk *next;
};

struct srsa {
	uint32_t chunk_max;
	uint32_t chunk_count;
	uint32_t chunk_used;
	uint32_t chunk_size;
	srsachunk *chunk;
	srpage *pu;
	srpager *pager;
};

static inline int
sr_sagrow(srsa *s)
{
	register srpage *page = sr_pagerpop(s->pager);
	if (srunlikely(page == NULL))
		return -1;
	page->next = s->pu;
	s->pu = page;
	s->chunk_used = 0;
	return 0;
}

static inline int
sr_aslabclose(sra *a)
{
	srsa *s = (srsa*)a->priv;
	srpage *p_next, *p;
	p = s->pu;
	while (p) {
		p_next = p->next;
		sr_pagerpush(s->pager, p);
		p = p_next;
	}
	return 0;
}

static inline int
sr_aslabopen(sra *a, va_list args) {
	assert(sizeof(a->priv) <= sizeof(srsa));
	srsa *s = (srsa*)a->priv;
	memset(s, 0, sizeof(*s));
	s->pager       = va_arg(args, srpager*);
	s->chunk_size  = va_arg(args, uint32_t);
	s->chunk_count = 0;
	s->chunk_max   = s->pager->page_size / (sizeof(srsachunk) + s->chunk_size);
	s->chunk_used  = 0;
	s->chunk       = NULL;
	s->pu          = NULL;
	int rc = sr_sagrow(s);
	if (srunlikely(rc == -1)) {
		sr_aslabclose(a);
		return -1;
	}
	return 0;
}

srhot static inline void*
sr_aslabmalloc(sra *a, int size srunused)
{
	srsa *s = (srsa*)a->priv;
	if (srlikely(s->chunk)) {
		register srsachunk *c = s->chunk;
		s->chunk = c->next;
		s->chunk_count++;
		c->next = NULL;
		return (char*)c + sizeof(srsachunk);
	}
	if (srunlikely(s->chunk_used == s->chunk_max)) {
		if (srunlikely(sr_sagrow(s) == -1))
			return NULL;
	}
	register int off = sizeof(srpage) +
		s->chunk_used * (sizeof(srsachunk) + s->chunk_size);
	register srsachunk *n =
		(srsachunk*)((char*)s->pu + off);
	s->chunk_used++;
	s->chunk_count++;
	n->next = NULL;
	return (char*)n + sizeof(srsachunk);
}

srhot static inline void
sr_aslabfree(sra *a, void *ptr)
{
	srsa *s = (srsa*)a->priv;
	register srsachunk *c =
		(srsachunk*)((char*)ptr - sizeof(srsachunk));
	c->next = s->chunk;
	s->chunk = c;
	s->chunk_count--;
}

sraif sr_aslab =
{
	.open    = sr_aslabopen,
	.close   = sr_aslabclose,
	.malloc  = sr_aslabmalloc,
	.realloc = NULL,
	.free    = sr_aslabfree 
};
