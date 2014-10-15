
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>

void sr_pagerinit(srpager *p, uint32_t pool_count, uint32_t page_size)
{
	p->page_size  = sizeof(srpage) + page_size;
	p->pool_count = pool_count;
	p->pool_size  = sizeof(srpagepool) + pool_count * p->page_size;
	p->pools      = 0;
	p->pp         = NULL;
	p->p          = NULL;
}

void sr_pagerfree(srpager *p)
{
	srpagepool *pp_next, *pp = p->pp;
	while (pp) {
		pp_next = pp->next;
		munmap(pp, p->pool_size);
		pp = pp_next;
	}
}

static inline void
sr_pagerprefetch(srpager *p, srpagepool *pp)
{
	register srpage *start =
		(srpage*)((char*)pp + sizeof(srpagepool));
	register srpage *prev = start;
	register uint32_t i = 1;
	start->pool = pp;
	while (i < p->pool_count) {
		srpage *page =
			(srpage*)((char*)start + i * p->page_size);
		page->pool = pp;
		prev->next = page;
		prev = page;
		i++;
	}
	prev->next = NULL;
	p->p = start;
}

int sr_pageradd(srpager *p)
{
	srpagepool *pp =
		mmap(NULL, p->pool_size, PROT_READ|PROT_WRITE|PROT_EXEC,
	         MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
	if (srunlikely(p == MAP_FAILED))
		return -1;
	pp->used = 0;
	pp->next = p->pp;
	p->pp = pp;
	p->pools++;
	sr_pagerprefetch(p, pp);
	return 0;
}

void *sr_pagerpop(srpager *p)
{
	if (p->p)
		goto fetch;
	if (srunlikely(sr_pageradd(p) == -1))
		return NULL;
fetch:;
	srpage *page = p->p;
	p->p = page->next;
	page->pool->used++;
	return page;
}

void sr_pagerpush(srpager *p, srpage *page)
{
	page->pool->used--;
	page->next = p->p;
	p->p = page;
}
