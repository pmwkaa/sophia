
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>

typedef struct {
	ssspinlock  lock;
	uint32_t    slab_size;
	uint64_t    slab_max;
	uint64_t    pool_size;
	char       *pool_next;
	ssmmap      pool;
	char       *pool_end;
	char       *pool_free;
	uint32_t    pool_free_count;
	ssvfs      *vfs;
} ssslaba;

static inline int
ss_slabaopen(ssa *a, va_list args)
{
	ssslaba *s = (ssslaba*)a->priv;
	s->vfs       = va_arg(args, ssvfs*);
	s->pool_size = va_arg(args, uint64_t);
	s->slab_size = va_arg(args, uint32_t);
	assert(s->slab_size >= sizeof(void*));
	s->slab_max  = s->pool_size / s->slab_size;
	s->pool_free_count = 0;
	s->pool_free = NULL;
	s->pool_next = NULL;
	ss_mmapinit(&s->pool);
	int rc = ss_vfsmmap_allocate(s->vfs, &s->pool, s->pool_size);
	if (ssunlikely(rc == -1))
		return -1;
	ss_spinlockinit(&s->lock);
	s->pool_next = s->pool.p;
	s->pool_end  = s->pool.p + (s->slab_max * s->slab_size);
	return 0;
}

static inline int
ss_slabaclose(ssa *a)
{
	ssslaba *s = (ssslaba*)a->priv;
	ss_vfsmunmap(s->vfs, &s->pool);
	ss_spinlockfree(&s->lock);
	return 0;
}

static inline sshot void*
ss_slabamalloc(ssa *a, int size ssunused)
{
	ssslaba *s = (ssslaba*)a->priv;
	assert(size == (int)s->slab_size);

	ss_spinlock(&s->lock);
	char *slab;
	if (sslikely(s->pool_free_count)) {
		slab = s->pool_free;
		s->pool_free = *(char**)slab;
		s->pool_free_count--;
		if (ssunlikely(s->pool_free_count == 0))
			s->pool_free = NULL;
	} else
	if (ssunlikely(s->pool_next == s->pool_end)) {
		slab = NULL;
	} else {
		slab = s->pool_next;
		s->pool_next += s->slab_size;
	}
	ss_spinunlock(&s->lock);
	return slab;
}

static inline sshot void
ss_slabafree(ssa *a, void *ptr)
{
	ssslaba *s = (ssslaba*)a->priv;
	assert(ptr != NULL);

	ss_spinlock(&s->lock);
	*(char**)ptr = s->pool_free;
	s->pool_free = ptr;
	s->pool_free_count++;
	ss_spinunlock(&s->lock);
}

ssaif ss_slaba =
{
	.open    = ss_slabaopen,
	.close   = ss_slabaclose,
	.malloc  = ss_slabamalloc,
	.realloc = NULL,
	.free    = ss_slabafree 
};
