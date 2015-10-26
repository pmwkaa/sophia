
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>

typedef struct ssooma ssooma;

struct ssooma {
	ssspinlock lock;
	uint32_t fail_from;
	uint32_t n;
};

static inline int
ss_oomaclose(ssa *a)
{
	ssooma *o = (ssooma*)a->priv;
	ss_spinlockfree(&o->lock);
	return 0;
}

static inline int
ss_oomaopen(ssa *a, va_list args)
{
	ssooma *o = (ssooma*)a->priv;
	o->fail_from = va_arg(args, int);
	o->n = 0;
	ss_spinlockinit(&o->lock);
	return 0;
}

sshot static inline void*
ss_oomamalloc(ssa *a, int size)
{
	ssooma *o = (ssooma*)a->priv;
	ss_spinlock(&o->lock);
	int generate_fail = o->n >= o->fail_from;
	o->n++;
	ss_spinunlock(&o->lock);
	if (generate_fail)
		return NULL;
	return malloc(size);
}

static inline void*
ss_oomarealloc(ssa *a, void *ptr, int size)
{
	ssooma *o = (ssooma*)a->priv;
	ss_spinlock(&o->lock);
	int generate_fail = o->n >= o->fail_from;
	o->n++;
	ss_spinunlock(&o->lock);
	if (generate_fail)
		return NULL;
	return realloc(ptr, size);
}

sshot static inline void
ss_oomafree(ssa *a ssunused, void *ptr)
{
	free(ptr);
}

ssaif ss_ooma =
{
	.open    = ss_oomaopen,
	.close   = ss_oomaclose,
	.malloc  = ss_oomamalloc,
	.realloc = ss_oomarealloc,
	.free    = ss_oomafree 
};
