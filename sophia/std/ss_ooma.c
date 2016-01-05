
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
	int ref;
};

static ssooma oom_alloc;

static inline int
ss_oomaclose(ssa *a ssunused)
{
	ss_spinlockfree(&oom_alloc.lock);
	return 0;
}

static inline int
ss_oomaopen(ssa *a ssunused, va_list args)
{
	oom_alloc.fail_from = va_arg(args, int);
	oom_alloc.n = 0;
	ss_spinlockinit(&oom_alloc.lock);
	return 0;
}

static inline int
ss_oomaevent(void)
{
	ss_spinlock(&oom_alloc.lock);
	int generate_fail = oom_alloc.n >= oom_alloc.fail_from;
	oom_alloc.n++;
	ss_spinunlock(&oom_alloc.lock);
	return generate_fail;
}

sshot static inline void*
ss_oomamalloc(ssa *a ssunused, int size)
{
	if (ss_oomaevent())
		return NULL;
	return malloc(size);
}

static inline int
ss_oomaensure(ssa *a ssunused, int n, int size)
{
	if (ss_oomaevent())
		return -1;
	(void)n;
	(void)size;
	return 0;
}

static inline void*
ss_oomarealloc(ssa *a ssunused, void *ptr, int size)
{
	if (ss_oomaevent())
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
	.ensure  = ss_oomaensure,
	.realloc = ss_oomarealloc,
	.free    = ss_oomafree 
};
