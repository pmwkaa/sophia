
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libso.h>
#include <libsv.h>
#include <libsd.h>
#include <libsl.h>
#include <libsi.h>
#include <libsy.h>
#include <libsc.h>

void sc_readpool_init(screadpool *p, sr *r)
{
	so_listinit(&p->list);
	so_listinit(&p->list_ready);
	so_listinit(&p->list_active);
	ss_mutexinit(&p->lock);
	ss_condinit(&p->cond);
	p->r = r;
}

void sc_readpool_free(screadpool *p)
{
	so_listdestroy(&p->list);
	so_listdestroy(&p->list_active);
	so_listdestroy(&p->list_ready);
	ss_mutexfree(&p->lock);
	ss_condfree(&p->cond);
}

static inline void
sc_readpool_add(screadpool *p, scread *r)
{
	ss_mutexlock(&p->lock);
	so_listadd(&p->list, &r->o);
	ss_condsignal(&p->cond);
	ss_mutexunlock(&p->lock);
}

so *sc_readpool_new(screadpool *p, so *o, int add)
{
	scread *r = (scread*)o;
	scread *n = ss_malloc(p->r->a, sizeof(scread));
	if (ssunlikely(r == NULL)) {
		sr_oom(p->r->e);
		return NULL;
	}
	memcpy(n, r, sizeof(*r));
	if (add) {
		sc_readpool_add(p, n);
	}
	return &r->o;
}

so *sc_readpool_pop(screadpool *p, int block)
{
	ss_mutexlock(&p->lock);
	if (p->list.n == 0) {
		if (! block)
			goto empty;
		ss_condwait(&p->cond, &p->lock);
		if (p->list.n == 0)
			goto empty;
	}
	so *o = so_listfirst(&p->list);
	so_listdel(&p->list, o);
	so_listadd(&p->list_active, o);
	ss_mutexunlock(&p->lock);
	return o;
empty:
	ss_mutexunlock(&p->lock);
	return NULL;
}

so *sc_readpool_popready(screadpool *p)
{
	ss_mutexlock(&p->lock);
	if (p->list_ready.n == 0) {
		ss_mutexunlock(&p->lock);
		return NULL;
	}
	so *o = so_listfirst(&p->list_ready);
	so_listdel(&p->list_ready, o);
	ss_mutexunlock(&p->lock);
	return o;
}

void sc_readpool_ready(screadpool *p, so *o)
{
	scread *r = (scread*)o;
	ss_mutexlock(&p->lock);
	so_listdel(&p->list_active, &r->o);
	so_listadd(&p->list_ready, &r->o);
	ss_mutexunlock(&p->lock);
}

void sc_readpool_wakeup(screadpool *p)
{
	ss_mutexlock(&p->lock);
	ss_condsignal(&p->cond);
	ss_mutexunlock(&p->lock);
}

int sc_readpool_queue(screadpool *p)
{
	ss_mutexlock(&p->lock);
	int n = p->list.n;
	ss_mutexunlock(&p->lock);
	return n;
}
