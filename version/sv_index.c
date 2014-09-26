
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>

sr_rbtruncate(sv_indextruncate,
              sv_vfree((sra*)arg, srcast(n, svv, node)))

int sv_indexinit(svindex *i)
{
	i->keymax = 0;
	i->count  = 0;
	sr_rbinit(&i->i);
	return 0;
}

int sv_indexfree(svindex *i, sr *r)
{
	if (i->i.root)
		sv_indextruncate(i->i.root, r->a);
	sr_rbinit(&i->i);
	return 0;
}

int sv_indexset(svindex *i, sr *r, uint64_t lsvn, svv *v, svv **old)
{
	srrbnode *n = NULL;
	svv *prev = NULL;
	int rc = sv_indexmatch(&i->i, r->cmp, sv_vkey(v), v->keysize, &n);
	if (rc == 0 && n) {
		prev = srcast(n, svv, node);
		prev = sv_vupdate(prev, v, lsvn);
		*old = prev;
		sr_rbreplace(&i->i, n, &v->node);
	} else {
		sr_rbset(&i->i, n, rc, &v->node);
		i->count++;
	}
	if (srunlikely(v->keysize > i->keymax))
		i->keymax = v->keysize;
	return 0;
}
