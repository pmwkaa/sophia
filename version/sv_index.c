
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

static inline svv*
sv_vset(svv *head, svv *v)
{
	if (srunlikely(head->lsn < v->lsn)) {
		v->next = head;
		return v;
	}
	svv *c = head;
	svv *prev = NULL;
	while (c) {
		if (c->lsn < v->lsn)
		{
			if (prev)
				prev->next = v;
			v->next = c;
			break;
		}
		c = c->next;
		prev = c;
	}
	return head;
}

static inline svv*
sv_vgc(svv *v, uint64_t lsvn)
{
	svv *prev = v;
	svv *c = v->next;
	while (c) {
		if (c->lsn < lsvn) {
			prev->next = NULL;
			return c;
		}
		c = c->next;
	}
	return NULL;
}

int sv_indexset(svindex *i, sr *r, uint64_t lsvn, svv *v, svv **gc)
{
	srrbnode *n = NULL;
	svv *head = NULL;
	int rc = sv_indexmatch(&i->i, r->cmp, sv_vkey(v), v->keysize, &n);
	if (rc == 0 && n) {
		head = srcast(n, svv, node);
		svv *update = sv_vset(head, v);
		if (head != update)
			sr_rbreplace(&i->i, n, &update->node);
		*gc = sv_vgc(update, lsvn);
	} else {
		sr_rbset(&i->i, n, rc, &v->node);
		i->count++;
	}
	if (srunlikely(v->keysize > i->keymax))
		i->keymax = v->keysize;
	return 0;
}
