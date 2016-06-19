
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
#include <libss.h>
#include <libsv.h>

ss_rbtruncate(sv_indextruncate,
              sv_vfree((sr*)arg, sscast(n, svv, node)))

int sv_indexinit(svindex *i)
{
	i->lsnmin = UINT64_MAX;
	i->count  = 0;
	i->used   = 0;
	ss_rbinit(&i->i);
	return 0;
}

int sv_indexfree(svindex *i, sr *r)
{
	if (i->i.root)
		sv_indextruncate(i->i.root, r);
	ss_rbinit(&i->i);
	return 0;
}

static inline svv*
sv_vset(svv *head, svv *v)
{
	assert(head->lsn != v->lsn);
	/* default */
	if (sslikely(head->lsn < v->lsn)) {
		v->next = head;
		head->flags |= SVDUP;
		return v;
	}
	/* redistribution (starting from highest lsn) */
	svv *prev = head;
	svv *c = head->next;
	while (c) {
		assert(c->lsn != v->lsn);
		if (c->lsn < v->lsn)
			break;
		prev = c;
		c = c->next;
	}
	prev->next = v;
	v->next = c;
	v->flags |= SVDUP;
	return head;
}

svv*
sv_indexget(svindex *i, sr *r, svindexpos *p, svv *v)
{
	p->rc = sv_indexmatch(&i->i, r->scheme, sv_vpointer(v),
	                      v->size, &p->node);
	if (p->rc == 0 && p->node)
		return sscast(p->node, svv, node);
	return NULL;
}

int sv_indexupdate(svindex *i, svindexpos *p, svv *v)
{
	if (p->rc == 0 && p->node) {
		svv *head = sscast(p->node, svv, node);
		svv *update = sv_vset(head, v);
		if (head != update)
			ss_rbreplace(&i->i, p->node, &update->node);
	} else {
		ss_rbset(&i->i, p->node, p->rc, &v->node);
	}
	if (v->lsn < i->lsnmin)
		i->lsnmin = v->lsn;
	i->count++;
	i->used += v->size;
	return 0;
}
