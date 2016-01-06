
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
              sv_reffree((sr*)arg, sscast(n, svref, node)))

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

static inline svref*
sv_vset(svref *head, svref *v)
{
	assert(head->v->lsn != v->v->lsn);
	svv *vv = v->v;
	/* default */
	if (sslikely(head->v->lsn < vv->lsn)) {
		v->next = head;
		head->flags |= SVDUP;
		return v;
	}
	/* redistribution (starting from highest lsn) */
	svref *prev = head;
	svref *c = head->next;
	while (c) {
		assert(c->v->lsn != vv->lsn);
		if (c->v->lsn < vv->lsn)
			break;
		prev = c;
		c = c->next;
	}
	prev->next = v;
	v->next = c;
	v->flags |= SVDUP;
	return head;
}

svref*
sv_indexget(svindex *i, sr *r, svindexpos *p, svref *v)
{
	p->rc = sv_indexmatch(&i->i, r->scheme, sv_vpointer(v->v), v->v->size, &p->node);
	if (p->rc == 0 && p->node)
		return sscast(p->node, svref, node);
	return NULL;
}

int sv_indexupdate(svindex *i, svindexpos *p, svref *v)
{
	if (p->rc == 0 && p->node) {
		svref *head = sscast(p->node, svref, node);
		svref *update = sv_vset(head, v);
		if (head != update)
			ss_rbreplace(&i->i, p->node, &update->node);
	} else {
		ss_rbset(&i->i, p->node, p->rc, &v->node);
	}
	if (v->v->lsn < i->lsnmin)
		i->lsnmin = v->v->lsn;
	i->count++;
	i->used += v->v->size;
	return 0;
}
