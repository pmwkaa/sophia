
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
	i->lsnmin = UINT64_MAX;
	i->count  = 0;
	i->used   = 0;
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
	/* default */
	if (srlikely(head->lsn < v->lsn)) {
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

#if 0
static inline svv*
sv_vgc(svv *v, uint64_t vlsn)
{
	svv *prev = v;
	svv *c = v->next;
	while (c) {
		if (c->lsn < vlsn) {
			prev->next = NULL;
			return c;
		}
		prev = c;
		c = c->next;
	}
	return NULL;
}

static inline uint32_t
sv_vstat(svv *v, uint32_t *count) {
	uint32_t size = 0;
	*count = 0;
	while (v) {
		size += v->keysize + v->valuesize;
		(*count)++;
		v = v->next;
	}
	return size;
}
#endif

int sv_indexset(svindex *i, sr *r, uint64_t vlsn srunused,
                svv  *v,
                svv **gc srunused)
{
	srrbnode *n = NULL;
	svv *head = NULL;
	if (v->lsn < i->lsnmin)
		i->lsnmin = v->lsn;
	int rc = sv_indexmatch(&i->i, r->cmp, sv_vpointer(v), v->size, &n);
	if (rc == 0 && n) {
		head = srcast(n, svv, node);
		svv *update = sv_vset(head, v);
		if (head != update)
			sr_rbreplace(&i->i, n, &update->node);
#if 0
		*gc = sv_vgc(update, vlsn);
		if (*gc) {
			uint32_t count = 0;
			i->used  -= sv_vstat(*gc, &count);
			i->count -= count;
		}
#endif
	} else {
		sr_rbset(&i->i, n, rc, &v->node);
	}
	i->count++;
	i->used += v->size;
	if (srunlikely(v->size > i->keymax))
		i->keymax = v->size;
	return 0;
}
