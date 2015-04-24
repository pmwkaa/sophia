
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsx.h>

int sx_init(sxmanager *m, sr *r, sra *asxv)
{
	sr_rbinit(&m->i);
	m->count = 0;
	sr_spinlockinit(&m->lock);
	m->asxv = asxv;
	m->r = r;
	return 0;
}

int sx_free(sxmanager *m)
{
	/* rollback active transactions */

	sr_spinlockfree(&m->lock);
	return 0;
}

int sx_indexinit(sxindex *i, void *ptr)
{
	sr_rbinit(&i->i);
	i->count = 0;
	i->cmp = NULL;
	i->ptr = ptr;
	return 0;
}

int sx_indexset(sxindex *i, uint32_t dsn, srcomparator *cmp)
{
	i->dsn = dsn;
	i->cmp = cmp;
	return 0;
}

sr_rbtruncate(sx_truncate,
              sx_vfree(((sra**)arg)[0],
                       ((sra**)arg)[1], srcast(n, sxv, node)))

int sx_indexfree(sxindex *i, sxmanager *m)
{
	sra *allocators[2] = { m->r->a, m->asxv };
	if (i->i.root)
		sx_truncate(i->i.root, allocators);
	return 0;
}

uint32_t sx_min(sxmanager *m)
{
	sr_spinlock(&m->lock);
	uint32_t id = 0;
	if (m->count) {
		srrbnode *node = sr_rbmin(&m->i);
		sx *min = srcast(node, sx, node);
		id = min->id;
	}
	sr_spinunlock(&m->lock);
	return id;
}

uint32_t sx_max(sxmanager *m)
{
	sr_spinlock(&m->lock);
	uint32_t id = 0;
	if (m->count) {
		srrbnode *node = sr_rbmax(&m->i);
		sx *max = srcast(node, sx, node);
		id = max->id;
	}
	sr_spinunlock(&m->lock);
	return id;
}

uint64_t sx_vlsn(sxmanager *m)
{
	sr_spinlock(&m->lock);
	uint64_t vlsn;
	if (m->count) {
		srrbnode *node = sr_rbmin(&m->i);
		sx *min = srcast(node, sx, node);
		vlsn = min->vlsn;
	} else {
		vlsn = sr_seq(m->r->seq, SR_LSN);
	}
	sr_spinunlock(&m->lock);
	return vlsn;
}

sr_rbget(sx_matchtx,
         sr_cmpu32((char*)&(srcast(n, sx, node))->id, sizeof(uint32_t),
                   (char*)key, sizeof(uint32_t), NULL))

sx *sx_find(sxmanager *m, uint32_t id)
{
	srrbnode *n = NULL;
	int rc = sx_matchtx(&m->i, NULL, (char*)&id, sizeof(id), &n);
	if (rc == 0 && n)
		return  srcast(n, sx, node);
	return NULL;
}

sxstate sx_begin(sxmanager *m, sx *t, uint64_t vlsn)
{
	t->s = SXREADY; 
	t->manager = m;
	sr_seqlock(m->r->seq);
	t->id = sr_seqdo(m->r->seq, SR_TSNNEXT);
	if (srlikely(vlsn == 0))
		t->vlsn = sr_seqdo(m->r->seq, SR_LSN);
	else
		t->vlsn = vlsn;
	sr_sequnlock(m->r->seq);
	sv_loginit(&t->log);
	sr_listinit(&t->deadlock);
	sr_spinlock(&m->lock);
	srrbnode *n = NULL;
	int rc = sx_matchtx(&m->i, NULL, (char*)&t->id, sizeof(t->id), &n);
	if (rc == 0 && n) {
		assert(0);
	} else {
		sr_rbset(&m->i, n, rc, &t->node);
	}
	m->count++;
	sr_spinunlock(&m->lock);
	return SXREADY;
}

sxstate sx_end(sx *t)
{
	sxmanager *m = t->manager;
	assert(t->s != SXUNDEF);
	sr_spinlock(&m->lock);
	sr_rbremove(&m->i, &t->node);
	t->s = SXUNDEF;
	m->count--;
	sr_spinunlock(&m->lock);
	sv_logfree(&t->log, m->r->a);
	return SXUNDEF;
}

sxstate sx_prepare(sx *t, sxpreparef prepare, void *arg)
{
	sriter i;
	sr_iterinit(sr_bufiter, &i, NULL);
	sr_iteropen(sr_bufiter, &i, &t->log.buf, sizeof(svlogv));
	sxstate s;
	for (; sr_iterhas(sr_bufiter, &i); sr_iternext(sr_bufiter, &i))
	{
		svlogv *lv = sr_iterof(sr_bufiter, &i);
		sxv *v = lv->v.v;
		/* cancelled by a concurrent commited
		 * transaction */
		if (v->v->flags & SVABORT)
			return SXROLLBACK;
		/* concurrent update in progress */
		if (v->prev != NULL)
			return SXLOCK;
		/* check that new key has not been committed by
		 * a concurrent transaction */
		if (prepare) {
			sxindex *i = v->index;
			s = prepare(t, &lv->v, arg, i->ptr);
			if (srunlikely(s != SXPREPARE))
				return s;
		}
	}
	s = SXPREPARE;
	t->s = s;
	return s;
}

sxstate sx_commit(sx *t)
{
	assert(t->s == SXPREPARE);
	sxmanager *m = t->manager;
	sriter i;
	sr_iterinit(sr_bufiter, &i, NULL);
	sr_iteropen(sr_bufiter, &i, &t->log.buf, sizeof(svlogv));
	for (; sr_iterhas(sr_bufiter, &i); sr_iternext(sr_bufiter, &i))
	{
		svlogv *lv = sr_iterof(sr_bufiter, &i);
		sxv *v = lv->v.v;
		/* mark waiters as aborted */
		sx_vabortwaiters(v);
		/* remove from concurrent index and replace
		 * head with a first waiter */
		sxindex *i = v->index;
		if (v->next == NULL)
			sr_rbremove(&i->i, &v->node);
		else
			sr_rbreplace(&i->i, &v->node, &v->next->node);
		/* unlink version */
		sx_vunlink(v);
		/* translate log version from sxv to svv */
		sv_init(&lv->v, &sv_vif, v->v, NULL);
		sr_free(m->asxv, v);
	}
	t->s = SXCOMMIT;
	return SXCOMMIT;
}

sxstate sx_rollback(sx *t)
{
	sxmanager *m = t->manager;
	sriter i;
	sr_iterinit(sr_bufiter, &i, NULL);
	sr_iteropen(sr_bufiter, &i, &t->log.buf, sizeof(svlogv));
	for (; sr_iterhas(sr_bufiter, &i); sr_iternext(sr_bufiter, &i))
	{
		svlogv *lv = sr_iterof(sr_bufiter, &i);
		sxv *v = lv->v.v;
		/* remove from index and replace head with
		 * a first waiter */
		if (v->prev)
			goto unlink;
		sxindex *i = v->index;
		if (v->next == NULL)
			sr_rbremove(&i->i, &v->node);
		else
			sr_rbreplace(&i->i, &v->node, &v->next->node);
unlink:
		sx_vunlink(v);
		sx_vfree(m->r->a, m->asxv, v);
	}
	t->s = SXROLLBACK;
	return SXROLLBACK;
}

sr_rbget(sx_match,
         sr_compare(cmp, sv_vkey((srcast(n, sxv, node))->v),
                    (srcast(n, sxv, node))->v->keysize,
                    key, keysize))

int sx_set(sx *t, sxindex *index, svv *version)
{
	sxmanager *m = t->manager;
	/* allocate mvcc container */
	sxv *v = sx_valloc(m->asxv, version);
	if (srunlikely(v == NULL))
		return -1;
	v->id = t->id;
	v->index = index;
	svlogv lv;
	lv.id   = index->dsn;
	lv.next = UINT32_MAX;
	sv_init(&lv.v, &sx_vif, v, NULL);
	/* update concurrent index */
	srrbnode *n = NULL;
	int rc = sx_match(&index->i, index->cmp,
	                  sv_vkey(version), version->keysize, &n);
	if (rc == 0 && n) {
		/* exists */
	} else {
		/* unique */
		v->lo = sv_logcount(&t->log);
		if (srunlikely(sv_logadd(&t->log, m->r->a, &lv, index->ptr) == -1))
			return sr_error(m->r->e, "%s", "memory allocation failed");
		sr_rbset(&index->i, n, rc, &v->node);
		return 0;
	}
	sxv *head = srcast(n, sxv, node);
	/* match previous update made by current
	 * transaction */
	sxv *own = sx_vmatch(head, t->id);
	if (srunlikely(own)) {
		/* replace old object with the new one */
		lv.next = sv_logat(&t->log, own->lo)->next;
		v->lo = own->lo;
		sx_vreplace(own, v);
		if (srlikely(head == own))
			sr_rbreplace(&index->i, &own->node, &v->node);
		/* update log */
		sv_logreplace(&t->log, v->lo, &lv);
		sx_vfree(m->r->a, m->asxv, own);
		return 0;
	}
	/* update log */
	rc = sv_logadd(&t->log, m->r->a, &lv, index->ptr);
	if (srunlikely(rc == -1)) {
		sx_vfree(m->r->a, m->asxv, v);
		return sr_error(m->r->e, "%s", "memory allocation failed");
	}
	/* add version */
	sx_vlink(head, v);
	return 0;
}

int sx_get(sx *t, sxindex *index, sv *key, sv *result)
{
	sxmanager *m = t->manager;
	srrbnode *n = NULL;
	int rc = sx_match(&index->i, index->cmp, sv_key(key),
	                  sv_keysize(key), &n);
	if (! (rc == 0 && n))
		return 0;
	sxv *head = srcast(n, sxv, node);
	sxv *v = sx_vmatch(head, t->id);
	if (v == NULL)
		return 0;
	if (srunlikely((v->v->flags & SVDELETE) > 0))
		return 2;
	sv vv;
	sv_init(&vv, &sv_vif, v->v, NULL);
	svv *ret = sv_valloc(m->r->a, &vv);
	if (srunlikely(ret == NULL))
		return sr_error(m->r->e, "%s", "memory allocation failed");
	sv_init(result, &sv_vif, ret, NULL);
	return 1;
}

sxstate sx_setstmt(sxmanager *m, sxindex *index, sv *v)
{
	sr_seq(m->r->seq, SR_TSNNEXT);
	srrbnode *n = NULL;
	int rc = sx_match(&index->i, index->cmp, sv_key(v), sv_keysize(v), &n);
	if (rc == 0 && n)
		return SXLOCK;
	return SXCOMMIT;
}

sxstate sx_getstmt(sxmanager *m, sxindex *index srunused)
{
	sr_seq(m->r->seq, SR_TSNNEXT);
	return SXCOMMIT;
}
