
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

int sx_managerinit(sxmanager *m, sr *r, sra *asxv)
{
	sr_rbinit(&m->i);
	m->count = 0;
	sr_spinlockinit(&m->lockupd);
	sr_spinlockinit(&m->lock);
	m->asxv = asxv;
	m->r = r;
	return 0;
}

int sx_managerfree(sxmanager *m)
{
	/* rollback active transactions */
	sr_spinlockfree(&m->lock);
	sr_spinlockfree(&m->lockupd);
	return 0;
}

int sx_indexinit(sxindex *i, void *ptr)
{
	sr_rbinit(&i->i);
	i->count = 0;
	i->scheme = NULL;
	i->ptr = ptr;
	return 0;
}

int sx_indexset(sxindex *i, uint32_t dsn, srscheme *scheme)
{
	i->dsn = dsn;
	i->scheme = scheme;
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

sr_rbget(sx_matchtx, sr_cmp((srcast(n, sx, node))->id, *(uint32_t*)key))

sx *sx_find(sxmanager *m, uint32_t id)
{
	srrbnode *n = NULL;
	int rc = sx_matchtx(&m->i, NULL, (char*)&id, sizeof(id), &n);
	if (rc == 0 && n)
		return  srcast(n, sx, node);
	return NULL;
}

void sx_init(sxmanager *m, sx *t)
{
	t->manager = m;
	sv_loginit(&t->log);
	sr_listinit(&t->deadlock);
}

sxstate sx_begin(sxmanager *m, sx *t, uint64_t vlsn)
{
	t->s = SXREADY; 
	sr_seqlock(m->r->seq);
	t->id = sr_seqdo(m->r->seq, SR_TSNNEXT);
	if (srlikely(vlsn == 0))
		t->vlsn = sr_seqdo(m->r->seq, SR_LSN);
	else
		t->vlsn = vlsn;
	sr_sequnlock(m->r->seq);
	sx_init(m, t);
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

void sx_gc(sx *t)
{
	sxmanager *m = t->manager;
	sriter i;
	sr_iterinit(sr_bufiter, &i, NULL);
	sr_iteropen(sr_bufiter, &i, &t->log.buf, sizeof(svlogv));
	if (srlikely(t->s == SXCOMMIT)) {
		for (; sr_iterhas(sr_bufiter, &i); sr_iternext(sr_bufiter, &i))
		{
			svlogv *lv = sr_iterof(sr_bufiter, &i);
			sxv *v = lv->vgc;
			sr_free(m->asxv, v);
		}
	} else
	if (t->s == SXROLLBACK) {
		for (; sr_iterhas(sr_bufiter, &i); sr_iternext(sr_bufiter, &i))
		{
			svlogv *lv = sr_iterof(sr_bufiter, &i);
			sxv *v = lv->v.v;
			sx_vfree(m->r->a, m->asxv, v);
		}
	}
	sv_logfree(&t->log, m->r->a);
	t->s = SXUNDEF;
}

static inline void
sx_end(sx *t)
{
	sxmanager *m = t->manager;
	sr_spinlock(&m->lock);
	sr_rbremove(&m->i, &t->node);
	m->count--;
	sr_spinunlock(&m->lock);
}

sxstate sx_prepare(sx *t, sxpreparef prepare, void *arg)
{
	sxmanager *m = t->manager;
	sriter i;
	sr_iterinit(sr_bufiter, &i, NULL);
	sr_iteropen(sr_bufiter, &i, &t->log.buf, sizeof(svlogv));
	sxstate s = SXPREPARE;
	sr_spinlock(&m->lockupd);
	for (; sr_iterhas(sr_bufiter, &i); sr_iternext(sr_bufiter, &i))
	{
		svlogv *lv = sr_iterof(sr_bufiter, &i);
		sxv *v = lv->v.v;
		/* cancelled by a concurrent commited
		 * transaction */
		if (v->v->flags & SVABORT) {
			s = SXROLLBACK;
			goto done;
		}
		/* concurrent update in progress */
		if (v->prev != NULL) {
			s = SXLOCK;
			goto done;
		}
		/* check that new key has not been committed by
		 * a concurrent transaction */
		if (prepare) {
			sxindex *i = v->index;
			s = prepare(t, &lv->v, arg, i->ptr);
			if (srunlikely(s != SXPREPARE))
				goto done;
		}
	}
done:
	sr_spinunlock(&m->lockupd);
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
	sr_spinlock(&m->lockupd);
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
		lv->vgc = v;
	}
	sr_spinunlock(&m->lockupd);
	t->s = SXCOMMIT;
	sx_end(t);
	return SXCOMMIT;
}

sxstate sx_rollback(sx *t)
{
	sxmanager *m = t->manager;
	sriter i;
	sr_iterinit(sr_bufiter, &i, NULL);
	sr_iteropen(sr_bufiter, &i, &t->log.buf, sizeof(svlogv));
	sr_spinlock(&m->lockupd);
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
	}
	sr_spinunlock(&m->lockupd);
	t->s = SXROLLBACK;
	sx_end(t);
	return SXROLLBACK;
}

sr_rbget(sx_match,
         sr_compare(scheme, sv_vpointer((srcast(n, sxv, node))->v),
                    (srcast(n, sxv, node))->v->size,
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
	lv.vgc  = NULL;
	lv.next = UINT32_MAX;
	sv_init(&lv.v, &sx_vif, v, NULL);
	/* update concurrent index */
	sr_spinlock(&m->lockupd);
	srrbnode *n = NULL;
	int rc = sx_match(&index->i, index->scheme, sv_vpointer(version),
	                  version->size, &n);
	if (srunlikely(rc == 0 && n)) {
		/* exists */
	} else {
		/* unique */
		v->lo = sv_logcount(&t->log);
		if (srunlikely((sv_logadd(&t->log, m->r->a, &lv, index->ptr)) == -1)) {
			rc = sr_error(m->r->e, "%s", "memory allocation failed");
		} else {
			sr_rbset(&index->i, n, rc, &v->node);
			rc = 0;
		}
		sr_spinunlock(&m->lockupd);
		return rc;
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
		sr_spinunlock(&m->lockupd);
		return 0;
	}
	/* update log */
	rc = sv_logadd(&t->log, m->r->a, &lv, index->ptr);
	if (srunlikely(rc == -1)) {
		sr_spinunlock(&m->lockupd);
		sx_vfree(m->r->a, m->asxv, v);
		return sr_error(m->r->e, "%s", "memory allocation failed");
	}
	/* add version */
	sx_vlink(head, v);
	sr_spinunlock(&m->lockupd);
	return 0;
}

int sx_get(sx *t, sxindex *index, sv *key, sv *result)
{
	sxmanager *m = t->manager;
	srrbnode *n = NULL;
	sr_spinlock(&m->lockupd);
	int rc = sx_match(&index->i, index->scheme,
	                  sv_pointer(key),
	                  sv_size(key), &n);
	if (! (rc == 0 && n)) {
		rc = 0;
		goto done;
	}
	sxv *head = srcast(n, sxv, node);
	sxv *v = sx_vmatch(head, t->id);
	if (v == NULL) {
		rc = 0;
		goto done;
	}
	if (srunlikely((v->v->flags & SVDELETE) > 0)) {
		rc = 2;
		goto done;
	}
	sv vv;
	sv_init(&vv, &sv_vif, v->v, NULL);
	svv *ret = sv_vdup(m->r->a, &vv);
	if (srunlikely(ret == NULL)) {
		rc = sr_error(m->r->e, "%s", "memory allocation failed");
	} else {
		sv_init(result, &sv_vif, ret, NULL);
		rc = 1;
	}
done:
	sr_spinunlock(&m->lockupd);
	return rc;
}

sxstate sx_setstmt(sxmanager *m, sxindex *index, sv *v)
{
	sr_seq(m->r->seq, SR_TSNNEXT);
	srrbnode *n = NULL;
	sr_spinlock(&m->lockupd);
	int rc = sx_match(&index->i, index->scheme, sv_pointer(v), sv_size(v), &n);
	sr_spinunlock(&m->lockupd);
	if (rc == 0 && n)
		return SXLOCK;
	return SXCOMMIT;
}

sxstate sx_getstmt(sxmanager *m, sxindex *index srunused)
{
	sr_seq(m->r->seq, SR_TSNNEXT);
	return SXCOMMIT;
}
