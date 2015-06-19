
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
#include <libsv.h>
#include <libsx.h>

int sx_managerinit(sxmanager *m, srseq *seq, ssa *a, ssa *asxv)
{
	ss_rbinit(&m->i);
	m->count = 0;
	ss_spinlockinit(&m->lockupd);
	ss_spinlockinit(&m->lock);
	m->seq = seq;
	m->asxv = asxv;
	m->a = a;
	return 0;
}

int sx_managerfree(sxmanager *m)
{
	/* rollback active transactions */
	ss_spinlockfree(&m->lock);
	ss_spinlockfree(&m->lockupd);
	return 0;
}

int sx_indexinit(sxindex *i, sr *r, void *ptr)
{
	ss_rbinit(&i->i);
	i->count = 0;
	i->scheme = NULL;
	i->ptr = ptr;
	i->r = r;
	return 0;
}

int sx_indexset(sxindex *i, uint32_t dsn, srscheme *scheme)
{
	i->dsn = dsn;
	i->scheme = scheme;
	return 0;
}

ss_rbtruncate(sx_truncate,
              sx_vfree(((ssa**)arg)[0],
                       ((ssa**)arg)[1], sscast(n, sxv, node)))

int sx_indexfree(sxindex *i, sxmanager *m)
{
	ssa *allocators[2] = { m->a, m->asxv };
	if (i->i.root)
		sx_truncate(i->i.root, allocators);
	return 0;
}

uint32_t sx_min(sxmanager *m)
{
	ss_spinlock(&m->lock);
	uint32_t id = 0;
	if (m->count) {
		ssrbnode *node = ss_rbmin(&m->i);
		sx *min = sscast(node, sx, node);
		id = min->id;
	}
	ss_spinunlock(&m->lock);
	return id;
}

uint32_t sx_max(sxmanager *m)
{
	ss_spinlock(&m->lock);
	uint32_t id = 0;
	if (m->count) {
		ssrbnode *node = ss_rbmax(&m->i);
		sx *max = sscast(node, sx, node);
		id = max->id;
	}
	ss_spinunlock(&m->lock);
	return id;
}

uint64_t sx_vlsn(sxmanager *m)
{
	ss_spinlock(&m->lock);
	uint64_t vlsn;
	if (m->count) {
		ssrbnode *node = ss_rbmin(&m->i);
		sx *min = sscast(node, sx, node);
		vlsn = min->vlsn;
	} else {
		vlsn = sr_seq(m->seq, SR_LSN);
	}
	ss_spinunlock(&m->lock);
	return vlsn;
}

ss_rbget(sx_matchtx, ss_cmp((sscast(n, sx, node))->id, *(uint32_t*)key))

sx *sx_find(sxmanager *m, uint32_t id)
{
	ssrbnode *n = NULL;
	int rc = sx_matchtx(&m->i, NULL, (char*)&id, sizeof(id), &n);
	if (rc == 0 && n)
		return  sscast(n, sx, node);
	return NULL;
}

void sx_init(sxmanager *m, sx *t)
{
	t->manager = m;
	sv_loginit(&t->log);
	ss_listinit(&t->deadlock);
}

sxstate sx_begin(sxmanager *m, sx *t, uint64_t vlsn)
{
	t->s = SXREADY; 
	sr_seqlock(m->seq);
	t->id = sr_seqdo(m->seq, SR_TSNNEXT);
	if (sslikely(vlsn == 0))
		t->vlsn = sr_seqdo(m->seq, SR_LSN);
	else
		t->vlsn = vlsn;
	sr_sequnlock(m->seq);
	sx_init(m, t);
	ss_spinlock(&m->lock);
	ssrbnode *n = NULL;
	int rc = sx_matchtx(&m->i, NULL, (char*)&t->id, sizeof(t->id), &n);
	if (rc == 0 && n) {
		assert(0);
	} else {
		ss_rbset(&m->i, n, rc, &t->node);
	}
	m->count++;
	ss_spinunlock(&m->lock);
	return SXREADY;
}

void sx_gc(sx *t)
{
	sxmanager *m = t->manager;
	ssiter i;
	ss_iterinit(ss_bufiter, &i);
	ss_iteropen(ss_bufiter, &i, &t->log.buf, sizeof(svlogv));
	if (sslikely(t->s == SXCOMMIT)) {
		for (; ss_iterhas(ss_bufiter, &i); ss_iternext(ss_bufiter, &i))
		{
			svlogv *lv = ss_iterof(ss_bufiter, &i);
			sxv *v = lv->vgc;
			ss_free(m->asxv, v);
		}
	} else
	if (t->s == SXROLLBACK) {
		for (; ss_iterhas(ss_bufiter, &i); ss_iternext(ss_bufiter, &i))
		{
			svlogv *lv = ss_iterof(ss_bufiter, &i);
			sxv *v = lv->v.v;
			sx_vfree(m->a, m->asxv, v);
		}
	}
	sv_logfree(&t->log, m->a);
	t->s = SXUNDEF;
}

static inline void
sx_end(sx *t)
{
	sxmanager *m = t->manager;
	ss_spinlock(&m->lock);
	ss_rbremove(&m->i, &t->node);
	m->count--;
	ss_spinunlock(&m->lock);
}

sxstate sx_prepare(sx *t, sxpreparef prepare, void *arg)
{
	sxmanager *m = t->manager;
	ssiter i;
	ss_iterinit(ss_bufiter, &i);
	ss_iteropen(ss_bufiter, &i, &t->log.buf, sizeof(svlogv));
	sxstate s = SXPREPARE;
	ss_spinlock(&m->lockupd);
	for (; ss_iterhas(ss_bufiter, &i); ss_iternext(ss_bufiter, &i))
	{
		svlogv *lv = ss_iterof(ss_bufiter, &i);
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
			if (ssunlikely(s != SXPREPARE))
				goto done;
		}
	}
done:
	ss_spinunlock(&m->lockupd);
	t->s = s;
	return s;
}

sxstate sx_commit(sx *t)
{
	assert(t->s == SXPREPARE);
	sxmanager *m = t->manager;
	ssiter i;
	ss_iterinit(ss_bufiter, &i);
	ss_iteropen(ss_bufiter, &i, &t->log.buf, sizeof(svlogv));
	ss_spinlock(&m->lockupd);
	for (; ss_iterhas(ss_bufiter, &i); ss_iternext(ss_bufiter, &i))
	{
		svlogv *lv = ss_iterof(ss_bufiter, &i);
		sxv *v = lv->v.v;
		/* mark waiters as aborted */
		sx_vabortwaiters(v);
		/* remove from concurrent index and replace
		 * head with a first waiter */
		sxindex *i = v->index;
		if (v->next == NULL)
			ss_rbremove(&i->i, &v->node);
		else
			ss_rbreplace(&i->i, &v->node, &v->next->node);
		/* unlink version */
		sx_vunlink(v);
		/* translate log version from sxv to svv */
		sv_init(&lv->v, &sv_vif, v->v, NULL);
		lv->vgc = v;
	}
	ss_spinunlock(&m->lockupd);
	t->s = SXCOMMIT;
	sx_end(t);
	return SXCOMMIT;
}

sxstate sx_rollback(sx *t)
{
	sxmanager *m = t->manager;
	ssiter i;
	ss_iterinit(ss_bufiter, &i);
	ss_iteropen(ss_bufiter, &i, &t->log.buf, sizeof(svlogv));
	ss_spinlock(&m->lockupd);
	for (; ss_iterhas(ss_bufiter, &i); ss_iternext(ss_bufiter, &i))
	{
		svlogv *lv = ss_iterof(ss_bufiter, &i);
		sxv *v = lv->v.v;
		/* remove from index and replace head with
		 * a first waiter */
		if (v->prev)
			goto unlink;
		sxindex *i = v->index;
		if (v->next == NULL)
			ss_rbremove(&i->i, &v->node);
		else
			ss_rbreplace(&i->i, &v->node, &v->next->node);
unlink:
		sx_vunlink(v);
	}
	ss_spinunlock(&m->lockupd);
	t->s = SXROLLBACK;
	sx_end(t);
	return SXROLLBACK;
}

ss_rbget(sx_match,
         sr_compare(scheme, sv_vpointer((sscast(n, sxv, node))->v),
                    (sscast(n, sxv, node))->v->size,
                    key, keysize))

int sx_set(sx *t, sxindex *index, svv *version)
{
	sxmanager *m = t->manager;
	/* allocate mvcc container */
	sxv *v = sx_valloc(m->asxv, version);
	if (ssunlikely(v == NULL))
		return -1;
	v->id = t->id;
	v->index = index;
	svlogv lv;
	lv.id   = index->dsn;
	lv.vgc  = NULL;
	lv.next = UINT32_MAX;
	sv_init(&lv.v, &sx_vif, v, NULL);
	/* update concurrent index */
	ss_spinlock(&m->lockupd);
	ssrbnode *n = NULL;
	int rc = sx_match(&index->i, index->scheme, sv_vpointer(version),
	                  version->size, &n);
	if (ssunlikely(rc == 0 && n)) {
		/* exists */
	} else {
		/* unique */
		v->lo = sv_logcount(&t->log);
		if (ssunlikely((sv_logadd(&t->log, m->a, &lv, index->ptr)) == -1)) {
			rc = sr_oom(index->r->e);
		} else {
			ss_rbset(&index->i, n, rc, &v->node);
			rc = 0;
		}
		ss_spinunlock(&m->lockupd);
		return rc;
	}
	sxv *head = sscast(n, sxv, node);
	/* match previous update made by current
	 * transaction */
	sxv *own = sx_vmatch(head, t->id);
	if (ssunlikely(own))
	{
		if (ssunlikely(version->flags & SVUPDATE))
		{
			if (own->v->flags == SVDELETE) {
				ss_spinunlock(&m->lockupd);
				sx_vfree(m->a, m->asxv, v);
				return -1;
			}
			sv a, b, c;
			sv_init(&a, &sv_vif, own->v, NULL);
			sv_init(&b, &sv_vif, v->v, NULL);
			int rc = sv_update(index->r, &a, &b, &c);
			if (ssunlikely(rc == -1)) {
				ss_spinunlock(&m->lockupd);
				sx_vfree(m->a, m->asxv, v);
				return sr_oom(index->r->e);
			}
			/* gc */
			uint32_t grow = sv_vsize(c.v);
			uint32_t gc = sv_vsize(v->v);
			ss_free(m->a, v->v);
			ss_quota(index->r->quota, SS_QGROW, grow);
			ss_quota(index->r->quota, SS_QREMOVE, gc);
			v->v = c.v;
		}
		/* replace old object with the new one */
		lv.next = sv_logat(&t->log, own->lo)->next;
		v->lo = own->lo;
		sx_vreplace(own, v);
		if (sslikely(head == own))
			ss_rbreplace(&index->i, &own->node, &v->node);
		/* update log */
		sv_logreplace(&t->log, v->lo, &lv);
		sx_vfree(m->a, m->asxv, own);
		ss_spinunlock(&m->lockupd);
		return 0;
	}
	/* update log */
	rc = sv_logadd(&t->log, m->a, &lv, index->ptr);
	if (ssunlikely(rc == -1)) {
		ss_spinunlock(&m->lockupd);
		sx_vfree(m->a, m->asxv, v);
		return sr_oom(index->r->e);
	}
	/* add version */
	sx_vlink(head, v);
	ss_spinunlock(&m->lockupd);
	return 0;
}

int sx_get(sx *t, sxindex *index, sv *key, sv *result)
{
	sxmanager *m = t->manager;
	ssrbnode *n = NULL;
	ss_spinlock(&m->lockupd);
	int rc = sx_match(&index->i, index->scheme,
	                  sv_pointer(key),
	                  sv_size(key), &n);
	if (! (rc == 0 && n)) {
		rc = 0;
		goto done;
	}
	sxv *head = sscast(n, sxv, node);
	sxv *v = sx_vmatch(head, t->id);
	if (v == NULL) {
		rc = 0;
		goto done;
	}
	if (ssunlikely((v->v->flags & SVDELETE) > 0)) {
		rc = 2;
		goto done;
	}
	sv vv;
	sv_init(&vv, &sv_vif, v->v, NULL);
	svv *ret = sv_vdup(m->a, &vv);
	if (ssunlikely(ret == NULL)) {
		rc = sr_oom(index->r->e);
	} else {
		sv_init(result, &sv_vif, ret, NULL);
		rc = 1;
	}
done:
	ss_spinunlock(&m->lockupd);
	return rc;
}

sxstate sx_setstmt(sxmanager *m, sxindex *index, sv *v)
{
	sr_seq(m->seq, SR_TSNNEXT);
	ssrbnode *n = NULL;
	ss_spinlock(&m->lockupd);
	int rc = sx_match(&index->i, index->scheme, sv_pointer(v), sv_size(v), &n);
	ss_spinunlock(&m->lockupd);
	if (rc == 0 && n)
		return SXLOCK;
	return SXCOMMIT;
}

sxstate sx_getstmt(sxmanager *m, sxindex *index ssunused)
{
	sr_seq(m->seq, SR_TSNNEXT);
	return SXCOMMIT;
}
