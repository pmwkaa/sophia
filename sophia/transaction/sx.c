
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
	ss_spinlockinit(&m->lock);
	ss_listinit(&m->indexes);
	m->csn = 0;
	m->seq = seq;
	m->asxv = asxv;
	m->a = a;
	return 0;
}

int sx_managerfree(sxmanager *m)
{
	assert(m->count == 0);
	ss_spinlockfree(&m->lock);
	return 0;
}

int sx_indexinit(sxindex *i, sxmanager *m, sr *r, void *ptr)
{
	ss_rbinit(&i->i);
	ss_listinit(&i->link);
	i->scheme = NULL;
	i->ptr = ptr;
	i->r = r;
	ss_listappend(&m->indexes, &i->link);
	return 0;
}

int sx_indexset(sxindex *i, uint32_t dsn, srscheme *scheme)
{
	i->dsn = dsn;
	i->scheme = scheme;
	return 0;
}

ss_rbtruncate(sx_truncate,
              sx_vfreeall(((ssa**)arg)[0],
                          ((ssa**)arg)[1], sscast(n, sxv, node)))

static inline void
sx_indextruncate(sxindex *i, sxmanager *m)
{
	if (i->i.root == NULL)
		return;
	ssa *allocators[2] = { m->a, m->asxv };
	sx_truncate(i->i.root, allocators);
	ss_rbinit(&i->i);
}

int sx_indexfree(sxindex *i, sxmanager *m)
{
	sx_indextruncate(i, m);
	ss_listunlink(&i->link);
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
	t->complete = 0;
	sr_seqlock(m->seq);
	t->csn = m->csn;
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
	t->s = SXUNDEF;
	sv_logfree(&t->log, m->a);
	if (m->count > 0)
		return;
	sslist *p;
	ss_listforeach(&m->indexes, p) {
		sxindex *i = sscast(p, sxindex, link);
		if (i->i.root)
			sx_indextruncate(i, m);
	}
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

sxstate sx_prepare(sx *t)
{
	ssiter i;
	ss_iterinit(ss_bufiter, &i);
	ss_iteropen(ss_bufiter, &i, &t->log.buf, sizeof(svlogv));
	for (; ss_iterhas(ss_bufiter, &i); ss_iternext(ss_bufiter, &i))
	{
		svlogv *lv = ss_iterof(ss_bufiter, &i);
		sxv *v = lv->v.v;
		if (v->prev == NULL)
			continue;
		if (sx_vcommitted(v->prev)) {
			if (v->prev->csn > t->csn) {
				t->s = SXROLLBACK;
				return t->s;
			}
			continue;
		}
		t->s = SXLOCK;
		return t->s;
	}
	t->s = SXPREPARE;
	return t->s;
}

sxstate sx_commit(sx *t)
{
	assert(t->s == SXPREPARE);
	if (t->complete)
		goto complete;
	sxmanager *m = t->manager;
	uint32_t csn = ++m->csn;
	ssiter i;
	ss_iterinit(ss_bufiter, &i);
	ss_iteropen(ss_bufiter, &i, &t->log.buf, sizeof(svlogv));
	for (; ss_iterhas(ss_bufiter, &i); ss_iternext(ss_bufiter, &i))
	{
		svlogv *lv = ss_iterof(ss_bufiter, &i);
		sxv *v = lv->v.v;
		/* translate log version from sxv to svv */
		sv_init(&lv->v, &sv_vif, v->v, NULL);
		/* mark stmt as commited */
		sx_vcommit(v, csn);
		sv_vref(v->v);
		/* stmt automatically scheduled for gc */
	}
complete:
	t->s = SXCOMMIT;
	sx_end(t);
	return SXCOMMIT;
}

static inline void
sx_rollback_index(sx *t, sr *r, int translate)
{
	sxmanager *m = t->manager;
	ssiter i;
	ss_iterinit(ss_bufiter, &i);
	ss_iteropen(ss_bufiter, &i, &t->log.buf, sizeof(svlogv));
	int gc = 0;
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

		/* translate log version from sxv to svv */
		if (translate) {
			sv_init(&lv->v, &sv_vif, v->v, NULL);
			continue;
		}
		gc += sv_vsize((svv*)v->v);
		sx_vfree(m->a, m->asxv, v);
	}
	if (gc > 0)
		ss_quota(r->quota, SS_QREMOVE, gc);
}

sxstate sx_rollback(sx *t, sr *r)
{
	if (! t->complete)
		sx_rollback_index(t, r, 0);
	t->s = SXROLLBACK;
	sx_end(t);
	return SXROLLBACK;
}

sxstate sx_complete(sx *t)
{
	assert(t->complete == 0);
	assert(t->s == SXPREPARE);
	sx_rollback_index(t, NULL, 1);
	t->complete = 1;
	return SXPREPARE;
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
	if (ssunlikely(v == NULL)) {
		sv_vfree(m->a, version);
		return -1;
	}
	v->id = t->id;
	v->index = index;
	svlogv lv;
	lv.id   = index->dsn;
	lv.next = UINT32_MAX;
	sv_init(&lv.v, &sx_vif, v, NULL);
	/* update concurrent index */
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
		return rc;
	}
	sxv *head = sscast(n, sxv, node);
	/* match previous update made by current
	 * transaction */
	sxv *own = sx_vmatch(head, t->id);
	if (ssunlikely(own))
	{
		if (ssunlikely(version->flags & SVUPDATE)) {
			sr_error(index->r->e, "%s", "only one update statement is "
			         "allowed per a transaction key");
			sx_vfree(m->a, m->asxv, v);
			return -1;
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
		return 0;
	}
	/* update log */
	rc = sv_logadd(&t->log, m->a, &lv, index->ptr);
	if (ssunlikely(rc == -1)) {
		sx_vfree(m->a, m->asxv, v);
		return sr_oom(index->r->e);
	}
	/* add version */
	sx_vlink(head, v);
	return 0;
}

int sx_get(sx *t, sxindex *index, sv *key, sv *result)
{
	sxmanager *m = t->manager;
	ssrbnode *n = NULL;
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
	return rc;
}

sxstate sx_getstmt(sxmanager *m, sxindex *index ssunused)
{
	sr_seq(m->seq, SR_TSNNEXT);
	return SXCOMMIT;
}
