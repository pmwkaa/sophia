
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
#include <libso.h>
#include <libsx.h>

static inline int
sx_count(sxmanager *m) {
	return m->count_rd + m->count_rw;
}

int sx_managerinit(sxmanager *m, srseq *seq, ssa *a)
{
	ss_rbinit(&m->i);
	m->count_rd = 0;
	m->count_rw = 0;
	m->count_gc = 0;
	m->csn = 0;
	m->gc  = NULL;
	ss_spinlockinit(&m->lock);
	ss_listinit(&m->indexes);
	sx_vpool_init(&m->pool, a);
	m->seq = seq;
	return 0;
}

int sx_managerfree(sxmanager *m)
{
	assert(sx_count(m) == 0);
	sx_vpool_free(&m->pool);
	ss_spinlockfree(&m->lock);
	return 0;
}

int sx_indexinit(sxindex *i, sxmanager *m, sr *r, so *object)
{
	ss_rbinit(&i->i);
	ss_listinit(&i->link);
	i->dsn = 0;
	i->object = object;
	i->r = r;
	ss_listappend(&m->indexes, &i->link);
	return 0;
}

int sx_indexset(sxindex *i, uint32_t dsn)
{
	i->dsn = dsn;
	return 0;
}

ss_rbtruncate(sx_truncate, sx_vfreeall( ((void**)arg)[1],
                                        ((void**)arg)[0], sscast(n, sxv, node)))

static inline void
sx_indextruncate(sxindex *i, sxmanager *m)
{
	if (i->i.root == NULL)
		return;
	void *args[2] = { i->r, &m->pool };
	sx_truncate(i->i.root, args);
	ss_rbinit(&i->i);
}

int sx_indexfree(sxindex *i, sxmanager *m)
{
	sx_indextruncate(i, m);
	ss_listunlink(&i->link);
	return 0;
}

uint64_t sx_vlsn(sxmanager *m)
{
	ss_spinlock(&m->lock);
	uint64_t vlsn;
	if (sx_count(m) > 0) {
		ssrbnode *node = ss_rbmin(&m->i);
		sx *min = sscast(node, sx, node);
		vlsn = min->vlsn;
	} else {
		vlsn = sr_seq(m->seq, SR_LSN);
	}
	ss_spinunlock(&m->lock);
	return vlsn;
}

ss_rbget(sx_matchtx, ss_cmp((sscast(n, sx, node))->id, sscastu64(key)))

sx *sx_find(sxmanager *m, uint64_t id)
{
	ssrbnode *n = NULL;
	int rc = sx_matchtx(&m->i, NULL, (char*)&id, sizeof(id), &n);
	if (rc == 0 && n)
		return  sscast(n, sx, node);
	return NULL;
}

static inline sxstate
sx_promote(sx *x, sxstate state)
{
	x->state = state;
	return state;
}

void sx_init(sxmanager *m, sx *x, svlog *log)
{
	x->manager = m;
	x->log = log;
	x->isolation = SX_SERIALIZABLE;
	sx_promote(x, SX_UNDEF);
	ss_listinit(&x->deadlock);
}

sxstate sx_begin(sxmanager *m, sx *x, sxtype type, svlog *log, uint64_t vlsn)
{
	sx_init(m, x, log);
	sx_promote(x, SX_READY);
	x->type = type;
	x->log_read = -1;
	sr_seqlock(m->seq);
	x->csn = m->csn;
	x->id = sr_seqdo(m->seq, SR_TSNNEXT);
	if (sslikely(vlsn == UINT64_MAX))
		x->vlsn = sr_seqdo(m->seq, SR_LSN);
	else
		x->vlsn = vlsn;
	sr_sequnlock(m->seq);
	ss_spinlock(&m->lock);
	ssrbnode *n = NULL;
	int rc = sx_matchtx(&m->i, NULL, (char*)&x->id, sizeof(x->id), &n);
	if (rc == 0 && n) {
		assert(0);
	} else {
		ss_rbset(&m->i, n, rc, &x->node);
	}
	if (type == SX_RO)
		m->count_rd++;
	else
		m->count_rw++;
	ss_spinunlock(&m->lock);
	return SX_READY;
}

static inline void
sx_untrack(sxv *v)
{
	if (v->prev == NULL) {
		sxindex *i = v->index;
		if (v->next == NULL)
			ss_rbremove(&i->i, &v->node);
		else
			ss_rbreplace(&i->i, &v->node, &v->next->node);
	}
	sx_vunlink(v);
}

static inline uint64_t
sx_csn(sxmanager *m)
{
	uint64_t csn = UINT64_MAX;
	if (m->count_rw == 0)
		return csn;
	ssrbnode *p = ss_rbmin(&m->i);
	sx *min = NULL;
	while (p) {
		min = sscast(p, sx, node);
		if (min->type == SX_RO) {
			p = ss_rbnext(&m->i, p);
			continue;
		}
		break;
	}
	assert(min != NULL);
	return min->csn;
}

static inline void
sx_garbage_collect(sxmanager *m)
{
	uint64_t min_csn = sx_csn(m);
	sxv *gc = NULL;
	uint32_t count = 0;
	sxv *next;
	sxv *v = m->gc;
	for (; v; v = next)
	{
		next = v->gc;
		sxindex *i = v->index;
		assert(sv_vflags(v->v, i->r) & SVGET);
		assert(sx_vcommitted(v));
		if (v->csn > min_csn) {
			v->gc = gc;
			gc = v;
			count++;
			continue;
		}
		sx_untrack(v);
		sx_vfree(&m->pool, i->r, v);
	}
	m->count_gc = count;
	m->gc = gc;
}

void sx_gc(sx *x)
{
	sxmanager *m = x->manager;
	sx_promote(x, SX_UNDEF);
	x->log = NULL;
	if (m->count_gc == 0)
		return;
	sx_garbage_collect(m);
}

static inline void
sx_end(sx *x)
{
	sxmanager *m = x->manager;
	ss_spinlock(&m->lock);
	ss_rbremove(&m->i, &x->node);
	if (x->type == SX_RO)
		m->count_rd--;
	else
		m->count_rw--;
	ss_spinunlock(&m->lock);
}

static inline void
sx_rollback_svp(sx *x, ssiter *i, int free)
{
	sxmanager *m = x->manager;
	for (; ss_iterhas(ss_bufiter, i); ss_iternext(ss_bufiter, i))
	{
		svlogv *lv = ss_iterof(ss_bufiter, i);
		sxv *v = lv->v.v;
		/* remove from index and replace head with
		 * a first waiter */
		sx_untrack(v);
		/* translate log version from sxv to svv */
		sv_init(&lv->v, &sv_vif, v->v, NULL);
		if (free) {
			sxindex *i = v->index;
			sv_vunref(i->r, v->v);
		}
		sx_vpool_push(&m->pool, v);
	}
}

sxstate sx_rollback(sx *x)
{
	assert(x->state != SX_COMMIT);
	ssiter i;
	ss_iterinit(ss_bufiter, &i);
	ss_iteropen(ss_bufiter, &i, &x->log->buf, sizeof(svlogv));
	sx_rollback_svp(x, &i, 1);
	sx_promote(x, SX_ROLLBACK);
	sx_end(x);
	return SX_ROLLBACK;
}

static inline int
sx_preparecb(sx *x, svlogv *v, uint64_t lsn, sxpreparef prepare, void *arg)
{
	if (sslikely(lsn == x->vlsn))
		return 0;
	if (prepare) {
		sxindex *i = ((sxv*)v->v.v)->index;
		if (prepare(x, &v->v, i->object, arg))
			return 1;
	}
	return 0;
}

sxstate sx_prepare(sx *x, sxpreparef prepare, void *arg)
{
	uint64_t lsn = sr_seq(x->manager->seq, SR_LSN);
	/* proceed read-only transactions */
	if (x->type == SX_RO || sv_logcount_write(x->log) == 0)
		return sx_promote(x, SX_PREPARE);
	ssiter i;
	ss_iterinit(ss_bufiter, &i);
	ss_iteropen(ss_bufiter, &i, &x->log->buf, sizeof(svlogv));
	sxstate rc;
	for (; ss_iterhas(ss_bufiter, &i); ss_iternext(ss_bufiter, &i))
	{
		svlogv *lv = ss_iterof(ss_bufiter, &i);
		sxv *v = lv->v.v;
		if ((int)v->lo == x->log_read)
			break;
		if (sx_vaborted(v))
			return sx_promote(x, SX_ROLLBACK);
		if (sslikely(v->prev == NULL)) {
			rc = sx_preparecb(x, lv, lsn, prepare, arg);
			if (ssunlikely(rc != 0))
				return sx_promote(x, SX_ROLLBACK);
			continue;
		}
		if (sx_vcommitted(v->prev)) {
			if (v->prev->csn > x->csn)
				return sx_promote(x, SX_ROLLBACK);
			continue;
		}
		/* force commit for read-only conflicts */
		sxindex *i = v->prev->index;
		if (sv_vflags(v->prev->v, i->r) & SVGET) {
			rc = sx_preparecb(x, lv, lsn, prepare, arg);
			if (ssunlikely(rc != 0))
				return sx_promote(x, SX_ROLLBACK);
			continue;
		}
		return sx_promote(x, SX_LOCK);
	}
	return sx_promote(x, SX_PREPARE);
}

sxstate sx_commit(sx *x)
{
	if (x->state == SX_COMMIT)
		return SX_COMMIT;
	assert(x->state == SX_PREPARE);
	sxmanager *m = x->manager;
	ssiter i;
	ss_iterinit(ss_bufiter, &i);
	ss_iteropen(ss_bufiter, &i, &x->log->buf, sizeof(svlogv));
	uint64_t csn = ++m->csn;
	for (; ss_iterhas(ss_bufiter, &i); ss_iternext(ss_bufiter, &i))
	{
		svlogv *lv = ss_iterof(ss_bufiter, &i);
		sxv *v = lv->v.v;
		if ((int)v->lo == x->log_read)
			break;
		/* abort conflict reader */
		if (v->prev && !sx_vcommitted(v->prev)) {
			sxindex *i = v->prev->index;
			assert(sv_vflags(v->prev->v, i->r) & SVGET);
			sx_vabort(v->prev);
		}
		/* abort waiters */
		sx_vabort_all(v->next);
		/* mark stmt as commited */
		sx_vcommit(v, csn);
		/* translate log version from sxv to svv */
		sv_init(&lv->v, &sv_vif, v->v, NULL);
		/* schedule read stmt for gc */

		sxindex *i = v->index;
		if (sv_vflags(v->v, i->r) & SVGET) {
			sv_vref(v->v);
			v->gc = m->gc;
			m->gc = v;
			m->count_gc++;
		} else {
			sx_untrack(v);
			sx_vpool_push(&m->pool, v);
		}
	}

	/* rollback latest reads */
	sx_rollback_svp(x, &i, 0);

	sx_promote(x, SX_COMMIT);
	sx_end(x);
	return SX_COMMIT;
}

int sx_isolation(sx *x, char *name, int size)
{
	if (x->state != SX_READY)
		return -1;
	if (ss_bufused(&x->log->buf) > 0)
		goto error;
	sxisolation isolation;
	if (strncasecmp(name, "serializable", size) == 0)
		isolation = SX_SERIALIZABLE;
	else
	if (strncasecmp(name, "batch", size) == 0)
		isolation = SX_BATCH;
	else
		goto error;
	switch (isolation) {
	case SX_BATCH:
		if (x->isolation != SX_SERIALIZABLE)
			goto error;
		sx_end(x);
		sx_promote(x, SX_COMMIT);
		break;
	case SX_SERIALIZABLE:
		if (x->isolation == SX_BATCH)
			goto error;
		break;
	}
	x->isolation = isolation;
	return 0;
error:
	return -1;
}

ss_rbget(sx_match,
         sf_compare(scheme, sv_vpointer((sscast(n, sxv, node))->v), key))

int sx_set(sx *x, sxindex *index, svv *version)
{
	sxmanager *m = x->manager;
	sr *r = index->r;

	svlogv lv;
	lv.index_id = index->dsn;
	lv.next = UINT32_MAX;

	/* batch isolation: directly write into the log */
	if (x->isolation == SX_BATCH) {
		sv_init(&lv.v, &sv_vif, version, NULL);
		return sv_logadd(x->log, r, &lv);
	}

	/* allocate mvcc container */
	sxv *v = sx_valloc(&m->pool, version);
	if (ssunlikely(v == NULL)) {
		sv_vunref(r, version);
		return -1;
	}
	v->id = x->id;
	v->index = index;
	sv_init(&lv.v, &sx_vif, v, NULL);

	if (! (sv_vflags(version, index->r) & SVGET))
		x->log_read = -1;

	/* update concurrent index */
	ssrbnode *n = NULL;
	int rc;
	rc = sx_match(&index->i, index->r->scheme,
	              sv_vpointer(version), 0, &n);
	if (ssunlikely(rc == 0 && n)) {
		/* exists */
	} else {
		int pos = rc;
		/* unique */
		v->lo = sv_logcount(x->log);
		rc = sv_logadd(x->log, r, &lv);
		if (ssunlikely(rc == -1)) {
			sr_oom(r->e);
			goto error;
		}
		ss_rbset(&index->i, n, pos, &v->node);
		return 0;
	}
	sxv *head = sscast(n, sxv, node);
	/* match previous update made by current
	 * transaction */
	sxv *own = sx_vmatch(head, x->id);
	if (ssunlikely(own))
	{
		if (ssunlikely(sv_vflags(version, index->r) & SVUPSERT)) {
			sr_error(r->e, "%s", "only one upsert statement is "
			         "allowed per a transaction key");
			goto error;
		}
		/* replace old document with the new one */
		lv.next = sv_logat(x->log, own->lo)->next;
		v->lo = own->lo;
		if (ssunlikely(sx_vaborted(own)))
			sx_vabort(v);
		sx_vreplace(own, v);
		if (sslikely(head == own))
			ss_rbreplace(&index->i, &own->node, &v->node);
		/* update log */
		sv_logreplace(x->log, r, v->lo, &lv);

		sx_vfree(&m->pool, r, own);
		return 0;
	}
	/* update log */
	v->lo = sv_logcount(x->log);
	rc = sv_logadd(x->log, r, &lv);
	if (ssunlikely(rc == -1)) {
		sr_oom(r->e);
		goto error;
	}
	/* add version */
	sx_vlink(head, v);
	return 0;
error:
	sx_vfree(&m->pool, r, v);
	return -1;
}

int sx_get(sx *x, sxindex *index, sv *key, sv *result)
{
	ssrbnode *n = NULL;
	assert(x->isolation == SX_SERIALIZABLE);
	int rc;
	rc = sx_match(&index->i, index->r->scheme,
	              sv_pointer(key), 0, &n);
	if (! (rc == 0 && n))
		goto add;
	sxv *head = sscast(n, sxv, node);
	sxv *v = sx_vmatch(head, x->id);
	if (v == NULL)
		goto add;
	if (ssunlikely(sv_vflags(v->v, index->r) & SVGET))
		return 0;
	if (ssunlikely(sv_vflags(v->v, index->r) & SVDELETE))
		return 2;
	sv vv;
	sv_init(&vv, &sv_vif, v->v, NULL);
	svv *ret = sv_vbuildraw(index->r, sv_pointer(&vv));
	if (ssunlikely(ret == NULL)) {
		rc = sr_oom(index->r->e);
	} else {
		sv_init(result, &sv_vif, ret, NULL);
		rc = 1;
	}
	return rc;

add:
	/* track a start of the latest read sequence in the
	 * transactional log */
	if (x->log_read == -1)
		x->log_read = sv_logcount(x->log);
	rc = sx_set(x, index, key->v);
	if (ssunlikely(rc == -1))
		return -1;
	sv_vref((svv*)key->v);
	return 0;
}

sxstate sx_set_autocommit(sxmanager *m, sxindex *index, sx *x, svlog *log, svv *v)
{
	if (sslikely(m->count_rw == 0)) {
		sx_init(m, x, log);
		svlogv lv;
		lv.index_id = index->dsn;
		lv.next = UINT32_MAX;
		sv_init(&lv.v, &sv_vif, v, NULL);
		sv_logadd(x->log, index->r, &lv);
		sr_seq(index->r->seq, SR_TSNNEXT);
		sx_promote(x, SX_COMMIT);
		return SX_COMMIT;
	}
	sx_begin(m, x, SX_RW, log, 0);
	int rc = sx_set(x, index, v);
	if (ssunlikely(rc == -1)) {
		sx_rollback(x);
		return SX_ROLLBACK;
	}
	sxstate s = sx_prepare(x, NULL, NULL);
	switch (s) {
	case SX_PREPARE:
		s = sx_commit(x);
		break;
	case SX_LOCK:
		s = sx_rollback(x);
		break;
	case SX_ROLLBACK:
		break;
	default:
		assert(0);
	}
	return s;
}

sxstate sx_get_autocommit(sxmanager *m, sxindex *index)
{
	(void)m;
	sr_seq(index->r->seq, SR_TSNNEXT);
	return SX_COMMIT;
}
