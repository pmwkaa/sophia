
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsm.h>

int sm_init(sm *c, sr *r, sra *asmv)
{
	sr_rbinit(&c->t);
	sr_rbinit(&c->i);
	sr_spinlockinit(&c->lock);
	c->tn = 0;
	c->asmv = asmv;
	c->r = r;
	return 0;
}

sr_rbtruncate(sm_truncate,
              sm_vfree(((sra**)arg)[0],
                       ((sra**)arg)[1], srcast(n, smv, node)))

int sm_free(sm *c)
{
	/* rollback active transactions */
	sra *allocators[2] = { c->r->a, c->asmv };
	if (c->i.root)
		sm_truncate(c->i.root, allocators);
	sr_spinlockfree(&c->lock);
	return 0;
}

uint64_t sm_lsvn(sm *c)
{
	sr_spinlock(&c->lock);
	uint64_t lsvn;
	if (c->tn) {
		srrbnode *node = sr_rbmin(&c->t);
		smtx *min = srcast(node, smtx, node);
		lsvn = min->lsvn;
	} else {
		lsvn = sr_seq(c->r->seq, SR_LSN) - 1;
	}
	sr_spinunlock(&c->lock);
	return lsvn;
}

sr_rbget(sm_matchtx,
         sr_cmpu32((char*)&(srcast(n, smtx, node))->id, sizeof(uint32_t),
                   (char*)key, sizeof(uint32_t), NULL))

smtx *sm_find(sm *c, uint32_t id)
{
	srrbnode *n = NULL;
	int rc = sm_matchtx(&c->t, NULL, (char*)&id, sizeof(id), &n);
	if (rc == 0 && n)
		return  srcast(n, smtx, node);
	return NULL;
}

smstate sm_begin(sm *c, smtx *t)
{
	t->s = SMREADY; 
	t->c = c;
	sr_seqlock(c->r->seq);
	t->id   = sr_seqdo(c->r->seq, SR_TSNNEXT);
	t->lsvn = sr_seqdo(c->r->seq, SR_LSN) - 1;
	sr_sequnlock(c->r->seq);
	sv_loginit(&t->log);
	sr_listinit(&t->deadlock);
	sr_spinlock(&c->lock);
	srrbnode *n = NULL;
	int rc = sm_matchtx(&c->t, NULL, (char*)&t->id, sizeof(t->id), &n);
	if (rc == 0 && n) {
		assert(0);
	} else {
		sr_rbset(&c->t, n, rc, &t->node);
	}
	c->tn++;
	sr_spinunlock(&c->lock);
	return SMREADY;
}

smstate sm_end(smtx *t)
{
	sm *c = t->c;
	assert(t->s != SMUNDEF);
	sr_spinlock(&c->lock);
	sr_rbremove(&c->t, &t->node);
	t->s = SMUNDEF;
	c->tn--;
	sr_spinunlock(&c->lock);
	sv_logfree(&t->log, c->r->a);
	return SMUNDEF;
}

smstate sm_prepare(smtx *t, smpreparef prepare, void *arg)
{
	sm *c = t->c;
	sriter i;
	sr_iterinit(&i, &sr_bufiter, c->r);
	sr_iteropen(&i, &t->log.buf, sizeof(sv));
	smstate s;
	for (; sr_iterhas(&i); sr_iternext(&i))
	{
		sv *vp = sr_iterof(&i);
		smv *v = vp->v;
		/* cancelled by a concurrent commited
		 * transaction */
		if (v->v->flags & SVABORT)
			return SMROLLBACK;
		/* concurrent update in progress */
		if (v->prev != NULL)
			return SMWAIT;
		/* check that new key has not been committed by
		 * a concurrent transaction */
		if (prepare) {
			s = prepare(t, vp, arg);
			if (srunlikely(s != SMPREPARE))
				return s;
		}
	}
	s = SMPREPARE;
	t->s = s;
	return s;
}

smstate sm_commit(smtx *t)
{
	assert(t->s == SMPREPARE);
	sm *c = t->c;
	sriter i;
	sr_iterinit(&i, &sr_bufiter, c->r);
	sr_iteropen(&i, &t->log.buf, sizeof(sv));
	for (; sr_iterhas(&i); sr_iternext(&i))
	{
		sv *vp = sr_iterof(&i);
		smv *v = vp->v;
		/* mark waiters as aborted */
		sm_vabortwaiters(v);
		/* remove from concurrent index and replace
		 * head with a first waiter */
		if (v->next == NULL)
			sr_rbremove(&c->i, &v->node);
		else
			sr_rbreplace(&c->i, &v->node, &v->next->node);
		/* unlink version */
		sm_vunlink(v);
		/* translate log version from smv to svv */
		svinit(vp, &sv_vif, v->v, NULL);
		sr_free(c->asmv, v);
	}
	t->s = SMCOMMIT;
	return SMCOMMIT;
}

smstate sm_rollback(smtx *t)
{
	sm *c = t->c;
	sriter i;
	sr_iterinit(&i, &sr_bufiter, c->r);
	sr_iteropen(&i, &t->log.buf, sizeof(sv));
	for (; sr_iterhas(&i); sr_iternext(&i))
	{
		sv *vp = sr_iterof(&i);
		smv *v = vp->v;
		/* remove from index and replace head with
		 * a first waiter */
		if (v->prev)
			goto unlink;
		if (v->next == NULL)
			sr_rbremove(&c->i, &v->node);
		else
			sr_rbreplace(&c->i, &v->node, &v->next->node);
unlink:
		sm_vunlink(v);
		sm_vfree(c->r->a, c->asmv, v);
	}
	t->s = SMROLLBACK;
	return SMROLLBACK;
}

sr_rbget(sm_match,
         sr_compare(cmp, sv_vkey((srcast(n, smv, node))->v),
                    (srcast(n, smv, node))->v->keysize,
                    key, keysize))

int sm_set(smtx *t, svv *version)
{
	sm *c = t->c;
	/* allocate mvcc container */
	smv *v = sm_valloc(c->asmv, version);
	if (srunlikely(v == NULL))
		return -1;
	v->id = t->id;
	sv vv;
	svinit(&vv, &sm_vif, v, NULL);
	/* update concurrent index */
	srrbnode *n = NULL;
	int rc = sm_match(&c->i, c->r->cmp,
	                  sv_vkey(version), version->keysize, &n);
	if (rc == 0 && n) {
		/* exists */
	} else {
		/* unique */
		v->lo = sv_logn(&t->log);
		if (srunlikely(sv_logadd(&t->log, c->r->a, &vv) == -1))
			return sr_error(t->c->r->e, "%s", "memory allocation failed");
		sr_rbset(&c->i, n, rc, &v->node);
		return 0;
	}
	smv *head = srcast(n, smv, node);
	/* match previous update made by current
	 * transaction */
	smv *own = sm_vmatch(head, t->id);
	if (srunlikely(own)) {
		/* replace old object with the new one */
		v->lo = own->lo;
		sm_vreplace(own, v);
		if (srlikely(head == own))
			sr_rbreplace(&c->i, &own->node, &v->node);
		/* update log */
		sv_logreplace(&t->log, v->lo, &vv);
		sm_vfree(c->r->a, c->asmv, own);
		return 0;
	}
	/* update log */
	rc = sv_logadd(&t->log, c->r->a, &vv);
	if (srunlikely(rc == -1)) {
		sm_vfree(c->r->a, c->asmv, v);
		return sr_error(t->c->r->e, "%s", "memory allocation failed");
	}
	/* add version */
	sm_vlink(head, v);
	return 0;
}

int sm_get(smtx *t, sv *key, sv *result)
{
	sm *c = t->c;
	srrbnode *n = NULL;
	int rc = sm_match(&c->i, c->r->cmp, svkey(key), svkeysize(key), &n);
	if (! (rc == 0 && n))
		return 0;
	smv *head = srcast(n, smv, node);
	smv *v = sm_vmatch(head, t->id);
	if (v == NULL)
		return 0;
	if (srunlikely((v->v->flags & SVDELETE) > 0))
		return 2;
	sv vv;
	svinit(&vv, &sv_vif, v->v, NULL);
	svv *ret = sv_valloc(c->r->a, &vv);
	if (srunlikely(ret == NULL))
		return sr_error(t->c->r->e, "%s", "memory allocation failed");
	svinit(result, &sv_vif, ret, NULL);
	return 1;
}

smstate sm_set_stmt(sm *c, sv *v)
{
	srrbnode *n = NULL;
	int rc = sm_match(&c->i, c->r->cmp, svkey(v), svkeysize(v), &n);
	if (rc == 0 && n)
		return SMWAIT;
	sr_seq(c->r->seq, SR_TSNNEXT);
	return SMCOMMIT;
}

smstate sm_get_stmt(sm *c)
{
	sr_seq(c->r->seq, SR_TSNNEXT);
	return SMCOMMIT;
}
