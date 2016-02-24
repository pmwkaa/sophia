
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
#include <libso.h>
#include <libsv.h>
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libsx.h>
#include <libsy.h>
#include <libsc.h>
#include <libse.h>

static inline int
se_txwrite(setx *t, sedocument *o, uint8_t flags)
{
	se *e = se_of(&t->o);
	sedb *db = se_cast(o->o.parent, sedb*, SEDB);
	/* validate req */
	if (ssunlikely(t->t.state == SXPREPARE)) {
		sr_error(&e->error, "%s", "transaction is in 'prepare' state (read-only)");
		goto error;
	}

	/* validate database status */
	int status = sr_status(&db->index.status);
	switch (status) {
	case SR_SHUTDOWN_PENDING:
	case SR_DROP_PENDING:
		if (ssunlikely(! se_dbvisible(db, t->t.id))) {
			sr_error(&e->error, "%s", "database is invisible for the transaction");
			goto error;
		}
		break;
	case SR_RECOVER:
	case SR_ONLINE: break;
	default: goto error;
	}
	if (flags == SVUPSERT && !sf_upserthas(&db->scheme.fmt_upsert))
		flags = 0;

	/* prepare document */
	svv *v;
	int rc = se_dbv(db, o, 0, &v);
	if (ssunlikely(rc == -1))
		goto error;
	v->flags = flags;
	v->log = o->log;
	sv vp;
	sv_init(&vp, &sv_vif, v, NULL);
	so_destroy(&o->o, 1);

	/* ensure quota */
	int size = sv_vsize(v);
	ss_quota(&e->quota, SS_QADD, size);

	/* concurrent index only */
	rc = sx_set(&t->t, &db->coindex, v);
	if (ssunlikely(rc == -1)) {
		ss_quota(&e->quota, SS_QREMOVE, size);
		return -1;
	}
	return 0;
error:
	so_destroy(&o->o, 1);
	return -1;
}

static int
se_txset(so *o, so *v)
{
	setx *t = se_cast(o, setx*, SETX);
	sedocument *key = se_cast(v, sedocument*, SEDOCUMENT);
	return se_txwrite(t, key, 0);
}

static int
se_txupsert(so *o, so *v)
{
	setx *t = se_cast(o, setx*, SETX);
	sedocument *key = se_cast(v, sedocument*, SEDOCUMENT);
	return se_txwrite(t, key, SVUPSERT);
}

static int
se_txdelete(so *o, so *v)
{
	setx *t = se_cast(o, setx*, SETX);
	sedocument *key = se_cast(v, sedocument*, SEDOCUMENT);
	return se_txwrite(t, key, SVDELETE);
}

static void*
se_txget(so *o, so *v)
{
	setx *t = se_cast(o, setx*, SETX);
	sedocument *key = se_cast(v, sedocument*, SEDOCUMENT);
	se *e = se_of(&t->o);
	sedb *db = se_cast(key->o.parent, sedb*, SEDB);
	/* validate database */
	int status = sr_status(&db->index.status);
	switch (status) {
	case SR_SHUTDOWN_PENDING:
	case SR_DROP_PENDING:
		if (ssunlikely(! se_dbvisible(db, t->t.id))) {
			sr_error(&e->error, "%s", "database is invisible for the transaction");
			goto error;
		}
		break;
	case SR_ONLINE:
	case SR_RECOVER:
		break;
	default: goto error;
	}
	return se_dbread(db, key, &t->t, 1, NULL, key->order);
error:
	so_destroy(&key->o, 1);
	return NULL;
}

static inline void
se_txfree(so *o)
{
	assert(o->destroyed);
	se *e = se_of(o);
	setx *t = (setx*)o;
	sv_logfree(&t->log, &e->a);
	ss_free(&e->a, o);
}

static inline void
se_txend(setx *t, int rlb, int conflict)
{
	se *e = se_of(&t->o);
	uint32_t count = sv_logcount(&t->log);
	sx_gc(&t->t);
	sv_logreset(&t->log);
	sr_stattx(&e->stat, t->start, count, rlb, conflict);
	se_dbunbind(e, t->t.id);
	so_mark_destroyed(&t->o);
	so_poolgc(&e->tx, &t->o);
}

static int
se_txrollback(so *o, int fe ssunused)
{
	setx *t = se_cast(o, setx*, SETX);
	sx_rollback(&t->t);
	se_txend(t, 1, 0);
	return 0;
}

static int
se_txprepare(sx *x, sv *v, so *o, void *ptr)
{
	sicache *cache = ptr;
	sedb *db = (sedb*)o;
	se *e = se_of(&db->o);

	scread q;
	sc_readopen(&q, &db->r, &db->o, &db->index);
	screadarg *arg = &q.arg;
	arg->v             = *v;
	arg->cache         = cache;
	arg->cachegc       = 0;
	arg->order         = SS_EQ;
	arg->has           = 1;
	arg->vlsn          = x->vlsn;
	arg->vlsn_generate = 0;
	int rc = sc_read(&q, &e->scheduler);
	sc_readclose(&q);
	return rc;
}

static int
se_txcommit(so *o)
{
	setx *t = se_cast(o, setx*, SETX);
	se *e = se_of(o);
	int status = sr_status(&e->status);
	if (ssunlikely(! sr_statusactive_is(status)))
		return -1;
	int recover = (status == SR_RECOVER);

	/* prepare transaction */
	if (t->t.state == SXREADY || t->t.state == SXLOCK)
	{
		sicache *cache = NULL;
		sxpreparef prepare = NULL;
		if (! recover) {
			prepare = se_txprepare;
			cache = si_cachepool_pop(&e->cachepool);
			if (ssunlikely(cache == NULL))
				return sr_oom(&e->error);
		}
		sxstate s = sx_prepare(&t->t, prepare, cache);
		if (cache)
			si_cachepool_push(cache);
		if (s == SXLOCK) {
			sr_stattx_lock(&e->stat);
			return 2;
		}
		if (s == SXROLLBACK) {
			sx_rollback(&t->t);
			se_txend(t, 0, 1);
			return 1;
		}
		assert(s == SXPREPARE);

		sx_commit(&t->t);

		if (t->half_commit) {
			/* Half commit mode.
			 *
			 * A half committed transaction is no longer
			 * being part of concurrent index, but still can be
			 * commited or rolled back.
			 * Yet, it is important to maintain external
			 * serial commit order.
			*/
			return 0;
		}
	}
	assert(t->t.state == SXCOMMIT);

	/* do wal write and backend commit */
	if (ssunlikely(recover))
		recover = (e->conf.recover == 3) ? 2: 1;
	int rc;
	rc = sc_write(&e->scheduler, &t->log, t->lsn, recover);
	if (ssunlikely(rc == -1))
		sx_rollback(&t->t);

	se_txend(t, 0, 0);
	return rc;
}

static int
se_txset_int(so *o, const char *path, int64_t v)
{
	setx *t = se_cast(o, setx*, SETX);
	if (strcmp(path, "lsn") == 0) {
		t->lsn = v;
		return 0;
	} else
	if (strcmp(path, "half_commit") == 0) {
		t->half_commit = v;
		return 0;
	}
	return -1;
}

static int64_t
se_txget_int(so *o, const char *path)
{
	setx *t = se_cast(o, setx*, SETX);
	if (strcmp(path, "deadlock") == 0)
		return sx_deadlock(&t->t);
	return -1;
}

static soif setxif =
{
	.open         = NULL,
	.close        = NULL,
	.destroy      = se_txrollback,
	.free         = se_txfree,
	.error        = NULL,
	.document     = NULL,
	.poll         = NULL,
	.drop         = NULL,
	.setstring    = NULL,
	.setint       = se_txset_int,
	.getobject    = NULL,
	.getstring    = NULL,
	.getint       = se_txget_int,
	.set          = se_txset,
	.upsert       = se_txupsert,
	.del          = se_txdelete,
	.get          = se_txget,
	.begin        = NULL,
	.prepare      = NULL,
	.commit       = se_txcommit,
	.cursor       = NULL
};

so *se_txnew(se *e)
{
	int cache;
	setx *t = (setx*)so_poolpop(&e->tx);
	if (! t) {
		t = ss_malloc(&e->a, sizeof(setx));
		cache = 0;
	} else {
		cache = 1;
	}
	if (ssunlikely(t == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	so_init(&t->o, &se_o[SETX], &setxif, &e->o, &e->o);
	if (! cache)
		sv_loginit(&t->log);
	sx_init(&e->xm, &t->t, &t->log);
	t->start = ss_utime();
	t->lsn = 0;
	t->half_commit = 0;
	sx_begin(&e->xm, &t->t, SXRW, &t->log, UINT64_MAX);
	se_dbbind(e);
	so_pooladd(&e->tx, &t->o);
	return &t->o;
}
