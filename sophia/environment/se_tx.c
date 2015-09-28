
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
#include <libse.h>

static inline int
se_txwrite(setx *t, sev *o, uint8_t flags)
{
	se *e = se_of(&t->o);
	sedb *db = se_cast(o->o.parent, sedb*, SEDB);
	/* validate req */
	if (ssunlikely(t->t.state == SXPREPARE)) {
		sr_error(&e->error, "%s", "transaction is in 'prepare' state (read-only)");
		goto error;
	}

	/* validate database status */
	int status = se_status(&db->status);
	switch (status) {
	case SE_SHUTDOWN:
		if (ssunlikely(! se_dbvisible(db, t->t.id))) {
			sr_error(&e->error, "%s", "database is invisible for the transaction");
			goto error;
		}
		break;
	case SE_RECOVER:
	case SE_ONLINE: break;
	default: goto error;
	}
	if (flags == SVUPDATE && !sf_updatehas(&db->scheme.fmt_update))
		flags = 0;

	/* prepare object */
	svv *v;
	int rc = se_dbv(db, o, 0, &v);
	if (ssunlikely(rc == -1))
		goto error;
	v->flags = flags;
	v->log = o->log;
	sv vp;
	sv_init(&vp, &sv_vif, v, NULL);
	so_destroy(&o->o);

	/* ensure quota */
	int size = sizeof(svv) + sv_size(&vp);
	ss_quota(&e->quota, SS_QADD, size);

	/* concurrent index only */
	rc = sx_set(&t->t, &db->coindex, v);
	if (ssunlikely(rc == -1)) {
		ss_quota(&e->quota, SS_QREMOVE, size);
		return -1;
	}
	return 0;
error:
	so_destroy(&o->o);
	return -1;
}

static int
se_txset(so *o, so *v)
{
	setx  *t = se_cast(o, setx*, SETX);
	sev *key = se_cast(v, sev*, SEV);
	return se_txwrite(t, key, 0);
}

static int
se_txupdate(so *o, so *v)
{
	setx  *t = se_cast(o, setx*, SETX);
	sev *key = se_cast(v, sev*, SEV);
	return se_txwrite(t, key, SVUPDATE);
}

static int
se_txdelete(so *o, so *v)
{
	setx  *t = se_cast(o, setx*, SETX);
	sev *key = se_cast(v, sev*, SEV);
	return se_txwrite(t, key, SVDELETE);
}

static void*
se_txget(so *o, so *v)
{
	setx  *t = se_cast(o, setx*, SETX);
	sev *key = se_cast(v, sev*, SEV);
	se *e = se_of(&t->o);
	sedb *db = se_cast(key->o.parent, sedb*, SEDB);
	/* validate database */
	int status = se_status(&db->status);
	switch (status) {
	case SE_SHUTDOWN:
		if (ssunlikely(! se_dbvisible(db, t->t.id))) {
			sr_error(&e->error, "%s", "database is invisible for the transaction");
			goto error;
		}
		break;
	case SE_ONLINE:
	case SE_RECOVER:
		break;
	default: goto error;
	}
	return se_dbread(db, key, &t->t, 1, NULL, SS_EQ);
error:
	so_destroy(&key->o);
	return NULL;
}

void se_txend(setx *t)
{
	se *e = se_of(&t->o);
	sx_gc(&t->t);
	se_dbunbind(e, t->t.id);
	so_listdel(&e->tx, &t->o);
	se_mark_destroyed(&t->o);
	ss_free(&e->a_tx, t);
}

static int
se_txrollback(so *o)
{
	setx *t = se_cast(o, setx*, SETX);
	sx_rollback(&t->t);
	se_txend(t);
	return 0;
}

static int
se_txcommit(so *o)
{
	setx *t = se_cast(o, setx*, SETX);
	se *e = se_of(o);
	int status = se_status(&e->status);
	if (ssunlikely(! se_statusactive_is(status)))
		return -1;
	int recover = (status == SE_RECOVER);
	/* prepare transaction */
	if (ssunlikely(! sv_logcount(&t->t.log))) {
		sx_prepare(&t->t);
		sx_commit(&t->t);
		se_txend(t);
		return 0;
	}
	if (t->t.state == SXREADY || t->t.state == SXLOCK)
	{
		sxstate s = sx_prepare(&t->t);
		if (s == SXLOCK)
			return 2;
		if (s == SXROLLBACK) {
			sx_rollback(&t->t);
			se_txend(t);
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

	/* prepare for wal and index write */
	sereq q;
	se_reqinit(e, &q, SE_REQWRITE, &t->o, NULL);
	sereqarg *arg = &q.arg;
	arg->log = &t->t.log;
	arg->lsn = 0;
	if (recover || e->meta.commit_lsn)
		arg->lsn = t->lsn;
	if (ssunlikely(recover)) {
		arg->recover = 1;
		arg->vlsn_generate = 0;
		arg->vlsn = sr_seq(e->r.seq, SR_LSN);
	} else {
		arg->vlsn_generate = 1;
		arg->vlsn = 0;
	}
	/* log write and commit */
	se_execute(&q);

	se_txend(t);
	return q.rc;
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

static soif setxif =
{
	.open         = NULL,
	.destroy      = se_txrollback,
	.error        = NULL,
	.object       = NULL,
	.asynchronous = NULL,
	.poll         = NULL,
	.drop         = NULL,
	.setobject    = NULL,
	.setstring    = NULL,
	.setint       = se_txset_int,
	.getobject    = NULL,
	.getstring    = NULL,
	.getint       = NULL,
	.set          = se_txset,
	.update       = se_txupdate,
	.del          = se_txdelete,
	.get          = se_txget,
	.batch        = NULL,
	.begin        = NULL,
	.prepare      = NULL,
	.commit       = se_txcommit,
	.cursor       = NULL,
};

so *se_txnew(se *e)
{
	setx *t = ss_malloc(&e->a_tx, sizeof(setx));
	if (ssunlikely(t == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	memset(t, 0, sizeof(*t));
	so_init(&t->o, &se_o[SETX], &setxif, &e->o, &e->o);
	sx_init(&e->xm, &t->t);
	t->lsn = 0;
	sx_begin(&e->xm, &t->t, SXRW, 0);
	se_dbbind(e);
	so_listadd(&e->tx, &t->o);
	return &t->o;
}
