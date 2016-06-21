
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

	/* validate database status */
	if (ssunlikely(! se_active(e)))
		goto error;

	/* ensure memory quota */
	int rc;
	rc = sr_quota(&db->quota, &db->stat);
	if (ssunlikely(rc)) {
		sr_error(&e->error, "%s", "memory quota limit reached");
		goto error;
	}

	/* create document */
	rc = se_document_validate(o, &db->o);
	if (ssunlikely(rc == -1))
		goto error;
	rc = se_document_create(o, flags);
	if (ssunlikely(rc == -1))
		goto error;

	svv *v = o->v.v;
	v->log = o->log;
	sv_vref(v);
	so_destroy(&o->o);

	/* concurrent index only */
	rc = sx_set(&t->t, &db->coindex, v);
	if (ssunlikely(rc == -1))
		return -1;
	return 0;
error:
	so_destroy(&o->o);
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
	se *e = se_of(&t->o);
	sedb *db = se_cast(v->parent, sedb*, SEDB);
	if (! sf_upserthas(&db->scheme->fmt_upsert)) {
		if (key->created <= 1)
			so_destroy(v);
		sr_error(&e->error, "%s", "upsert callback is not set");
		return -1;
	}
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
	if (ssunlikely(! se_active(e)))
		goto error;

	/* ensure batch transactions are write-only */
	if (t->t.isolation == SX_BATCH) {
		sr_error(&e->error, "%s", "transaction is in write-only mode");
		goto error;
	}

	return se_read(db, key, &t->t, t->t.vlsn, NULL);
error:
	so_destroy(&key->o);
	return NULL;
}

static inline void
se_txfree(so *o)
{
	assert(o->destroyed);
	se *e = se_of(o);
	setx *t = (setx*)o;
	sv_logfree(&t->log, &e->r);
	ss_free(&e->a, o);
}

static inline void
se_txend(setx *t, int rlb, int conflict)
{
	se *e = se_of(&t->o);
	sx_gc(&t->t);
	sv_logreset(&t->log, e->db.n);
	(void)rlb;
	(void)conflict;
	// XXX
	/*uint32_t count = sv_logcount(&t->log);*/
	/*sr_stattx(&e->stat, t->start, count, rlb, conflict);*/
	so_mark_destroyed(&t->o);
	so_poolgc(&e->tx, &t->o);
}

static int
se_txdestroy(so *o)
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
	siread rq;
	si_readopen(&rq, db->index, cache,
	            SS_EQ,
	            x->vlsn,
	            sv_pointer(v),
	            NULL,
	            NULL,
	            0,
	            0,
	            1,
	            0);
	int rc = si_read(&rq);
	si_readclose(&rq);
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
	int rc;

	/* prepare transaction */
	if (t->t.state == SX_READY || t->t.state == SX_LOCK)
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
		if (s == SX_LOCK) {
			// XXX
			/*sr_stattx_lock(&e->stat);*/
			return 2;
		}
		if (s == SX_ROLLBACK) {
			sx_rollback(&t->t);
			se_txend(t, 0, 1);
			return 1;
		}
		assert(s == SX_PREPARE);

		sx_commit(&t->t);
	}
	assert(t->t.state == SX_COMMIT);

	/* wal write and multi-index write */
	rc = sc_commit(&e->scheduler, &t->log, t->lsn, recover);
	if (ssunlikely(rc == -1)) {
		/* free the transaction log in case of
		 * commit error */
		ssiter i;
		ss_iterinit(ss_bufiter, &i);
		ss_iteropen(ss_bufiter, &i, &t->log.buf, sizeof(svlogv));
		for (; ss_iterhas(ss_bufiter, &i); ss_iternext(ss_bufiter, &i))
		{
			svlogv *lv = ss_iterof(ss_bufiter, &i);
			sedb *db = (sedb*)se_dbmatch_id(e, lv->index_id);
			assert(db != NULL);
			svv *v = lv->v.v;
			sv_vunref(db->r, v);
		}
	}
	se_txend(t, 0, 0);
	return rc;
}

static int
se_txset_string(so *o, const char *path, void *pointer, int size)
{
	setx *t = se_cast(o, setx*, SETX);
	if (strcmp(path, "isolation") == 0) {
		if (size == 0)
			size = strlen(pointer);
		return sx_isolation(&t->t, pointer, size);
	}
	return -1;
}

static int
se_txset_int(so *o, const char *path, int64_t v)
{
	setx *t = se_cast(o, setx*, SETX);
	if (strcmp(path, "lsn") == 0) {
		t->lsn = v;
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
	.destroy      = se_txdestroy,
	.free         = se_txfree,
	.document     = NULL,
	.setstring    = se_txset_string,
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
	if (! cache) {
		int rc = sv_loginit(&t->log, &e->r, e->db.n);
		if (ssunlikely(rc == -1)) {
			ss_free(&e->a, t);
			return NULL;
		}
		sslist *i;
		ss_listforeach(&e->db.list, i) {
			sedb *db = (sedb*)sscast(i, so, link);
			sv_loginit_index(&t->log, db->index->scheme.id, db->r);
		}
	}
	sx_init(&e->xm, &t->t, &t->log);
	t->start = ss_utime();
	t->lsn = 0;
	sx_begin(&e->xm, &t->t, SX_RW, &t->log, UINT64_MAX);
	so_pooladd(&e->tx, &t->o);
	return &t->o;
}
