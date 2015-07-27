
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

static int
se_batchrollback(so *o)
{
	sebatch *b = se_cast(o, sebatch*, SEBATCH);
	se *e = se_of(o);
	ssiter i;
	ss_iterinit(ss_bufiter, &i);
	ss_iteropen(ss_bufiter, &i, &b->log.buf, sizeof(svlogv));
	int gc = 0;
	for (; ss_iterhas(ss_bufiter, &i); ss_iternext(ss_bufiter, &i))
	{
		svlogv *lv = ss_iterof(ss_bufiter, &i);
		gc += sv_vsize((svv*)lv->v.v);
		ss_free(&e->a, lv->v.v);
	}
	ss_quota(&e->quota, SS_QREMOVE, gc);
	sv_logfree(&b->log, &e->a);
	sedb *db = (sedb*)b->o.parent;
	se_dbunref(db, 0);
	so_listdel(&db->batch, &b->o);
	se_mark_destroyed(&b->o);
	ss_free(&e->a_batch, b);
	return 0;
}

static inline int
se_batchwrite(sebatch *b, sev *o, uint8_t flags)
{
	se *e = se_of(&b->o);
	sedb *db = se_cast(o->o.parent, sedb*, SEDB);

	/* xxx: validate database status */
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

	/* log add */
	svlogv lv;
	sv_logvinit(&lv, db->coindex.dsn);
	sv_init(&lv.v, &sv_vif, v, NULL);
	rc = sv_logadd(&b->log, &e->a, &lv, db->coindex.ptr);
	if (ssunlikely(rc == -1)) {
		ss_quota(&e->quota, SS_QREMOVE, size);
		return sr_oom(&e->error);
	}
	return 0;
error:
	so_destroy(&o->o);
	return -1;
}

static int
se_batchset(so *o, so *v)
{
	sebatch *b = se_cast(o, sebatch*, SEBATCH);
	sev *key = se_cast(v, sev*, SEV);
	return se_batchwrite(b, key, 0);
}

static int
se_batchupdate(so *o, so *v)
{
	sebatch *b = se_cast(o, sebatch*, SEBATCH);
	sev *key = se_cast(v, sev*, SEV);
	return se_batchwrite(b, key, SVUPDATE);
}

static int
se_batchdelete(so *o, so *v)
{
	sebatch *b = se_cast(o, sebatch*, SEBATCH);
	sev *key = se_cast(v, sev*, SEV);
	return se_batchwrite(b, key, SVDELETE);
}

static int
se_batchcommit(so *o)
{
	sebatch *b = se_cast(o, sebatch*, SEBATCH);
	se *e = se_of(o);
	uint64_t lsn = sr_seq(&e->seq, SR_LSN);
	if (ssunlikely(lsn != b->lsn)) {
		se_batchrollback(o);
		return 1;
	}
	if (ssunlikely(! sv_logcount(&b->log))) {
		se_batchrollback(o);
		return 0;
	}

	/* log write */
	sl_prepare(&e->lp, &b->log, 0);
	sltx tl;
	sl_begin(&e->lp, &tl);
	int rc = sl_write(&tl, &b->log);
	if (ssunlikely(rc == -1)) {
		sl_rollback(&tl);
		se_batchrollback(o);
		return -1;
	}
	sl_commit(&tl);

	/* commit */
	uint64_t vlsn = sx_vlsn(&e->xm);
	uint64_t now = ss_utime();
	svlogindex *i   = (svlogindex*)b->log.index.s;
	svlogindex *end = (svlogindex*)b->log.index.p;
	while (i < end) {
		sedb *db = i->ptr;
		sitx ti;
		si_begin(&ti, &db->index, vlsn, now, &b->log, i);
		si_write(&ti, 0);
		si_commit(&ti);
		i++;
	}
	sv_logfree(&b->log, &e->a);
	return 0;
}

static soif sebatchif =
{
	.open         = NULL,
	.destroy      = se_batchrollback,
	.error        = NULL,
	.object       = NULL,
	.asynchronous = NULL,
	.poll         = NULL,
	.drop         = NULL,
	.setobject    = NULL,
	.setstring    = NULL,
	.setint       = NULL,
	.getobject    = NULL,
	.getstring    = NULL,
	.getint       = NULL,
	.set          = se_batchset,
	.update       = se_batchupdate,
	.del          = se_batchdelete,
	.get          = NULL,
	.batch        = NULL,
	.begin        = NULL,
	.prepare      = NULL,
	.commit       = se_batchcommit,
	.cursor       = NULL,
};

so *se_batchnew(sedb *db)
{
	se *e = se_of(&db->o);
	sebatch *b = ss_malloc(&e->a_batch, sizeof(sebatch));
	if (ssunlikely(b == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	so_init(&b->o, &se_o[SEBATCH], &sebatchif, &db->o, &e->o);
	b->lsn = sr_seq(&e->seq, SR_LSN);
	sv_loginit(&b->log);
	se_dbref(db, 0);
	so_listadd(&db->batch, &b->o);
	return &b->o;
}
