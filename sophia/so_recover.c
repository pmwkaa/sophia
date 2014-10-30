
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
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libso.h>
#include <sophia.h>

int so_recoverbegin(sodb *db)
{
	so_statusset(&db->status, SO_RECOVER);
	so *e = db->e;
	/* turn off memory limits during recovery */
	si_qosenable(&db->index, 0);
	/* open and recover repository */
	siconf *c = &db->indexconf;
	c->node_size      = e->ctl.node_size;
	c->node_page_size = e->ctl.node_page_size;
	c->node_branch_wm = e->ctl.node_branch_wm;
	c->node_merge_wm  = e->ctl.node_merge_wm;
	c->dir            = db->ctl.dir;
	c->dir_write      = db->ctl.dir_write;
	c->dir_create     = db->ctl.dir_create;
	c->sync           = db->ctl.dir_sync;
	si_init(&db->index, &db->indexconf);
	int rc = si_open(&db->index, &db->r);
	if (srunlikely(rc == -1))
		goto error;
	db->ctl.dir_created = rc;
	return 0;
error:
	so_dbmalfunction(db);
	return -1;
}

int so_recoverend(sodb *db)
{
	si_qosenable(&db->index, 1);
	so_statusset(&db->status, SO_ONLINE);
	return 0;
}

static inline int
so_recoverlog(so *e, sl *log)
{
	sodb *db = NULL;
	void *tx = NULL;
	sriter i;
	sr_iterinit(&i, &sl_iter, &e->r);
	int rc = sr_iteropen(&i, &log->file, 1);
	if (srunlikely(rc == -1))
		return -1;

	for (;;)
	{
		sv *v = sr_iterof(&i);
		if (srunlikely(v == NULL))
			break;

		/* match a database */
		uint32_t dsn = sl_vdsn(v);
		if (db == NULL || db->ctl.id != dsn)
			db = (sodb*)so_dbmatch_id(e, dsn);
		if (srunlikely(db == NULL)) {
			sr_error(&e->error, "%s",
			         "database id %" PRIu32 "is not declared", dsn);
			goto error;
		}

		/* reply transaction */
		uint64_t lsn = svlsn(v);
		tx = so_objbegin(&db->o);
		if (srunlikely(tx == NULL))
			goto error;
		while (sr_iterhas(&i)) {
			v = sr_iterof(&i);
			assert(svlsn(v) == lsn);
			void *o = so_objobject(&db->o);
			if (srunlikely(o == NULL))
				goto rlb;
			so_objset(o, "key", svkey(v), svkeysize(v));
			so_objset(o, "value", svvalue(v), svvaluesize(v));
			so_objset(o, "lsn", lsn);
			so_objset(o, "log", log);
			if (svflags(v) == SVSET)
				rc = so_objset(tx, o);
			else
			if (svflags(v) == SVDELETE)
				rc = so_objdelete(tx, o);
			if (srunlikely(rc == -1))
				goto rlb;
			sr_gcmark(&log->gc, 1);
			sr_iternext(&i);
		}
		if (srunlikely(sl_itererror(&i)))
			goto rlb;
		rc = so_objcommit(tx);
		if (srunlikely(rc != 0))
			goto error;

		rc = sl_itercontinue(&i);
		if (srunlikely(rc == -1))
			goto error;
		if (rc == 0)
			break;
	}
	sr_iterclose(&i);
	return 0;
rlb:
	so_objrollback(tx);
error:
	sr_iterclose(&i);
	return -1;
}

static inline int
so_recoverlogpool(so *e)
{
	srlist *i;
	sr_listforeach(&e->lp.list, i) {
		sl *log = srcast(i, sl, link);
		int rc = so_recoverlog(e, log);
		if (srunlikely(rc == -1))
			return -1;
		sr_gccomplete(&log->gc);
	}
	return 0;
}

int so_recover(so *e)
{
	slconf *lc = &e->lpconf;
	lc->dir            = e->ctl.log_dir;
	lc->dir_write      = e->ctl.log_dirwrite;
	lc->dir_create     = e->ctl.log_dircreate;
	lc->rotatewm       = e->ctl.log_rotate_wm;
	lc->sync_on_rotate = e->ctl.log_rotate_sync;
	lc->sync_on_write  = e->ctl.log_sync;
	int rc = sl_poolinit(&e->lp, &e->r, lc);
	if (srunlikely(rc == -1))
		return -1;
	rc = sl_poolopen(&e->lp);
	if (srunlikely(rc == -1))
		return -1;
	if (e->ctl.two_phase_recover)
		return 0;
	/* recover log files */
	rc = so_recoverlogpool(e);
	if (srunlikely(rc == -1))
		goto error;
	rc = sl_poolrotate(&e->lp);
	if (srunlikely(rc == -1))
		goto error;
	return 0;
error:
	so_statusset(&e->status, SO_MALFUNCTION);
	return -1;
}
