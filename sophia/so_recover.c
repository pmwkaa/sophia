
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

static inline int
so_recoverlog(sodb *db, sl *log)
{
	void *tx = NULL;
	sriter i;
	sr_iterinit(&i, &sl_iter, &db->r);
	int rc = sr_iteropen(&i, &log->file, 1);
	if (srunlikely(rc == -1))
		return -1;
	for (;;)
	{
		sv *v = sr_iterof(&i);
		if (srunlikely(v == NULL))
			break;

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
so_recoverlogpool(sodb *db)
{
	srlist *i;
	sr_listforeach(&db->lp.list, i) {
		sl *log = srcast(i, sl, link);
		int rc = so_recoverlog(db, log);
		if (srunlikely(rc == -1))
			return -1;
		sr_gccomplete(&log->gc);
	}
	return 0;
}

int so_recover(sodb *db)
{
	so_statusset(&db->status, SO_RECOVER);

	/* turn off memory limits during recovery */
	si_qosenable(&db->index, 0);

	/* open logdir */
	slconf *lc = &db->lpconf;
	lc->dir            = db->ctl.log_dir;
	lc->dir_write      = db->ctl.log_dirwrite;
	lc->dir_create     = db->ctl.log_dircreate;
	lc->rotatewm       = db->ctl.log_rotate_wm;
	lc->sync_on_rotate = db->ctl.log_rotate_sync;
	lc->sync_on_write  = db->ctl.log_sync;
	int rc = sl_poolinit(&db->lp, &db->r, lc);
	if (srunlikely(rc == -1))
		return -1;
	rc = sl_poolopen(&db->lp);
	if (srunlikely(rc == -1))
		return -1;

	/* open and recover repository */
	siconf *c = &db->indexconf;
	c->node_size      = db->e->ctl.node_size;
	c->node_page_size = db->e->ctl.node_page_size;
	c->node_branch_wm = db->e->ctl.node_branch_wm;
	c->node_merge_wm  = db->e->ctl.node_merge_wm;
	c->dir            = db->ctl.dir;
	c->dir_write      = db->ctl.dir_write;
	c->dir_create     = db->ctl.dir_create;
	c->sync           = db->ctl.dir_sync;
	si_init(&db->index, &db->indexconf);
	rc = si_open(&db->index, &db->r);
	if (srunlikely(rc == -1))
		goto error;
	db->ctl.dir_created = rc;

	/* recover log files */
	if (db->ctl.two_phase_recover)
		return 0;
	if (! db->ctl.dir_created) {
		rc = so_recoverlogpool(db);
		if (srunlikely(rc == -1))
			goto error;
	}
	rc = sl_poolrotate(&db->lp);
	if (srunlikely(rc == -1))
		goto error;
	return 0;
error:
	so_dbmalfunction(db);
	return -1;
}
