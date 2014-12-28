
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsx.h>
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libse.h>
#include <libso.h>

int so_recoverbegin(sodb *db)
{
	so_statusset(&db->status, SO_RECOVER);
	so *e = db->e;
	/* open and recover repository */
	siconf *c = &db->indexconf;
	c->node_size      = e->ctl.node_size;
	c->node_page_size = e->ctl.page_size;
	c->path           = db->ctl.path;
	c->sync           = db->ctl.sync;
	int rc = si_open(&db->index, &db->r, &db->indexconf);
	if (srunlikely(rc == -1))
		goto error;
	db->ctl.created = rc;
	return 0;
error:
	so_dbmalfunction(db);
	return -1;
}

int so_recoverend(sodb *db)
{
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

		/* reply transaction */
		uint64_t lsn = svlsn(v);
		tx = so_objbegin(&e->o);
		if (srunlikely(tx == NULL))
			goto error;

		while (sr_iterhas(&i)) {
			v = sr_iterof(&i);
			assert(svlsn(v) == lsn);
			/* match a database */
			uint32_t dsn = sl_vdsn(v);
			if (db == NULL || db->ctl.id != dsn)
				db = (sodb*)so_dbmatch_id(e, dsn);
			if (srunlikely(db == NULL)) {
				sr_error(&e->error, "%s",
				         "database id %" PRIu32 "is not declared", dsn);
				goto rlb;
			}
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
	lc->enable         = e->ctl.log_enable;
	lc->path           = e->ctl.log_path;
	lc->rotatewm       = e->ctl.log_rotate_wm;
	lc->sync_on_rotate = e->ctl.log_rotate_sync;
	lc->sync_on_write  = e->ctl.log_sync;
	int rc = sl_poolopen(&e->lp, lc);
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

int so_recover_repository(so *e)
{
	e->seconf.path = e->ctl.path;
	e->seconf.sync = 0;
	return se_open(&e->se, &e->r, &e->seconf);
}

int so_recover_snapshot(so *e)
{
	if (srunlikely(e->ctl.disable_snapshot))
		return 0;
	/* recreate snapshot objects */
	sosnapshotdb *db = (sosnapshotdb*)so_dbmatch(e, "snapshot");
	assert(db != NULL);
	assert(db->o.id == SOSNAPSHOTDB);
	void *o = so_objobject(&db->o);
	if (srunlikely(o == NULL))
		return -1;
	void *c = so_objcursor(&db->o, o);
	if (srunlikely(c == NULL))
		return -1;
	while ((o = so_objget(c))) {
		char *name = so_objget(o, "key", NULL);
		uint64_t lsn = *(uint64_t*)so_objget(o, "value", NULL);
		soobj *s = so_snapshotnew(db, lsn, name);
		if (srunlikely(s == NULL)) {
			so_objdestroy(c);
			return -1;
		}
		so_objindex_register(&db->list, s);
	}
	so_objdestroy(c);
	return 0;
}
