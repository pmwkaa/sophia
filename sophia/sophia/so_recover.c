
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
	so *e = so_of(&db->o);
	/* open and recover repository */
	siconf *c = &db->indexconf;
	c->node_size           = e->ctl.node_size;
	c->node_page_size      = e->ctl.page_size;
	c->path_backup         = e->ctl.backup_path;
	c->path                = db->ctl.path;
	c->path_fail_on_exists = 0;
	/* do not allow to recover existing databases
	 * during online (only create), since logpool
	 * reply is required. */
	if (so_status(&e->status) == SO_ONLINE)
		c->path_fail_on_exists = 1;
	c->name                = db->ctl.name;
	c->sync                = db->ctl.sync;
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
	soobj *transaction = NULL;
	sodb *db = NULL;
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
		transaction = so_objbegin(&e->o);
		if (srunlikely(transaction == NULL))
			goto error;

		while (sr_iterhas(&i)) {
			v = sr_iterof(&i);
			assert(svlsn(v) == lsn);
			/* match a database */
			uint32_t dsn = sl_vdsn(v);
			if (db == NULL || db->ctl.id != dsn)
				db = (sodb*)so_dbmatch_id(e, dsn);
			if (srunlikely(db == NULL)) {
				sr_malfunction(&e->error, "%s",
				               "database id %" PRIu32 "is not declared", dsn);
				goto rlb;
			}
			void *o = so_objobject(&db->o);
			if (srunlikely(o == NULL))
				goto rlb;
			so_objset(o, "key", svkey(v), svkeysize(v));
			so_objset(o, "value", svvalue(v), svvaluesize(v));
			so_objset(o, "log", log);
			if (svflags(v) == SVSET)
				rc = so_objset(transaction, o);
			else
			if (svflags(v) == SVDELETE)
				rc = so_objdelete(transaction, o);
			if (srunlikely(rc == -1))
				goto rlb;
			sr_gcmark(&log->gc, 1);
			sr_iternext(&i);
		}
		if (srunlikely(sl_itererror(&i)))
			goto rlb;

		rc = so_objprepare(transaction, lsn);
		if (srunlikely(rc != 0))
			goto error;
		rc = so_objcommit(transaction);
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
	so_objdestroy(transaction);
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
	seconf *ec = &e->seconf;
	ec->path        = e->ctl.path;
	ec->path_create = e->ctl.path_create;
	ec->path_backup = e->ctl.backup_path;
	ec->sync = 0;
	return se_open(&e->se, &e->r, &e->seconf);
}
