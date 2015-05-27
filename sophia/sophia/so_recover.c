
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
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libsx.h>
#include <libse.h>
#include <libso.h>

int so_recoverbegin(sodb *db)
{
	/* open and recover repository */
	so_statusset(&db->status, SO_RECOVER);
	so *e = so_of(&db->o);
	/* do not allow to recover existing databases
	 * during online (only create), since logpool
	 * reply is required. */
	if (so_status(&e->status) == SO_ONLINE)
		db->scheme.path_fail_on_exists = 1;
	int rc = si_open(&db->index, &db->r, &db->scheme);
	if (ssunlikely(rc == -1))
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
	srobj *transaction = NULL;
	sodb *db = NULL;
	ssiter i;
	ss_iterinit(sl_iter, &i);
	int rc = ss_iteropen(sl_iter, &i, &e->r, &log->file, 1);
	if (ssunlikely(rc == -1))
		return -1;
	for (;;)
	{
		sv *v = ss_iteratorof(&i);
		if (ssunlikely(v == NULL))
			break;

		/* reply transaction */
		uint64_t lsn = sv_lsn(v);
		transaction = sr_objbegin(&e->o);
		if (ssunlikely(transaction == NULL))
			goto error;

		while (ss_iteratorhas(&i)) {
			v = ss_iteratorof(&i);
			assert(sv_lsn(v) == lsn);
			/* match a database */
			uint32_t dsn = sl_vdsn(v);
			if (db == NULL || db->scheme.id != dsn)
				db = (sodb*)so_dbmatch_id(e, dsn);
			if (ssunlikely(db == NULL)) {
				sr_malfunction(&e->error, "%s",
				               "database id %" PRIu32 "is not declared", dsn);
				goto rlb;
			}
			void *o = sr_objobject(&db->o);
			if (ssunlikely(o == NULL))
				goto rlb;
			sr_objset(o, "raw", sv_pointer(v), sv_size(v));
			sr_objset(o, "log", log);
			int flags = sv_flags(v);
			if (flags == SVDELETE) {
				rc = sr_objdelete(transaction, o);
			} else {
				assert(flags == 0);
				rc = sr_objset(transaction, o);
			}
			if (ssunlikely(rc == -1))
				goto rlb;
			ss_gcmark(&log->gc, 1);
			ss_iteratornext(&i);
		}
		if (ssunlikely(sl_iter_error(&i)))
			goto rlb;

		rc = sr_objcommit(transaction, lsn);
		if (ssunlikely(rc != 0))
			goto error;
		rc = sl_iter_continue(&i);
		if (ssunlikely(rc == -1))
			goto error;
		if (rc == 0)
			break;
	}
	ss_iteratorclose(&i);
	return 0;
rlb:
	sr_objdestroy(transaction);
error:
	ss_iteratorclose(&i);
	return -1;
}

static inline int
so_recoverlogpool(so *e)
{
	sslist *i;
	ss_listforeach(&e->lp.list, i) {
		sl *log = sscast(i, sl, link);
		int rc = so_recoverlog(e, log);
		if (ssunlikely(rc == -1))
			return -1;
		ss_gccomplete(&log->gc);
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
	if (ssunlikely(rc == -1))
		return -1;
	if (e->ctl.two_phase_recover)
		return 0;
	/* recover log files */
	rc = so_recoverlogpool(e);
	if (ssunlikely(rc == -1))
		goto error;
	rc = sl_poolrotate(&e->lp);
	if (ssunlikely(rc == -1))
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
