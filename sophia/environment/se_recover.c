
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
#include <libsw.h>
#include <libsd.h>
#include <libsi.h>
#include <libsx.h>
#include <libsy.h>
#include <libsc.h>
#include <libse.h>

static int
se_recover_log(se *e, sw *log)
{
	so *tx = NULL;
	sedb *db = NULL;
	ssiter i;
	ss_iterinit(sw_iter, &i);
	int processed = 0;
	int rc = ss_iteropen(sw_iter, &i, &e->r, &log->file, 1);
	if (ssunlikely(rc == -1))
		return -1;
	for (;;)
	{
		swv *v = ss_iteratorof(&i);
		if (ssunlikely(v == NULL))
			break;

		/* reply transaction */
		uint64_t lsn = UINT64_MAX;
		tx = so_begin(&e->o);
		if (ssunlikely(tx == NULL))
			goto error;

		while (ss_iteratorhas(&i)) {
			v = ss_iteratorof(&i);
			/* match a database */
			uint32_t dsn = v->dsn;
			if (db == NULL || db->scheme->id != dsn)
				db = (sedb*)se_dbmatch_id(e, dsn);
			if (ssunlikely(db == NULL)) {
				sr_malfunction(&e->error, "database id %" PRIu32
				               " is not declared", dsn);
				goto rlb;
			}
			char *data = sw_vpointer(v);
			lsn = sf_lsn(db->r->scheme, data);
			so *o = so_document(&db->o);
			if (ssunlikely(o == NULL))
				goto rlb;
			so_setstring(o, "raw", data, 0);
			so_setstring(o, "log", log, 0);
			
			int flags = sf_flags(db->r->scheme, data);
			if (flags == SVDELETE) {
				rc = so_delete(tx, o);
			} else
			if (flags == SVUPSERT) {
				rc = so_upsert(tx, o);
			} else {
				assert(flags == 0);
				rc = so_set(tx, o);
			}
			if (ssunlikely(rc == -1))
				goto rlb;
			ss_gcmark(&log->gc, 1);
			processed++;
			if ((processed % 100000) == 0)
				sr_log(&e->log, " %.1fM processed", processed / 1000000.0);
			ss_iteratornext(&i);
		}
		if (ssunlikely(sw_iter_error(&i)))
			goto rlb;

		so_setint(tx, "lsn", lsn);
		rc = so_commit(tx);
		if (ssunlikely(rc != 0))
			goto error;
		rc = sw_iter_continue(&i);
		if (ssunlikely(rc == -1))
			goto error;
		if (rc == 0)
			break;
	}
	ss_iteratorclose(&i);
	return 0;
rlb:
	so_destroy(tx);
error:
	ss_iteratorclose(&i);
	return -1;
}

static inline int
se_recover_logpool(se *e)
{
	sr_log(&e->log, "loading journals '%s'", e->wm_conf->path);
	uint32_t current = 1;
	sslist *i;
	ss_listforeach(&e->wm.list, i) {
		sw *log = sscast(i, sw, link);
		sr_log(&e->log, "(%" PRIu32 "/%" PRIu32 ") %020" PRIu64".log",
		       current, e->wm.n, log->id);
		int rc = se_recover_log(e, log);
		if (ssunlikely(rc == -1))
			return -1;
		current++;
	}
	return 0;
}

int se_recover(se *e)
{
	int rc = sw_manageropen(&e->wm);
	if (ssunlikely(rc == -1))
		goto error;
	rc = se_recover_logpool(e);
	if (ssunlikely(rc == -1))
		goto error;
	return 0;
error:
	sr_statusset(&e->status, SR_MALFUNCTION);
	return -1;
}
