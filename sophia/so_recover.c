
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
		if (lsn > db->r.seq->lsn)
			db->r.seq->lsn = lsn;

		/* ensure that this update was not previously
		 * merged to disk */
		if (si_querycommited(&db->index, &db->r, v))
		{
			/* skip transaction */
			while (sr_iterhas(&i)) {
				sr_gcmark(&log->gc, 1);
				sr_gcsweep(&log->gc, 1);
				sr_iternext(&i);
			}
			if (srunlikely(sl_itererror(&i)))
				goto error;
			if (! sl_itercontinue(&i) )
				break;
			continue;
		}
		tx = sp_begin(db);
		if (srunlikely(tx == NULL))
			goto error;
		while (sr_iterhas(&i)) {
			v = sr_iterof(&i);
			assert(svlsn(v) == lsn);
			rc = sp_set(tx, v);
			if (srunlikely(rc == -1))
				goto rlb;
			sr_gcmark(&log->gc, 1);
			sr_iternext(&i);
		}
		if (srunlikely(sl_itererror(&i)))
			goto rlb;
		rc = sp_commit(tx, lsn, log);
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
	sp_rollback(tx);
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
	sr_seq(db->r.seq, SR_LSNNEXT);
	return 0;
}

int so_recover(sodb *db)
{
	db->mode = SO_RECOVER;
	siconf *c = &db->indexconf;
	c->node_size      = db->conf.node_size;
	c->node_page_size = db->conf.node_page_size;
	c->node_branch_wm = db->conf.node_branch_wm;
	c->node_merge_wm  = db->conf.node_merge_wm;
	c->memory_limit   = db->conf.memory_limit;
	c->dir            = db->conf.dir;
	c->dir_read       = db->conf.dir_read;
	c->dir_write      = db->conf.dir_write;
	c->dir_create     = db->conf.dir_create;
	si_init(&db->index, &db->indexconf);
	int rc = si_open(&db->index, &db->r);
	if (srunlikely(rc == -1))
		return -1;
	si_qosenable(&db->index, 0);
	int index_isnew = rc;
	slconf *lc = &db->lpconf;
	lc->dir          = db->conf.logdir;
	lc->dir_read     = db->conf.logdir_read;
	lc->dir_write    = db->conf.logdir_write;
	lc->dir_create   = db->conf.logdir_create;
	lc->rotatewm     = db->conf.logdir_rotate_wm;
	rc = sl_poolinit(&db->lp, &db->r, lc);
	if (srunlikely(rc == -1))
		return -1;
	rc = sl_poolopen(&db->lp);
	if (srunlikely(rc == -1))
		return -1;
	if (! index_isnew) {
		rc = so_recoverlogpool(db);
		if (srunlikely(rc == -1))
			return -1;
	}
	rc = sl_poolrotate(&db->lp);
	if (srunlikely(rc == -1))
		return -1;
	si_qosenable(&db->index, 1);
	return 0;
}
