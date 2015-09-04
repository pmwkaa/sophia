
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
se_open(so *o)
{
	se *e = se_cast(o, se*, SE);
	int status = se_status(&e->status);
	if (status == SE_RECOVER) {
		assert(e->meta.two_phase_recover == 1);
		goto online;
	}
	if (status != SE_OFFLINE)
		return -1;
	int rc;
	rc = se_metavalidate(&e->meta);
	if (ssunlikely(rc == -1))
		return -1;
	se_statusset(&e->status, SE_RECOVER);

	/* set memory quota (disable during recovery) */
	ss_quotaset(&e->quota, e->meta.memory_limit);
	ss_quotaenable(&e->quota, 0);

	/* repository recover */
	rc = se_recover_repository(e);
	if (ssunlikely(rc == -1))
		return -1;
	/* databases recover */
	sslist *i, *n;
	ss_listforeach_safe(&e->db.list, i, n) {
		so *o = sscast(i, so, link);
		rc = so_open(o);
		if (ssunlikely(rc == -1))
			return -1;
	}
	/* recover logpool */
	rc = se_recover(e);
	if (ssunlikely(rc == -1))
		return -1;
	if (e->meta.two_phase_recover)
		return 0;

online:
	/* complete */
	ss_listforeach_safe(&e->db.list, i, n) {
		so *o = sscast(i, so, link);
		rc = so_open(o);
		if (ssunlikely(rc == -1))
			return -1;
	}
	/* enable quota */
	ss_quotaenable(&e->quota, 1);
	se_statusset(&e->status, SE_ONLINE);
	/* run thread-pool and scheduler */
	rc = se_scheduler_run(&e->sched);
	if (ssunlikely(rc == -1))
		return -1;
	return 0;
}

static int
se_destroy(so *o)
{
	se *e = se_cast(o, se*, SE);
	int rcret = 0;
	int rc;
	se_statusset(&e->status, SE_SHUTDOWN);
	rc = se_scheduler_shutdown(&e->sched);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = so_listdestroy(&e->req);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = so_listdestroy(&e->reqactive);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = so_listdestroy(&e->reqready);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = so_listdestroy(&e->cursor);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = so_listdestroy(&e->tx);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = so_listdestroy(&e->snapshot);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = so_listdestroy(&e->metacursor);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = so_listdestroy(&e->db);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = so_listdestroy(&e->db_shutdown);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = sl_poolshutdown(&e->lp);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = sy_close(&e->rep, &e->r);
	if (ssunlikely(rc == -1))
		rcret = -1;
	sx_managerfree(&e->xm);
	si_cachepool_free(&e->cachepool, &e->r);
	se_metafree(&e->meta);
	ss_quotafree(&e->quota);
	ss_mutexfree(&e->apilock);
	ss_mutexfree(&e->reqlock);
	ss_condfree(&e->reqcond);
	ss_spinlockfree(&e->dblock);
	sr_seqfree(&e->seq);
	ss_pagerfree(&e->pager);
	ss_pagerfree(&e->pagersx);
	se_statusfree(&e->status);
	se_mark_destroyed(&e->o);
	free(e);
	return rcret;
}

static void*
se_begin(so *o)
{
	se *e = se_of(o);
	return se_txnew(e);
}

static void*
se_poll(so *o)
{
	se *e = se_cast(o, se*, SE);
	so *result;
	if (e->meta.event_on_backup) {
		ss_mutexlock(&e->sched.lock);
		if (ssunlikely(e->sched.backup_events > 0)) {
			e->sched.backup_events--;
			sereq r;
			se_reqinit(e, &r, SE_REQON_BACKUP, &e->o, NULL);
			result = se_reqresult(&r, 1);
			ss_mutexunlock(&e->sched.lock);
			return result;
		}
		ss_mutexunlock(&e->sched.lock);
	}
	sereq *req = se_reqdispatch_ready(e);
	if (req == NULL)
		return NULL;
	result = se_reqresult(req, 1);
	so_destroy(&req->o);
	return result;
}

static int
se_error(so *o)
{
	se *e = se_cast(o, se*, SE);
	int status = sr_errorof(&e->error);
	if (status == SR_ERROR_MALFUNCTION)
		return 1;
	status = se_status(&e->status);
	if (status == SE_MALFUNCTION)
		return 1;
	return 0;
}

static void*
se_cursor(so *o)
{
	se *e = se_cast(o, se*, SE);
	return se_cursornew(e, 0);
}

static soif seif =
{
	.open         = se_open,
	.destroy      = se_destroy,
	.error        = se_error,
	.object       = NULL,
	.asynchronous = NULL,
	.poll         = se_poll,
	.drop         = NULL,
	.setobject    = se_metaset_object,
	.setstring    = se_metaset_string,
	.setint       = se_metaset_int,
	.getobject    = se_metaget_object,
	.getstring    = se_metaget_string,
	.getint       = se_metaget_int,
	.set          = NULL,
	.update       = NULL,
	.del          = NULL,
	.get          = NULL,
	.batch        = NULL,
	.begin        = se_begin,
	.prepare      = NULL,
	.commit       = NULL,
	.cursor       = se_cursor,
};

so *se_new(void)
{
	se *e = malloc(sizeof(*e));
	if (ssunlikely(e == NULL))
		return NULL;
	memset(e, 0, sizeof(*e));
	so_init(&e->o, &se_o[SE], &seif, &e->o, &e->o /* self */);
	se_statusinit(&e->status);
	se_statusset(&e->status, SE_OFFLINE);
	ss_pagerinit(&e->pager, 10, 4096);
	int rc = ss_pageradd(&e->pager);
	if (ssunlikely(rc == -1)) {
		free(e);
		return NULL;
	}
	ss_pagerinit(&e->pagersx, 10, 4096);
	rc = ss_pageradd(&e->pagersx);
	if (ssunlikely(rc == -1)) {
		ss_pagerfree(&e->pager);
		free(e);
		return NULL;
	}
	ss_aopen(&e->a, &ss_stda);
	ss_aopen(&e->a_db, &ss_slaba, &e->pager, sizeof(sedb));
	ss_aopen(&e->a_v, &ss_slaba, &e->pager, sizeof(sev));
	ss_aopen(&e->a_cursor, &ss_slaba, &e->pager, sizeof(secursor));
	ss_aopen(&e->a_cachebranch, &ss_slaba, &e->pager, sizeof(sicachebranch));
	ss_aopen(&e->a_cache, &ss_slaba, &e->pager, sizeof(sicache));
	ss_aopen(&e->a_metacursor, &ss_slaba, &e->pager, sizeof(semetacursor));
	ss_aopen(&e->a_metav, &ss_slaba, &e->pager, sizeof(semetav));
	ss_aopen(&e->a_snapshot, &ss_slaba, &e->pager, sizeof(sesnapshot));
	ss_aopen(&e->a_snapshotcursor, &ss_slaba, &e->pager, sizeof(sesnapshotcursor));
	ss_aopen(&e->a_batch, &ss_slaba, &e->pager, sizeof(sebatch));
	ss_aopen(&e->a_tx, &ss_slaba, &e->pager, sizeof(setx));
	ss_aopen(&e->a_req, &ss_slaba, &e->pager, sizeof(sereq));
	ss_aopen(&e->a_sxv, &ss_slaba, &e->pagersx, sizeof(sxv));
	se_metainit(&e->meta, &e->o);
	so_listinit(&e->db);
	so_listinit(&e->db_shutdown);
	so_listinit(&e->cursor);
	so_listinit(&e->tx);
	so_listinit(&e->snapshot);
	so_listinit(&e->metacursor);
	so_listinit(&e->req);
	so_listinit(&e->reqready);
	so_listinit(&e->reqactive);
	ss_mutexinit(&e->apilock);
	ss_mutexinit(&e->reqlock);
	ss_condinit(&e->reqcond);
	ss_spinlockinit(&e->dblock);
	ss_quotainit(&e->quota);
	sr_seqinit(&e->seq);
	sr_errorinit(&e->error);
	sscrcf crc = ss_crc32c_function();
	sr_init(&e->r, &e->error, &e->a, &e->quota, &e->seq,
	        SF_KV, SF_SRAW, NULL,
	        &e->meta.scheme, &e->ei, crc, NULL);
	sy_init(&e->rep);
	sl_poolinit(&e->lp, &e->r);
	sx_managerinit(&e->xm, &e->r, &e->a_sxv);
	si_cachepool_init(&e->cachepool, &e->a_cache, &e->a_cachebranch);
	se_scheduler_init(&e->sched, &e->o);
	return &e->o;
}
