
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

static void*
so_asyncbegin(srobj *o, va_list args ssunused) {
	return so_txnew(so_of(o), 1);
}

static void*
so_asynctype(srobj *o ssunused, va_list args ssunused) {
	return "env_async";
}

static srobjif soasyncif =
{
	.ctl     = NULL,
	.async   = NULL,
	.open    = NULL,
	.destroy = NULL,
	.error   = NULL,
	.set     = NULL,
	.del     = NULL,
	.get     = NULL,
	.poll    = NULL,
	.drop    = NULL,
	.begin   = so_asyncbegin,
	.prepare = NULL,
	.commit  = NULL,
	.cursor  = NULL,
	.object  = NULL,
	.type    = so_asynctype
};

static void*
so_ctl(srobj *obj, va_list args ssunused)
{
	so *o = (so*)obj;
	return &o->ctl;
}

static void*
so_async(srobj *obj, va_list args ssunused)
{
	so *o = (so*)obj;
	return &o->async;
}

static int
so_open(srobj *o, va_list args)
{
	so *e = (so*)o;
	int status = so_status(&e->status);
	if (status == SO_RECOVER) {
		assert(e->ctl.two_phase_recover == 1);
		goto online;
	}
	if (status != SO_OFFLINE)
		return -1;
	int rc;
	rc = so_ctlvalidate(&e->ctl);
	if (ssunlikely(rc == -1))
		return -1;
	so_statusset(&e->status, SO_RECOVER);

	/* set memory quota (disable during recovery) */
	ss_quotaset(&e->quota, e->ctl.memory_limit);
	ss_quotaenable(&e->quota, 0);

	/* repository recover */
	rc = so_recover_repository(e);
	if (ssunlikely(rc == -1))
		return -1;
	/* databases recover */
	sslist *i, *n;
	ss_listforeach_safe(&e->db.list, i, n) {
		srobj *o = sscast(i, srobj, link);
		rc = o->i->open(o, args);
		if (ssunlikely(rc == -1))
			return -1;
	}
	/* recover logpool */
	rc = so_recover(e);
	if (ssunlikely(rc == -1))
		return -1;
	if (e->ctl.two_phase_recover)
		return 0;

online:
	/* complete */
	ss_listforeach_safe(&e->db.list, i, n) {
		srobj *o = sscast(i, srobj, link);
		rc = o->i->open(o, args);
		if (ssunlikely(rc == -1))
			return -1;
	}
	/* enable quota */
	ss_quotaenable(&e->quota, 1);
	so_statusset(&e->status, SO_ONLINE);
	/* run thread-pool and scheduler */
	rc = so_scheduler_run(&e->sched);
	if (ssunlikely(rc == -1))
		return -1;
	return 0;
}

static int
so_destroy(srobj *o, va_list args ssunused)
{
	so *e = (so*)o;
	int rcret = 0;
	int rc;
	so_statusset(&e->status, SO_SHUTDOWN);
	rc = so_scheduler_shutdown(&e->sched);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = sr_objlist_destroy(&e->req);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = sr_objlist_destroy(&e->reqactive);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = sr_objlist_destroy(&e->reqready);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = sr_objlist_destroy(&e->tx);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = sr_objlist_destroy(&e->snapshot);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = sr_objlist_destroy(&e->ctlcursor);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = sr_objlist_destroy(&e->db);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = sr_objlist_destroy(&e->db_shutdown);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = sl_poolshutdown(&e->lp);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = se_close(&e->se, &e->r);
	if (ssunlikely(rc == -1))
		rcret = -1;
	sx_managerfree(&e->xm);
	si_cachepool_free(&e->cachepool, &e->r);
	so_ctlfree(&e->ctl);
	ss_quotafree(&e->quota);
	ss_mutexfree(&e->apilock);
	ss_mutexfree(&e->reqlock);
	ss_condfree(&e->reqcond);
	ss_spinlockfree(&e->dblock);
	sr_seqfree(&e->seq);
	ss_pagerfree(&e->pager);
	ss_pagerfree(&e->pagersx);
	so_statusfree(&e->status);
	free(e);
	return rcret;
}

static void*
so_begin(srobj *o, va_list args ssunused) {
	return so_txnew((so*)o, 0);
}

static void*
so_poll(srobj *o, va_list args ssunused)
{
	so *e = (so*)o;
	if (e->ctl.event_on_backup) {
		ss_mutexlock(&e->sched.lock);
		if (ssunlikely(e->sched.backup_events > 0)) {
			e->sched.backup_events--;
			sorequest *req = so_requestnew(e, SO_REQON_BACKUP, &e->o, NULL);
			if (ssunlikely(req == NULL)) {
				ss_mutexunlock(&e->sched.lock);
				return NULL;
			}
			ss_mutexunlock(&e->sched.lock);
			return &req->o;
		}
		ss_mutexunlock(&e->sched.lock);
	}
	sorequest *req = so_requestdispatch_ready(e);
	if (req == NULL)
		return NULL;
	so_requestresult(req);
	return &req->o;
}

static int
so_error(srobj *o, va_list args ssunused)
{
	so *e = (so*)o;
	int status = sr_errorof(&e->error);
	if (status == SR_ERROR_MALFUNCTION)
		return 1;
	status = so_status(&e->status);
	if (status == SO_MALFUNCTION)
		return 1;
	return 0;
}

static void*
so_type(srobj *o ssunused, va_list args ssunused) {
	return "env";
}

static srobjif soif =
{
	.ctl     = so_ctl,
	.async   = so_async,
	.open    = so_open,
	.destroy = so_destroy,
	.error   = so_error,
	.set     = NULL,
	.del     = NULL,
	.get     = NULL,
	.poll    = so_poll,
	.drop    = NULL,
	.begin   = so_begin,
	.prepare = NULL,
	.commit  = NULL,
	.cursor  = NULL,
	.object  = NULL,
	.type    = so_type
};

srobj *so_new(void)
{
	so *e = malloc(sizeof(*e));
	if (ssunlikely(e == NULL))
		return NULL;
	memset(e, 0, sizeof(*e));
	sr_objinit(&e->o, SOENV, &soif, &e->o /* self */);
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
	ss_aopen(&e->a_db, &ss_slaba, &e->pager, sizeof(sodb));
	ss_aopen(&e->a_v, &ss_slaba, &e->pager, sizeof(sov));
	ss_aopen(&e->a_cursor, &ss_slaba, &e->pager, sizeof(socursor));
	ss_aopen(&e->a_cachebranch, &ss_slaba, &e->pager, sizeof(sicachebranch));
	ss_aopen(&e->a_cache, &ss_slaba, &e->pager, sizeof(sicache));
	ss_aopen(&e->a_ctlcursor, &ss_slaba, &e->pager, sizeof(soctlcursor));
	ss_aopen(&e->a_snapshot, &ss_slaba, &e->pager, sizeof(sosnapshot));
	ss_aopen(&e->a_tx, &ss_slaba, &e->pager, sizeof(sotx));
	ss_aopen(&e->a_req, &ss_slaba, &e->pager, sizeof(sorequest));
	ss_aopen(&e->a_sxv, &ss_slaba, &e->pagersx, sizeof(sxv));
	so_statusinit(&e->status);
	so_statusset(&e->status, SO_OFFLINE);
	so_ctlinit(&e->ctl, e);
	sr_objinit(&e->async.o, SOENVASYNC, &soasyncif, &e->o);
	sr_objlist_init(&e->db);
	sr_objlist_init(&e->db_shutdown);
	sr_objlist_init(&e->tx);
	sr_objlist_init(&e->snapshot);
	sr_objlist_init(&e->ctlcursor);
	sr_objlist_init(&e->req);
	sr_objlist_init(&e->reqready);
	sr_objlist_init(&e->reqactive);
	ss_mutexinit(&e->apilock);
	ss_mutexinit(&e->reqlock);
	ss_condinit(&e->reqcond);
	ss_spinlockinit(&e->dblock);
	ss_quotainit(&e->quota);
	sr_seqinit(&e->seq);
	sr_errorinit(&e->error);
	sscrcf crc = ss_crc32c_function();
	sr_init(&e->r, &e->error, &e->a, &e->seq,
	        SF_KV, SF_SRAW,
	        &e->ctl.ctlscheme, &e->ei, crc, NULL);
	se_init(&e->se);
	sl_poolinit(&e->lp, &e->r);
	sx_managerinit(&e->xm, &e->r, &e->a_sxv);
	si_cachepool_init(&e->cachepool, &e->a_cache, &e->a_cachebranch);
	so_scheduler_init(&e->sched, e);
	return &e->o;
}
