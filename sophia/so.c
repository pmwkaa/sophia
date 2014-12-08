
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
#include <libse.h>
#include <libso.h>

static void*
so_ctl(soobj *obj, va_list args srunused)
{
	so *o = (so*)obj;
	return &o->ctl;
}

static int
so_open(soobj *o, va_list args)
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
	if (srunlikely(rc == -1))
		return -1;
	so_statusset(&e->status, SO_RECOVER);

	/* set memory quota (disable during recovery) */
	sr_quotaset(&e->quota, e->ctl.memory_limit);
	sr_quotaenable(&e->quota, 0);

	/* repository recover */
	rc = so_recover_repository(e);
	if (srunlikely(rc == -1))
		return -1;

	/* databases recover */
	srlist *i, *n;
	sr_listforeach_safe(&e->db.list, i, n) {
		soobj *o = srcast(i, soobj, link);
		rc = o->i->open(o, args);
		if (srunlikely(rc == -1))
			return -1;
	}

	/* recover logpool */
	rc = so_recover(e);
	if (srunlikely(rc == -1))
		return -1;
	if (e->ctl.two_phase_recover)
		return 0;

online:
	/* complete */
	sr_listforeach_safe(&e->db.list, i, n) {
		soobj *o = srcast(i, soobj, link);
		rc = o->i->open(o, args);
		if (srunlikely(rc == -1))
			return -1;
	}
	/* enable quota */
	sr_quotaenable(&e->quota, 1);
	so_statusset(&e->status, SO_ONLINE);
	/* run thread-pool and scheduler */
	rc = so_scheduler_run(&e->sched);
	if (srunlikely(rc == -1))
		return -1;
	return 0;
}

static int
so_destroy(soobj *o)
{
	so *e = (so*)o;
	int rcret = 0;
	int rc;
	so_statusset(&e->status, SO_SHUTDOWN);
	rc = so_scheduler_shutdown(&e->sched);
	if (srunlikely(rc == -1))
		rcret = -1;
	rc = so_objindex_destroy(&e->ctlcursor);
	if (srunlikely(rc == -1))
		rcret = -1;
	rc = so_objindex_destroy(&e->db);
	if (srunlikely(rc == -1))
		rcret = -1;
	rc = sl_poolshutdown(&e->lp);
	if (srunlikely(rc == -1))
		rcret = -1;
	rc = se_close(&e->se, &e->r);
	if (srunlikely(rc == -1))
		rcret = -1;
	so_ctlfree(&e->ctl);
	sr_quotafree(&e->quota);
	sr_mutexfree(&e->apilock);
	sr_seqfree(&e->seq);
	sr_pagerfree(&e->pager);
	so_statusfree(&e->status);
	free(e);
	return rcret;
}

static void*
so_type(soobj *o srunused, va_list args srunused) {
	return "env";
}

static soobjif soif =
{
	.ctl      = so_ctl,
	.open     = so_open,
	.destroy  = so_destroy,
	.error    = NULL,
	.set      = NULL,
	.get      = NULL,
	.del      = NULL,
	.begin    = NULL,
	.prepare  = NULL,
	.commit   = NULL,
	.rollback = NULL,
	.cursor   = NULL,
	.object   = NULL,
	.type     = so_type
};

soobj *so_new(void)
{
	so *e = malloc(sizeof(*e));
	if (srunlikely(e == NULL))
		return NULL;
	memset(e, 0, sizeof(*e));
	so_objinit(&e->o, SOENV, &soif, &e->o /* self */);
	sr_pagerinit(&e->pager, 10, 1024);
	int rc = sr_pageradd(&e->pager);
	if (srunlikely(rc == -1)) {
		free(e);
		return NULL;
	}
	sr_allocopen(&e->a, &sr_astd);
	sr_allocopen(&e->a_db, &sr_aslab, &e->pager, sizeof(sodb));
	sr_allocopen(&e->a_v, &sr_aslab, &e->pager, sizeof(sov));
	sr_allocopen(&e->a_cursor, &sr_aslab, &e->pager, sizeof(socursor));
	sr_allocopen(&e->a_ctlcursor, &sr_aslab, &e->pager, sizeof(soctlcursor));
	sr_allocopen(&e->a_logcursor, &sr_aslab, &e->pager, sizeof(sologcursor));
	sr_allocopen(&e->a_tx, &sr_aslab, &e->pager, sizeof(sotx));
	sr_allocopen(&e->a_smv, &sr_aslab, &e->pager, sizeof(smv));
	so_statusinit(&e->status);
	so_statusset(&e->status, SO_OFFLINE);
	so_ctlinit(&e->ctl, e);
	so_objindex_init(&e->db);
	so_objindex_init(&e->ctlcursor);
	sr_mutexinit(&e->apilock);
	sr_quotainit(&e->quota);
	sr_seqinit(&e->seq);
	sr_errorinit(&e->error);
	sr_init(&e->r, &e->error, &e->a, &e->seq, NULL, NULL);
	se_init(&e->se);
	sl_poolinit(&e->lp, &e->r);
	so_scheduler_init(&e->sched, e);
	return &e->o;
}
