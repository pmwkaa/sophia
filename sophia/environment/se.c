
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
#include <libsc.h>
#include <libse.h>

static void*
se_worker(void *arg)
{
	ssthread *self = arg;
	se *e = self->arg;
	scworker *w = sc_workerpool_pop(&e->scheduler.wp, &e->r);
	if (ssunlikely(w == NULL))
		return NULL;
	for (;;)
	{
		int rc = se_active(e);
		if (ssunlikely(rc == 0))
			break;
		rc = sc_step(&e->scheduler, w, sx_vlsn(&e->xm));
		if (ssunlikely(rc == -1))
			break;
		if (ssunlikely(rc == 0))
			ss_sleep(10000000); /* 10ms */
	}
	sc_workerpool_push(&e->scheduler.wp, w);
	return NULL;
}

int se_service_threads(se *e, int n)
{
	/* run more threads */
	return sc_create(&e->scheduler, se_worker, e, n);
}

static int
se_open(so *o)
{
	se *e = se_cast(o, se*, SE);
	/* recover phases */
	int status = sr_status(&e->status);
	if (status != SR_OFFLINE)
		return -1;

	sr_log(&e->log, "sophia %d.%d git: %s",
	       SR_VERSION_A - '0',
	       SR_VERSION_B - '0',
	       SR_VERSION_COMMIT);

	/* validate configuration */
	int rc;
	rc = se_confvalidate(&e->conf);
	if (ssunlikely(rc == -1))
		return -1;

	/* switch to recover phase */
	sr_statusset(&e->status, SR_RECOVER);

	/* set memory quota (disable during recovery) */
	sr_quotaset(&e->quota, e->conf.memory_limit);
	sr_quotaenable(&e->quota, 0);

	/* repository recover */
	rc = se_recover_repository(e);
	if (ssunlikely(rc == -1))
		return -1;

	/* databases recover */
	sslist *i, *n;
	ss_listforeach_safe(&e->db.list, i, n) {
		so *o = sscast(i, so, link);
		rc = se_dbopen(o);
		if (ssunlikely(rc == -1))
			return -1;
	}

	/* recover logpool */
	rc = se_recover(e);
	if (ssunlikely(rc == -1))
		return -1;

	/* put storage on-line */
	ss_listforeach_safe(&e->db.list, i, n) {
		so *o = sscast(i, so, link);
		rc = se_dbopen(o);
		if (ssunlikely(rc == -1))
			return -1;
	}

	/* enable quota */
	sr_quotaenable(&e->quota, 1);
	sr_statusset(&e->status, SR_ONLINE);

	/* run thread-pool and scheduler */
	sc_set(&e->scheduler, e->conf.anticache,
	        e->conf.backup_path);
	rc = se_service_threads(e, e->conf.threads);
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
	sr_statusset(&e->status, SR_SHUTDOWN);
	rc = sc_shutdown(&e->scheduler);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = so_pooldestroy(&e->cursor);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = so_pooldestroy(&e->tx);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = so_pooldestroy(&e->confcursor_kv);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = so_pooldestroy(&e->confcursor);
	if (ssunlikely(rc == -1))
		rcret = -1;
	sslist *i, *n;
	ss_listforeach_safe(&e->db.list, i, n) {
		so *db = sscast(i, so, link);
		rc = se_dbdestroy(db);
		if (ssunlikely(rc == -1))
			rcret = -1;
	}
	rc = so_pooldestroy(&e->document);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = sl_poolshutdown(&e->lp);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = sy_close(&e->rep, &e->r);
	if (ssunlikely(rc == -1))
		rcret = -1;
	sx_managerfree(&e->xm);
	ss_vfsfree(&e->vfs);
	si_cachepool_free(&e->cachepool);
	se_conffree(&e->conf);
	ss_mutexfree(&e->apilock);
	sf_limitfree(&e->limit, &e->a);
	sr_statfree(&e->stat);
	sr_seqfree(&e->seq);
	sr_statusfree(&e->status);
	so_mark_destroyed(&e->o);
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
	if (e->conf.event_on_backup) {
		int event = sc_ctl_backup_event(&e->scheduler);
		if (event) {
			sedocument *doc;
			result = se_document_new(e, &e->o, NULL);
			if (ssunlikely(result == NULL))
				return NULL;
			doc = (sedocument*)result;
			doc->event = 1;
			return result;
		}
	}
	return NULL;
}

static int
se_error(so *o)
{
	se *e = se_cast(o, se*, SE);
	int status = sr_errorof(&e->error);
	if (status == SR_ERROR_MALFUNCTION)
		return 1;
	status = sr_status(&e->status);
	if (status == SR_MALFUNCTION)
		return 1;
	return 0;
}

static void*
se_cursor(so *o)
{
	se *e = se_cast(o, se*, SE);
	return se_cursornew(e, UINT64_MAX);
}

static soif seif =
{
	.open         = se_open,
	.destroy      = se_destroy,
	.free         = NULL,
	.error        = se_error,
	.document     = NULL,
	.poll         = se_poll,
	.setstring    = se_confset_string,
	.setint       = se_confset_int,
	.setobject    = NULL,
	.getobject    = se_confget_object,
	.getstring    = se_confget_string,
	.getint       = se_confget_int,
	.set          = NULL,
	.upsert       = NULL,
	.del          = NULL,
	.get          = NULL,
	.begin        = se_begin,
	.prepare      = NULL,
	.commit       = NULL,
	.cursor       = se_cursor,
};

int se_service(so *o)
{
	se *e = se_cast(o, se*, SE);
	return sc_ctl_call(&e->scheduler, sx_vlsn(&e->xm));
}

so *se_new(void)
{
	se *e = malloc(sizeof(*e));
	if (ssunlikely(e == NULL))
		return NULL;
	memset(e, 0, sizeof(*e));
	so_init(&e->o, &se_o[SE], &seif, &e->o, &e->o /* self */);
	sr_statusinit(&e->status);
	sr_statusset(&e->status, SR_OFFLINE);
	ss_vfsinit(&e->vfs, &ss_stdvfs);
	ss_aopen(&e->a, &ss_stda);
	ss_aopen(&e->a_ref, &ss_stda);
	int rc;
	rc = se_confinit(&e->conf, &e->o);
	if (ssunlikely(rc == -1))
		goto error;
	so_poolinit(&e->document, 1024);
	so_poolinit(&e->cursor, 512);
	so_poolinit(&e->tx, 512);
	so_poolinit(&e->confcursor, 2);
	so_poolinit(&e->confcursor_kv, 1);
	so_listinit(&e->db);
	ss_mutexinit(&e->apilock);
	sr_quotainit(&e->quota);
	sr_seqinit(&e->seq);
	sr_loginit(&e->log);
	sr_errorinit(&e->error, &e->log);
	sr_statinit(&e->stat);
	sf_limitinit(&e->limit, &e->a);
	sscrcf crc = ss_crc32c_function();
	sr_init(&e->r, &e->status, &e->log, &e->error, &e->a, &e->a_ref, &e->vfs, &e->quota,
	        &e->conf.zones, &e->seq, SF_RAW, NULL,
	        NULL, &e->ei, &e->stat, crc);
	sy_init(&e->rep);
	sl_poolinit(&e->lp, &e->r);
	sx_managerinit(&e->xm, &e->r);
	si_cachepool_init(&e->cachepool, &e->r);
	sc_init(&e->scheduler, &e->r, &e->conf.on_event, &e->lp);
	return &e->o;
error:
	sr_statusfree(&e->status);
	free(e);
	return NULL;
}
