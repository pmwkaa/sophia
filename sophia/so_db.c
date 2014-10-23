
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

static inline void so_sleep(void)
{
	struct timespec ts;
	ts.tv_sec  = 0;
	ts.tv_nsec = 10000000; /* 10 ms */
	nanosleep(&ts, NULL);
}

static inline void *so_merger(void *arg) 
{
	soworker *self = arg;
	sodb *o = self->arg;
	for (;;)
	{
		int rc = so_active(o->e) && so_dbactive(o);
		if (srunlikely(rc == 0))
			break;
		uint64_t lsvn = sm_lsvn(&o->mvcc);
		rc = si_merge(&o->index,
		              &o->r, &self->dc,
		              lsvn,
		              o->ctl.node_merge_wm);
		if (srunlikely(rc == -1)) {
			so_dbmalfunction(o);
			break;
		}
		if (rc == 0)
			so_sleep();
	}
	return NULL;
}

static inline void *so_brancher(void *arg) 
{
	soworker *self = arg;
	sodb *o = self->arg;
	for (;;)
	{
		int rc = so_active(o->e) && so_dbactive(o);
		if (srunlikely(rc == 0))
			break;
		uint64_t lsvn = sm_lsvn(&o->mvcc);
		rc = si_branch(&o->index,
		               &o->r, &self->dc,
		               lsvn,
		               o->ctl.node_branch_wm);
		if (srunlikely(rc == -1)) {
			so_dbmalfunction(o);
			break;
		}
		int nojob = rc == 0;
		rc = sl_poolgc(&o->lp);
		if (srunlikely(rc == -1)) {
			so_dbmalfunction(o);
			break;
		}
		rc = sl_poolrotate_ready(&o->lp, o->ctl.log_rotate_wm);
		if (rc) {
			rc = sl_poolrotate(&o->lp);
			if (srunlikely(rc == -1)) {
				so_dbmalfunction(o);
				break;
			}
		}
		if (nojob)
			so_sleep();
	}
	return NULL;
}

static int
so_dbonline(soobj *obj)
{
	sodb *o = (sodb*)obj;
	si_qosenable(&o->index, 1);
	so_statusset(&o->status, SO_ONLINE);
	int threads = o->ctl.threads;
	int rc;
	if (threads) {
		rc = so_workersnew(&o->workers, &o->r, 1, so_brancher, o);
		if (srunlikely(rc == -1))
			return -1;
		threads--;
	}
	if (threads) {
		rc = so_workersnew(&o->workers, &o->r, threads, so_merger, o);
		if (srunlikely(rc == -1))
			return -1;
	}
	return 0;
}

static int
so_dbopen(soobj *obj, va_list args srunused)
{
	sodb *o = (sodb*)obj;
	int status = so_status(&o->status);
	if (status == SO_RECOVER) {
		assert(o->ctl.two_phase_recover == 1);
		return so_dbonline(obj);
	}
	if (status != SO_OFFLINE)
		return -1;
	int rc;
	rc = so_dbctl_validate(&o->ctl);
	if (srunlikely(rc == -1))
		return -1;
	o->r.cmp = &o->ctl.cmp;
	rc = so_recover(o);
	if (srunlikely(rc == -1))
		return -1;
	if (o->ctl.two_phase_recover)
		return 0;
	return so_dbonline(obj);
}

static int
so_dbdestroy(soobj *obj)
{
	sodb *o = (sodb*)obj;
	so_statusset(&o->status, SO_SHUTDOWN);
	int rcret = 0;
	int rc;
	rc = so_workersshutdown(&o->workers, &o->r);
	if (srunlikely(rc == -1))
		rcret = -1;
	rc = so_objindex_destroy(&o->tx);
	if (srunlikely(rc == -1))
		rcret = -1;
	rc = so_objindex_destroy(&o->cursor);
	if (srunlikely(rc == -1))
		rcret = -1;
	sm_free(&o->mvcc);
	rc = sl_poolshutdown(&o->lp);
	if (srunlikely(rc == -1))
		rcret = -1;
	rc = si_close(&o->index, &o->r);
	if (srunlikely(rc == -1))
		rcret = -1;
	so_dbctl_free(&o->ctl);
	sd_cfree(&o->dc, &o->r);
	so_statusfree(&o->status);
	so_objindex_unregister(&o->e->db, &o->o);
	sr_free(&o->e->a_db, o);
	return rcret;
}

static int
so_dberror(soobj *obj, va_list args srunused)
{
	sodb *o = (sodb*)obj;
	int status = sr_erroris(&o->e->error);
	int recoverable = sr_erroris_recoverable(&o->e->error);
	if (srunlikely(status && recoverable))
		return 2;
	if (srunlikely(status))
		return 1;
	return 0;
}

static int
so_dbset(soobj *obj, va_list args)
{
	sodb *o = (sodb*)obj;
	return so_txdbset(o, SVSET, args);
}

static void*
so_dbget(soobj *obj, va_list args)
{
	sodb *o = (sodb*)obj;
	return so_txdbget(o, args);
}

static int
so_dbdel(soobj *obj, va_list args)
{
	sodb *o = (sodb*)obj;
	return so_txdbset(o, SVDELETE, args);
}

static void*
so_dbbegin(soobj *o)
{
	sodb *db = (sodb*)o;
	return so_txnew(db);
}

static void*
so_dbcursor(soobj *o, va_list args)
{
	sodb *db = (sodb*)o;
	return so_cursornew(db, args);
}

static void*
so_dbobj(soobj *obj, va_list args srunused)
{
	sodb *o = (sodb*)obj;
	return so_vnew(o->e);
}

static void*
so_dbtype(soobj *o srunused, va_list args srunused) {
	return "database";
}

static soobjif sodbif =
{
	.ctl      = NULL,
	.open     = so_dbopen,
	.destroy  = so_dbdestroy,
	.error    = so_dberror,
	.set      = so_dbset,
	.get      = so_dbget,
	.del      = so_dbdel,
	.begin    = so_dbbegin,
	.prepare  = NULL,
	.commit   = NULL,
	.rollback = NULL,
	.cursor   = so_dbcursor,
	.object   = so_dbobj,
	.type     = so_dbtype
};

soobj *so_dbnew(so *e, char *name)
{
	sodb *o = sr_malloc(&e->a_db, sizeof(sodb));
	if (srunlikely(o == NULL)) {
		sr_error(&e->error, "%s", "memory allocation failed");
		sr_error_recoverable(&e->error);
		return NULL;
	}
	memset(o, 0, sizeof(*o));
	so_objinit(&o->o, SODB, &sodbif, &e->o);
	so_objindex_init(&o->tx);
	so_objindex_init(&o->cursor);
	so_statusinit(&o->status);
	so_statusset(&o->status, SO_OFFLINE);
	o->e     = e;
	o->r     = e->r;
	o->r.cmp = &o->ctl.cmp;
	o->r.i   = &o->ei;
	int rc = so_dbctl_init(&o->ctl, name, o);
	if (srunlikely(rc == -1)) {
		sr_free(&e->a_db, o);
		return NULL;
	}
	sm_init(&o->mvcc, &o->r, &e->a_smv);
	sd_cinit(&o->dc, &o->r);
	so_workersinit(&o->workers);
	return &o->o;
}

soobj *so_dbmatch(so *e, char *name)
{
	srlist *i;
	sr_listforeach(&e->db.list, i) {
		soobj *o = srcast(i, soobj, link);
		sodb *db = (sodb*)o;
		if (strcmp(db->ctl.name, name) == 0)
			return o;
	}
	return NULL;
}

int so_dbmalfunction(sodb *o)
{
	so_statusset(&o->status, SO_MALFUNCTION);
	return -1;
}
