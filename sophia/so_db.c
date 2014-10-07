
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
		if (srunlikely(rc == -1))
			break;
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
		if (srunlikely(rc == -1))
			break;
		int nojob = rc == 0;
		rc = sl_poolgc(&o->lp);
		if (srunlikely(rc == -1))
			break;
		rc = sl_poolrotate_ready(&o->lp, o->ctl.logdir_rotate_wm);
		if (rc) {
			rc = sl_poolrotate(&o->lp);
			if (srunlikely(rc == -1))
				break;
		}
		if (nojob)
			so_sleep();
	}
	return NULL;
}

static int
so_dbopen(soobj *obj, va_list args srunused)
{
	sodb *o = (sodb*)obj;
	if (so_dbactive(o))
		return -1;
	int rc;
	rc = so_dbctl_validate(&o->ctl);
	if (srunlikely(rc == -1))
		return -1;
	o->r.cmp = &o->ctl.cmp;
	rc = so_recover(o);
	if (srunlikely(rc == -1))
		return -1;
	o->mode = SO_ONLINE;
	int threads = o->ctl.threads;
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
so_dbdestroy(soobj *obj)
{
	sodb *o = (sodb*)obj;
	so_objindex_unregister(&o->e->db, &o->o);
	o->mode = SO_SHUTDOWN;
	int rcret = 0;
	int rc;
	rc = so_workersshutdown(&o->workers, &o->r);
	if (srunlikely(rc == -1))
		rcret = -1;
	so_objindex_free(&o->tx);
	so_objindex_free(&o->cursor);
	sm_free(&o->mvcc);
	rc = sl_poolshutdown(&o->lp);
	if (srunlikely(rc == -1))
		rcret = -1;
	rc = si_close(&o->index, &o->r);
	if (srunlikely(rc == -1))
		rcret = -1;
	so_dbctl_free(&o->ctl);
	sd_cfree(&o->dc, &o->r);
	sr_free(&o->e->a, o);
	return rcret;
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
	.set      = so_dbset,
	.get      = so_dbget,
	.del      = so_dbdel,
	.begin    = so_dbbegin,
	.commit   = NULL,
	.rollback = NULL,
	.cursor   = so_dbcursor,
	.object   = so_dbobj,
	.type     = so_dbtype,
	.copy     = NULL
};

soobj *so_dbnew(so *e, char *name)
{
	sodb *o = sr_malloc(&e->a, sizeof(sodb));
	if (srunlikely(o == NULL))
		return NULL;
	memset(o, 0, sizeof(*o));
	so_objinit(&o->o, SODB, &sodbif);
	so_objindex_init(&o->tx);
	so_objindex_init(&o->cursor);
	o->mode = SO_OFFLINE;
	o->e = e;
	o->r = e->r;
	o->r.cmp = &o->ctl.cmp;
	int rc = so_dbctl_init(&o->ctl, name, o);
	if (srunlikely(rc == -1)) {
		sr_free(&e->a, o);
		return NULL;
	}
	sm_init(&o->mvcc, &o->r);
	sd_cinit(&o->dc, &o->r);
	so_workersinit(&o->workers);
	return &o->o;
}

soobj *so_dbmatch(so *e, char *name)
{
	srlist *i;
	sr_listforeach(&e->db.list, i) {
		soobj *o = srcast(i, soobj, olink);
		sodb *db = (sodb*)o;
		if (strcmp(db->ctl.name, name) == 0)
			return o;
	}
	return NULL;
}
