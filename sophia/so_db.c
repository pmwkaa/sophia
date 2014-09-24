
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

static int
so_dbctlset(soobj *obj, va_list args)
{
	sodbctl *c = (sodbctl*)obj;
	sodb *db = c->parent;
	char *name = va_arg(args, char*);
	int rc = 0;
	sdc dc;
	sd_cinit(&dc, &c->parent->r);
	if (strcmp(name, "merge") == 0) {
		while (1) {
			rc = si_merge(&db->index, &db->r, &dc, db->conf.node_merge_wm);
			if (srunlikely(rc <= 0))
				break;
		}
	} else
	if (strcmp(name, "merge_step") == 0) {
		rc = si_merge(&db->index, &db->r, &dc, db->conf.node_merge_wm);
	} else
	if (strcmp(name, "branch") == 0) {
		while (1) {
			rc = si_branch(&db->index, &db->r, &dc, db->conf.node_branch_wm);
			if (srunlikely(rc <= 0))
				break;
		}
	} else
	if (strcmp(name, "branch_step") == 0) {
		rc = si_branch(&db->index, &db->r, &dc, db->conf.node_branch_wm);
	} else
	if (strcmp(name, "logrotate") == 0) {
		rc = sl_poolrotate(&db->lp);
	}
	sd_cfree(&dc, &c->parent->r);
	return rc;
}

static void*
so_dbtype(soobj *o srunused, va_list args srunused) {
	return "database";
}

static soobjif sodbctlif =
{
	.ctl       = NULL,
	.storage   = NULL,
	.open      = NULL,
	.destroy   = NULL,
	.set       = so_dbctlset,
	.get       = NULL,
	.del       = NULL,
	.begin     = NULL,
	.commit    = NULL,
	.rollback  = NULL,
	.cursor    = NULL,
	.backup    = NULL,
	.object    = NULL,
	.type      = so_dbtype,
	.copy      = NULL
};

static void*
so_dbctl(soobj *obj, va_list args)
{
	sodb *o = (sodb*)obj;
	char *name = va_arg(args, char*);
	if (strcmp(name, "ctl") == 0)
		return &o->ctl.o;
	if (strcmp(name, "conf") == 0)
		return &o->conf.o;
	return NULL;
}

static void*
so_dbobj(soobj *obj, va_list args srunused)
{
	sodb *o = (sodb*)obj;
	return so_vnew(o->e);
}

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
		rc = si_merge(&o->index,
		              &o->r, &self->dc,
		              o->conf.node_merge_wm);
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
		rc = si_branch(&o->index,
		               &o->r, &self->dc,
		               o->conf.node_branch_wm);
		if (srunlikely(rc == -1))
			break;
		int nojob = rc == 0;
		rc = sl_poolgc(&o->lp);
		if (srunlikely(rc == -1))
			break;
		rc = sl_poolrotate_ready(&o->lp, o->conf.logdir_rotate_wm);
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
	rc = so_dbconf_validate(&o->conf);
	if (srunlikely(rc == -1))
		return -1;
	o->r.cmp = &o->conf.cmp;
	rc = so_recover(o);
	if (srunlikely(rc == -1))
		return -1;
	o->mode = SO_ONLINE;
	int threads = o->conf.threads;
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
	o->conf.o.oif->destroy(&o->conf.o);
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

static soobjif sodbif =
{
	.ctl       = so_dbctl,
	.storage   = NULL,
	.open      = so_dbopen,
	.destroy   = so_dbdestroy,
	.set       = so_dbset,
	.get       = so_dbget,
	.del       = so_dbdel,
	.begin     = so_dbbegin,
	.commit    = NULL,
	.rollback  = NULL,
	.cursor    = so_dbcursor,
	.backup    = NULL,
	.object    = so_dbobj,
	.type      = NULL,
	.copy      = NULL
};

soobj *so_dbnew(so *e)
{
	sodb *o = sr_malloc(&e->a, sizeof(sodb));
	if (srunlikely(o == NULL))
		return NULL;
	memset(o, 0, sizeof(*o));
	so_objinit(&o->o, SODB, &sodbif);
	so_objinit(&o->ctl.o, SODBCTL, &sodbctlif);
	o->ctl.parent = o;
	so_dbconf_init(&o->conf, o);
	so_objindex_init(&o->tx);
	so_objindex_init(&o->cursor);
	o->mode = SO_OFFLINE;
	o->e = e;
	o->r = e->r;
	o->r.cmp = &o->conf.cmp;
	sm_init(&o->mvcc, &o->r);
	sd_cinit(&o->dc, &o->r);
	so_workersinit(&o->workers);
	return &o->o;
}
