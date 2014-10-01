
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

int so_dbctl_init(sodbctl *c, char *name, void *db)
{
	memset(c, 0, sizeof(*c));
	sodb *o = db;
	c->name = sr_strdup(&o->e->a, name);
	if (srunlikely(c->name == NULL))
		return -1;
	c->parent           = db;
	c->logdir_create    = 1;
	c->logdir_write     = 1;
	c->logdir_rotate_wm = 500000;
	c->dir_create       = 1;
	c->dir_write        = 1;
	c->cmp.cmp          = sr_cmpstring;
	c->cmp.cmparg       = NULL;
	c->memory_limit     = 0;
	c->node_size        = 128 * 1024 * 1024; 
	c->node_page_size   = 128 * 1024;
	c->node_branch_wm   = 10 * 1024 * 1024;
	c->node_merge_wm    = 1;
	c->threads_merge    = 2;
	c->threads_branch   = 1;
	c->threads          = 3;
	return 0;
}

int so_dbctl_free(sodbctl *c)
{
	sodb *o = c->parent;
	if (so_dbactive(o))
		return -1;
	if (c->name) {
		sr_free(&o->e->a, c->name);
		c->name = NULL;
	}
	if (c->dir) {
		sr_free(&o->e->a, c->dir);
		c->dir = NULL;
	}
	if (c->logdir) {
		sr_free(&o->e->a, c->logdir);
		c->logdir = NULL;
	}
	return 0;
}

int so_dbctl_validate(sodbctl *c)
{
	if (c->dir == NULL)
		return -1;
	return 0;
}

static int
so_dbctl_branch(srctl *c srunused, void *arg, va_list args srunused)
{
	sodb *db = arg;
	sdc dc;
	sd_cinit(&dc, &db->r);
	int rc;
	while (1) {
		rc = si_branch(&db->index, &db->r, &dc, db->ctl.node_branch_wm);
		if (srunlikely(rc <= 0))
			break;
	}
	sd_cfree(&dc, &db->r);
	return rc;
}

static int
so_dbctl_merge(srctl *c srunused, void *arg, va_list args srunused)
{
	sodb *db = arg;
	sdc dc;
	sd_cinit(&dc, &db->r);
	int rc;
	while (1) {
		rc = si_merge(&db->index, &db->r, &dc, db->ctl.node_merge_wm);
		if (srunlikely(rc <= 0))
			break;
	}
	sd_cfree(&dc, &db->r);
	return rc;
}

static int
so_dbctl_logrotate(srctl *c srunused, void *arg, va_list args srunused)
{
	sodb *db = arg;
	return sl_poolrotate(&db->lp);
}

static int
so_dbctl_cmp(srctl *c srunused, void *arg, va_list args)
{
	sodb *db = arg;
	db->ctl.cmp.cmp = va_arg(args, srcmpf);
	return 0;
}

static int
so_dbctl_cmparg(srctl *c srunused, void *arg, va_list args srunused)
{
	sodb *db = arg;
	db->ctl.cmp.cmparg = va_arg(args, void*);
	return 0;
}

static inline srctl*
setctl(srctl *c, char *name, int type, void *v, srctlf func)
{
	c->name = name;
	c->type = type;
	c->v    = v;
	c->func = func;
	return ++c;
}

static inline void
so_dbctl_prepare(srctl *t, sodbctl *c)
{
	srctl *p = t;
	p = setctl(p, "name",           SR_CTLSTRING|SR_CTLRO, &c->name,           NULL);
	p = setctl(p, "dir",            SR_CTLSTRING,          &c->dir,            NULL);
	p = setctl(p, "dir_read",       SR_CTLINT,             &c->dir_read,       NULL);
	p = setctl(p, "dir_write",      SR_CTLINT,             &c->dir_write,      NULL);
	p = setctl(p, "dir_create",     SR_CTLINT,             &c->dir_create,     NULL);
	p = setctl(p, "logdir",         SR_CTLSTRING,          &c->logdir,         NULL);
	p = setctl(p, "logdir_read",    SR_CTLINT,             &c->logdir_read,    NULL);
	p = setctl(p, "logdir_write",   SR_CTLINT,             &c->logdir_write,   NULL);
	p = setctl(p, "logdir_create",  SR_CTLINT,             &c->logdir_create,  NULL);
	p = setctl(p, "node_size",      SR_CTLINT,             &c->node_size,      NULL);
	p = setctl(p, "node_page_size", SR_CTLINT,             &c->node_page_size, NULL);
	p = setctl(p, "node_branch_wm", SR_CTLINT,             &c->node_branch_wm, NULL);
	p = setctl(p, "node_merge_wm",  SR_CTLINT,             &c->node_merge_wm,  NULL);
	p = setctl(p, "threads",        SR_CTLINT,             &c->threads,        NULL);
	p = setctl(p, "memory_limit",   SR_CTLU64,             &c->memory_limit,   NULL);
	p = setctl(p, "cmp",            SR_CTLTRIGGER,         NULL,               so_dbctl_cmp);
	p = setctl(p, "cmp_arg",        SR_CTLTRIGGER,         NULL,               so_dbctl_cmparg);
	p = setctl(p, "run_branch",     SR_CTLTRIGGER,         NULL,               so_dbctl_branch);
	p = setctl(p, "run_merge",      SR_CTLTRIGGER,         NULL,               so_dbctl_merge);
	p = setctl(p, "run_logrotate",  SR_CTLTRIGGER,         NULL,               so_dbctl_logrotate);
	p = setctl(p, NULL,             0,                     NULL,               NULL);
}

int so_dbctl_set(sodbctl *c, char *path, va_list args)
{
	sodb *db = c->parent;
	srctl ctls[30];
	so_dbctl_prepare(&ctls[0], c);
	srctl *match = NULL;
	int rc = sr_ctlget(&ctls[0], path, &match);
	if (srunlikely(rc ==  1))
		return 0; /* self */
	if (srunlikely(rc == -1))
		return -1;
	if (so_dbactive(db))
		if (match->type != SR_CTLTRIGGER)
			return -1;
	rc = sr_ctlset(match, db->r.a, db, args);
	if (srunlikely(rc == -1))
		return -1;
	return 0;
}

void *so_dbctl_get(sodbctl *c, char *path, va_list args srunused)
{
	sodb *db = c->parent;
	srctl ctls[30];
	so_dbctl_prepare(&ctls[0], c);
	srctl *match = NULL;
	int rc = sr_ctlget(&ctls[0], path, &match);
	if (srunlikely(rc ==  1))
		return &db->o; /* self */
	if (srunlikely(rc == -1))
		return NULL;
	int size = 0;
	int type = match->type & ~SR_CTLRO;
	switch (type) {
	case SR_CTLINT: size = sizeof(int);
		break;
	case SR_CTLU32: size = sizeof(uint32_t);
		break;
	case SR_CTLU64: size = sizeof(uint64_t);
		break;
	case SR_CTLSTRING:
		size = strlen(*(char**)match->v) + 1;
		break;
	default: return NULL;
	}
	svlocal l;
	l.lsn       = 0;
	l.flags     = 0;
	l.keysize   = strlen(match->name) + 1;
	l.key       = match->name;
	l.valuesize = size;
	if (type == SR_CTLSTRING)
		l.value = *(char**)match->v;
	else
		l.value = match->v;
	sv vp;
	svinit(&vp, &sv_localif, &l, NULL);
	svv *v = sv_valloc(&db->e->a, &vp);
	if (srunlikely(v == NULL))
		return NULL;
	sov *result = (sov*)so_vnew(db->e);
	if (srunlikely(result == NULL)) {
		sv_vfree(&db->e->a, v);
		return NULL;
	}
	svinit(&vp, &sv_vif, v, NULL);
	return so_vput(result, &vp);
}
