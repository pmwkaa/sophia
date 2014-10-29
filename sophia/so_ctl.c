
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

void*
so_ctlreturn(srctl *match, void *o)
{
	so *e = o;
	int size = 0;
	int type = match->type & ~SR_CTLRO;
	char integer[64];
	char *value;
	switch (type) {
	case SR_CTLINT:
		size = snprintf(integer, sizeof(integer), "%d", *(int*)match->v);
		value = integer;
		break;
	case SR_CTLU32:
		size = snprintf(integer, sizeof(integer), "%"PRIu32, *(uint32_t*)match->v);
		value = integer;
		break;
	case SR_CTLU64:
		size = snprintf(integer, sizeof(integer), "%"PRIu64, *(uint64_t*)match->v);
		value = integer;
		break;
	case SR_CTLSTRINGREF:
		value = *(char**)match->v;
		if (value)
			size = strlen(value);
		break;
	case SR_CTLSTRING:
		value = match->v;
		if (value)
			size = strlen(value);
		break;
	case SR_CTLTRIGGER: {
		char hint[] = "function";
		value = hint;
		size = sizeof(hint);
		break;
	}
	case SR_CTLSUB: assert(0);
		break;
	}
	if (value)
		size++;
	svlocal l;
	l.lsn       = 0;
	l.flags     = 0;
	l.keysize   = strlen(match->name) + 1;
	l.key       = match->name;
	l.valuesize = size;
	l.value     = value;
	sv vp;
	svinit(&vp, &sv_localif, &l, NULL);
	svv *v = sv_valloc(&e->a, &vp);
	if (srunlikely(v == NULL)) {
		sr_error(&e->error, "%s", "memory allocation failed");
		sr_error_recoverable(&e->error);
		return NULL;
	}
	sov *result = (sov*)so_vnew(e);
	if (srunlikely(result == NULL)) {
		sv_vfree(&e->a, v);
		sr_error(&e->error, "%s", "memory allocation failed");
		sr_error_recoverable(&e->error);
		return NULL;
	}
	svinit(&vp, &sv_vif, v, NULL);
	return so_vput(result, &vp);
}

static int
so_ctlsophia_set(soctl *c, char *path srunused, va_list args srunused)
{
	so *e = c->e;
	sr_error(&e->error, "%s", "control path is ready-only");
	sr_error_recoverable(&e->error);
	return -1;
}

static void*
so_ctlsophia_get(soctl *c, char *path, va_list args srunused)
{
	so *e = c->e;
	int version_major = SR_VERSION_MAJOR - '0';
	int version_minor = SR_VERSION_MINOR - '0';
	char version[16];
	char *version_ptr = version;
	snprintf(version, sizeof(version), "%d.%d",
	         version_major,
	         version_minor);
	char errorsz[128];
	char *error;
	errorsz[0] = 0;
	int errorlen = sr_errorcopy(&e->error, errorsz, sizeof(errorsz));
	if (srlikely(errorlen == 0))
		error = NULL;
	else
		error = errorsz;
	srctl ctls[30];
	srctl *p = ctls;
	p = sr_ctladd(p, "version", SR_CTLSTRING|SR_CTLRO, version_ptr,       NULL);
	p = sr_ctladd(p, "build",   SR_CTLSTRING|SR_CTLRO, SR_VERSION_COMMIT, NULL);
	p = sr_ctladd(p, "error",   SR_CTLSTRING|SR_CTLRO, error,             NULL);
	p = sr_ctladd(p,  NULL,     0,                     NULL,              NULL);
	srctl *match = NULL;
	int rc = sr_ctlget(&ctls[0], &path, &match);
	if (srunlikely(rc == 1 || rc == -1)) {
		sr_error(&e->error, "%s", "bad control path");
		sr_error_recoverable(&e->error);
		return NULL;
	}
	return so_ctlreturn(match, c->e);
}

static int
so_ctlsophia_dump(soctl *c, srbuf *dump)
{
	so *e = c->e;
	int version_major = SR_VERSION_MAJOR - '0';
	int version_minor = SR_VERSION_MINOR - '0';
	char version[16];
	char *version_ptr = version;
	snprintf(version, sizeof(version), "%d.%d",
	         version_major,
	         version_minor);
	char errorsz[128];
	char *error;
	errorsz[0] = 0;
	int errorlen = sr_errorcopy(&e->error, errorsz, sizeof(errorsz));
	if (srlikely(errorlen == 0))
		error = NULL;
	else
		error = errorsz;
	srctl ctls[30];
	srctl *p = ctls;
	p = sr_ctladd(p, "version", SR_CTLSTRING|SR_CTLRO, version_ptr,       NULL);
	p = sr_ctladd(p, "build",   SR_CTLSTRING|SR_CTLRO, SR_VERSION_COMMIT, NULL);
	p = sr_ctladd(p, "error",   SR_CTLSTRING|SR_CTLRO, error,             NULL);
	p = sr_ctladd(p,  NULL,     0,                     NULL,              NULL);
	int rc = sr_ctlserialize(&ctls[0], &e->a, "sophia.", dump);
	if (srunlikely(rc == -1)) {
		sr_error(&e->error, "%s", "memory allocation failed");
		sr_error_recoverable(&e->error);
		return -1;
	}
	return 0;
}

static int
so_ctldb_set(soctl *c, char *path, va_list args)
{
	so *e = c->e;
	char *token;
	token = strtok_r(NULL, ".", &path);
	if (srunlikely(token == NULL)) {
		sr_error(&e->error, "%s", "bad control path");
		sr_error_recoverable(&e->error);
		return -1;
	}
	char *name = token;
	sodb *db = (sodb*)so_dbmatch(e, name);
	if (db == NULL) {
		db = (sodb*)so_dbnew(e, name);
		if (srunlikely(db == NULL))
			return -1;
		so_objindex_register(&e->db, &db->o);
	}
	return so_dbctl_set(&db->ctl, path, args);
}

static void*
so_ctldb_get(soctl *c, char *path, va_list args)
{
	so *e = c->e;
	char *token;
	token = strtok_r(NULL, ".", &path);
	if (srunlikely(token == NULL)) {
		sr_error(&e->error, "%s", "bad control path");
		sr_error_recoverable(&e->error);
		return NULL;
	}
	char *name = token;
	sodb *db = (sodb*)so_dbmatch(e, name);
	if (db == NULL)
		return NULL;
	return so_dbctl_get(&db->ctl, path, args);
}

static int
so_ctldb_dump(soctl *c, srbuf *dump)
{
	so *e = c->e;
	srlist *i;
	sr_listforeach(&e->db.list, i) {
		soobj *o = srcast(i, soobj, link);
		sodb *db = (sodb*)o;
		int rc = so_dbctl_dump(&db->ctl, dump);
		if (srunlikely(rc == -1))
			return -1;
	}
	return 0;
}

static inline void
so_ctlmemory_prepare(srctl *t, soctl *c, srpager *pager)
{
	srctl *p = t;
	p = sr_ctladd(p, "limit",           SR_CTLU64,          &c->memory_limit,  NULL);
	p = sr_ctladd(p, "pager_pool_size", SR_CTLU32|SR_CTLRO, &pager->pool_size, NULL);
	p = sr_ctladd(p, "pager_page_size", SR_CTLU32|SR_CTLRO, &pager->page_size, NULL);
	p = sr_ctladd(p, "pager_pools",     SR_CTLINT|SR_CTLRO, &pager->pools,     NULL);
	p = sr_ctladd(p,  NULL,             0,                  NULL,              NULL);
}

static int
so_ctlmemory_set(so *o, char *path, va_list args)
{
	srctl ctls[30];
	so_ctlmemory_prepare(&ctls[0], &o->ctl, &o->pager);
	srctl *match = NULL;
	int rc = sr_ctlget(&ctls[0], &path, &match);
	if (srunlikely(rc ==  1))
		return -1; /* self */
	if (srunlikely(rc == -1)) {
		sr_error(&o->error, "%s", "bad control path");
		sr_error_recoverable(&o->error);
		return -1;
	}
	int type = match->type & ~SR_CTLRO;
	if (so_active(o) && (type != SR_CTLTRIGGER)) {
		sr_error(&o->error, "%s", "failed to set control path");
		sr_error_recoverable(&o->error);
		return -1;
	}
	rc = sr_ctlset(match, &o->a, NULL, args);
	if (srunlikely(rc == -1)) {
		sr_error_recoverable(&o->error);
		return -1;
	}
	return rc;
}

static void*
so_ctlmemory_get(soctl *c, char *path, va_list args srunused)
{
	so *e = c->e;
	srctl ctls[30];
	so_ctlmemory_prepare(&ctls[0], &e->ctl, &e->pager);
	srctl *match = NULL;
	int rc = sr_ctlget(&ctls[0], &path, &match);
	if (srunlikely(rc ==  1))
		return NULL; /* self */
	return so_ctlreturn(match, e);
}

static int
so_ctlmemory_dump(soctl *c, srbuf *dump)
{
	so *e = c->e;
	srctl ctls[30];
	so_ctlmemory_prepare(&ctls[0], &e->ctl, &e->pager);
	char prefix[64];
	snprintf(prefix, sizeof(prefix), "memory.");
	int rc = sr_ctlserialize(&ctls[0], &e->a, prefix, dump);
	if (srunlikely(rc == -1)) {
		sr_error(&e->error, "%s", "memory allocation failed");
		sr_error_recoverable(&e->error);
		return -1;
	}
	return 0;
}

static inline void
so_ctlscheduler_prepare(srctl *t, soctl *c)
{
	srctl *p = t;
	p = sr_ctladd(p, "threads",        SR_CTLU32, &c->threads,        NULL);
	p = sr_ctladd(p, "node_size",      SR_CTLU32, &c->node_size,      NULL);
	p = sr_ctladd(p, "node_page_size", SR_CTLU32, &c->node_page_size, NULL);
	p = sr_ctladd(p, "node_branch_wm", SR_CTLU32, &c->node_branch_wm, NULL);
	p = sr_ctladd(p, "node_merge_wm",  SR_CTLU32, &c->node_merge_wm,  NULL);
	p = sr_ctladd(p,  NULL,            0,         NULL,               NULL);
}

static int
so_ctlscheduler_set(so *o, char *path, va_list args)
{
	srctl ctls[30];
	so_ctlscheduler_prepare(&ctls[0], &o->ctl);
	srctl *match = NULL;
	int rc = sr_ctlget(&ctls[0], &path, &match);
	if (srunlikely(rc ==  1))
		return -1; /* self */
	if (srunlikely(rc == -1)) {
		sr_error(&o->error, "%s", "bad control path");
		sr_error_recoverable(&o->error);
		return -1;
	}
	int type = match->type & ~SR_CTLRO;
	if (so_active(o) && (type != SR_CTLTRIGGER)) {
		sr_error(&o->error, "%s", "failed to set control path");
		sr_error_recoverable(&o->error);
		return -1;
	}
	rc = sr_ctlset(match, &o->a, NULL, args);
	if (srunlikely(rc == -1)) {
		sr_error_recoverable(&o->error);
		return -1;
	}
	return rc;
}

static void*
so_ctlscheduler_get(soctl *c, char *path, va_list args srunused)
{
	so *e = c->e;
	srctl ctls[30];
	so_ctlscheduler_prepare(&ctls[0], &e->ctl);
	srctl *match = NULL;
	int rc = sr_ctlget(&ctls[0], &path, &match);
	if (srunlikely(rc ==  1))
		return NULL; /* self */
	return so_ctlreturn(match, e);
}

static int
so_ctlscheduler_dump(soctl *c, srbuf *dump)
{
	so *e = c->e;
	srctl ctls[30];
	so_ctlscheduler_prepare(&ctls[0], &e->ctl);
	char prefix[64];
	snprintf(prefix, sizeof(prefix), "scheduler.");
	int rc = sr_ctlserialize(&ctls[0], &e->a, prefix, dump);
	if (srunlikely(rc == -1)) {
		sr_error(&e->error, "%s", "memory allocation failed");
		sr_error_recoverable(&e->error);
		return -1;
	}
	return 0;
}

static int
so_ctlset(soobj *obj, va_list args)
{
	soctl *c = (soctl*)obj;
	so *e = c->e;
	char *path = va_arg(args, char*);
	char q[200];
	snprintf(q, sizeof(q), "%s", path);
	char *ptr = NULL;
	char *token;
	token = strtok_r(q, ".", &ptr);
	if (srunlikely(token == NULL)) {
		sr_error(&e->error, "%s", "bad control path");
		sr_error_recoverable(&e->error);
		return -1;
	}
	if (strcmp(token, "sophia") == 0)
		return so_ctlsophia_set(c, ptr, args);
	else
	if (strcmp(token, "memory") == 0)
		return so_ctlmemory_set(e, ptr, args);
	else
	if (strcmp(token, "scheduler") == 0)
		return so_ctlscheduler_set(e, ptr, args);
	else
	if (strcmp(token, "db") == 0)
		return so_ctldb_set(c, ptr, args);
	sr_error(&e->error, "%s", "unknown control path");
	sr_error_recoverable(&e->error);
	return -1;
}

static void*
so_ctlget(soobj *obj, va_list args)
{
	soctl *c = (soctl*)obj;
	so *e = c->e;
	char *path = va_arg(args, char*);
	char q[200];
	snprintf(q, sizeof(q), "%s", path);
	char *ptr = NULL;
	char *token;
	token = strtok_r(q, ".", &ptr);
	if (srunlikely(token == NULL)) {
		sr_error(&e->error, "%s", "bad control path");
		sr_error_recoverable(&e->error);
		return NULL;
	}
	if (strcmp(token, "sophia") == 0)
		return so_ctlsophia_get(c, ptr, args);
	else
	if (strcmp(token, "memory") == 0)
		return so_ctlmemory_get(c, ptr, args);
	else
	if (strcmp(token, "scheduler") == 0)
		return so_ctlscheduler_get(c, ptr, args);
	else
	if (strcmp(token, "db") == 0)
		return so_ctldb_get(c, ptr, args);
	sr_error(&e->error, "%s", "unknown control path");
	sr_error_recoverable(&e->error);
	return NULL;
}

int so_ctldump(soctl *c, srbuf *dump)
{
	int rc = so_ctlsophia_dump(c, dump);
	if (srunlikely(rc == -1))
		return -1;
	rc = so_ctlmemory_dump(c, dump);
	if (srunlikely(rc == -1))
		return -1;
	rc = so_ctlscheduler_dump(c, dump);
	if (srunlikely(rc == -1))
		return -1;
	rc = so_ctldb_dump(c, dump);
	if (srunlikely(rc == -1))
		return -1;
	return 0;
}

static void*
so_ctlcursor(soobj *o, va_list args srunused)
{
	soctl *c = (soctl*)o;
	return so_ctlcursor_new(c->e);
}

static void*
so_ctltype(soobj *o srunused, va_list args srunused) {
	return "ctl";
}

static soobjif soctlif =
{
	.ctl      = NULL,
	.open     = NULL,
	.destroy  = NULL,
	.error    = NULL,
	.set      = so_ctlset,
	.get      = so_ctlget,
	.del      = NULL,
	.begin    = NULL,
	.prepare  = NULL,
	.commit   = NULL,
	.rollback = NULL,
	.cursor   = so_ctlcursor,
	.object   = NULL,
	.type     = so_ctltype
};

void so_ctlinit(soctl *c, void *e)
{
	so_objinit(&c->o, SOCTL, &soctlif, e);
	c->e = e;
	c->memory_limit   = 0;
	c->node_size      = 128 * 1024 * 1024;
	c->node_page_size = 128 * 1024;
	c->node_branch_wm = 10 * 1024 * 1024;
	c->node_merge_wm  = 1;
	c->threads        = 5;
}
