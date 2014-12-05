
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

void*
so_ctlreturn(srctl *match, void *o)
{
	so *e = o;
	int size = 0;
	int type = match->type & ~SR_CTLRO;
	char *value = NULL;
	char function_sz[] = "function";
	char integer[64];
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
		value = function_sz;
		size = sizeof(function_sz);
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
	p = sr_ctladd(p, "path",    SR_CTLSTRINGREF,       &c->path,          NULL);
	p = sr_ctladd(p,  NULL,     0,                     NULL,              NULL);
	srctl *match = NULL;
	int rc = sr_ctlget(&ctls[0], &path, &match);
	if (srunlikely(rc == 1 || rc == -1)) {
		sr_error(&e->error, "%s", "bad control path");
		sr_error_recoverable(&e->error);
		return -1;
	}
	int type = match->type & ~SR_CTLRO;
	if (so_active(e) && (type != SR_CTLTRIGGER)) {
		sr_error(&e->error, "%s", "failed to set control path");
		sr_error_recoverable(&e->error);
		return -1;
	}
	rc = sr_ctlset(match, &e->a, e, args);
	if (srunlikely(rc == -1)) {
		sr_error_recoverable(&e->error);
		return -1;
	}
	return rc;
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
	p = sr_ctladd(p, "path",    SR_CTLSTRINGREF,       &c->path,          NULL);
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
	p = sr_ctladd(p, "path",    SR_CTLSTRINGREF,       &c->path,          NULL);
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

typedef struct {
	uint64_t used;
} somemoryinfo;

static inline void
so_ctlmemory_prepare(srctl *t, soctl *c, srpager *pager, somemoryinfo *mi)
{
	so *e = c->e;
	mi->used = sr_quotaused(&e->quota);
	srctl *p = t;
	p = sr_ctladd(p, "limit",           SR_CTLU64,          &c->memory_limit,  NULL);
	p = sr_ctladd(p, "used",            SR_CTLU64|SR_CTLRO, &mi->used,         NULL);
	p = sr_ctladd(p, "pager_pool_size", SR_CTLU32|SR_CTLRO, &pager->pool_size, NULL);
	p = sr_ctladd(p, "pager_page_size", SR_CTLU32|SR_CTLRO, &pager->page_size, NULL);
	p = sr_ctladd(p, "pager_pools",     SR_CTLINT|SR_CTLRO, &pager->pools,     NULL);
	p = sr_ctladd(p,  NULL,             0,                  NULL,              NULL);
}

static int
so_ctlmemory_set(so *o, char *path, va_list args)
{
	srctl ctls[30];
	somemoryinfo mi;
	so_ctlmemory_prepare(&ctls[0], &o->ctl, &o->pager, &mi);
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
	rc = sr_ctlset(match, &o->a, o, args);
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
	somemoryinfo mi;
	so_ctlmemory_prepare(&ctls[0], &e->ctl, &e->pager, &mi);
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
	somemoryinfo mi;
	so_ctlmemory_prepare(&ctls[0], &e->ctl, &e->pager, &mi);
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

static int
so_ctllog_rotate(srctl *c srunused, void *arg, va_list args srunused)
{
	so *e = arg;
	return sl_poolrotate(&e->lp);
}

static int
so_ctllog_gc(srctl *c srunused, void *arg, va_list args srunused)
{
	so *e = arg;
	return sl_poolgc(&e->lp);
}

typedef struct {
	int files;
} soctllog;

static inline void
so_ctllog_prepare(srctl *t, soctl *c, soctllog *l)
{
	so *e = c->e;
	l->files = sl_poolfiles(&e->lp);
	srctl *p = t;
	p = sr_ctladd(p, "enable",            SR_CTLINT,          &c->log_enable,        NULL);
	p = sr_ctladd(p, "path",              SR_CTLSTRINGREF,    &c->log_path,          NULL);
	p = sr_ctladd(p, "sync",              SR_CTLINT,          &c->log_sync,          NULL);
	p = sr_ctladd(p, "rotate_wm",         SR_CTLINT,          &c->log_rotate_wm,     NULL);
	p = sr_ctladd(p, "rotate_sync",       SR_CTLINT,          &c->log_rotate_sync,   NULL);
	p = sr_ctladd(p, "rotate",            SR_CTLTRIGGER,      NULL,                  so_ctllog_rotate);
	p = sr_ctladd(p, "gc",                SR_CTLTRIGGER,      NULL,                  so_ctllog_gc);
	p = sr_ctladd(p, "files",             SR_CTLINT|SR_CTLRO, &l->files,             NULL);
	p = sr_ctladd(p, "two_phase_recover", SR_CTLINT,          &c->two_phase_recover, NULL);
	p = sr_ctladd(p, "commit_lsn",        SR_CTLINT,          &c->commit_lsn,        NULL);
	p = sr_ctladd(p,  NULL,               0,                  NULL,                  NULL);
}

static int
so_ctllog_set(so *o, char *path, va_list args)
{
	srctl ctls[30];
	soctllog l;
	memset(&l, 0, sizeof(l));
	so_ctllog_prepare(&ctls[0], &o->ctl, &l);
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
	rc = sr_ctlset(match, &o->a, o, args);
	if (srunlikely(rc == -1)) {
		sr_error_recoverable(&o->error);
		return -1;
	}
	return rc;
}

static void*
so_ctllog_get(soctl *c, char *path, va_list args srunused)
{
	so *e = c->e;
	soctllog l;
	memset(&l, 0, sizeof(l));
	srctl ctls[30];
	so_ctllog_prepare(&ctls[0], &e->ctl, &l);
	srctl *match = NULL;
	int rc = sr_ctlget(&ctls[0], &path, &match);
	if (srunlikely(rc ==  1))
		return NULL; /* self */
	return so_ctlreturn(match, e);
}

static int
so_ctllog_dump(soctl *c, srbuf *dump)
{
	so *e = c->e;
	soctllog l;
	memset(&l, 0, sizeof(l));
	srctl ctls[30];
	so_ctllog_prepare(&ctls[0], &e->ctl, &l);
	char prefix[64];
	snprintf(prefix, sizeof(prefix), "log.");
	int rc = sr_ctlserialize(&ctls[0], &e->a, prefix, dump);
	if (srunlikely(rc == -1)) {
		sr_error(&e->error, "%s", "memory allocation failed");
		sr_error_recoverable(&e->error);
		return -1;
	}
	return 0;
}

static inline void
so_ctlcompaction_prepare(srctl *t, soctl *c)
{
	srctl *p = t;
	p = sr_ctladd(p, "node_size", SR_CTLU32, &c->node_size, NULL);
	p = sr_ctladd(p, "page_size", SR_CTLU32, &c->page_size, NULL);
	int i = 0;
	while (i < 11) {
		sizone *z = &c->zones.zones[i];
		if (z->enable) {
			p = sr_ctladd(p, z->name, SR_CTLSUB, z, NULL);
		}
		i++;
	}
	p = sr_ctladd(p, NULL, 0, NULL, NULL);
}

static inline void
so_ctlzone_prepare(srctl *t, sizone *z)
{
	srctl *p = t;
	p = sr_ctladd(p, "mode",          SR_CTLU32, &z->mode,          NULL);
	p = sr_ctladd(p, "compact_wm",    SR_CTLU32, &z->compact_wm,    NULL);
	p = sr_ctladd(p, "branch_prio",   SR_CTLU32, &z->branch_prio,   NULL);
	p = sr_ctladd(p, "branch_wm",     SR_CTLU32, &z->branch_wm,     NULL);
	p = sr_ctladd(p, "branch_ttl",    SR_CTLU32, &z->branch_ttl,    NULL);
	p = sr_ctladd(p, "branch_ttl_wm", SR_CTLU32, &z->branch_ttl_wm, NULL);
	p = sr_ctladd(p,  NULL,           0,         NULL,              NULL);
}

static int
so_ctlzone_set(so *o, sizone *z, char *path, va_list args)
{
	srctl ctls[30];
	so_ctlzone_prepare(&ctls[0], z);
	srctl *match = NULL;
	int rc = sr_ctlget(&ctls[0], &path, &match);
	if (srunlikely(rc ==  1))
		return  0; /* self */
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
	rc = sr_ctlset(match, &o->a, o, args);
	if (srunlikely(rc == -1)) {
		sr_error_recoverable(&o->error);
		return -1;
	}
	return rc;
}

static void*
so_ctlzone_get(soctl *c, sizone *z, char *path, va_list args srunused)
{
	so *e = c->e;
	srctl ctls[30];
	so_ctlzone_prepare(&ctls[0], z);
	srctl *match = NULL;
	int rc = sr_ctlget(&ctls[0], &path, &match);
	if (srunlikely(rc ==  1))
		return NULL; /* self */
	return so_ctlreturn(match, e);
}

static int
so_ctlzone_dump(soctl *c, sizone *z, char *name, srbuf *dump)
{
	so *e = c->e;
	srctl ctls[30];
	so_ctlzone_prepare(&ctls[0], z);
	char prefix[64];
	snprintf(prefix, sizeof(prefix), "compaction.%s.", name);
	int rc = sr_ctlserialize(&ctls[0], &e->a, prefix, dump);
	if (srunlikely(rc == -1)) {
		sr_error(&e->error, "%s", "memory allocation failed");
		sr_error_recoverable(&e->error);
		return -1;
	}
	return 0;
}

static int
so_ctlcompaction_set(so *o, char *path, va_list args)
{
	srctl ctls[30];
	so_ctlcompaction_prepare(&ctls[0], &o->ctl);
	if (so_active(o)) {
		sr_error(&o->error, "%s", "failed to set control path");
		sr_error_recoverable(&o->error);
		return -1;
	}
	char *token;
	token = strtok_r(NULL, ".", &path);
	if (srunlikely(token == NULL)) {
		sr_error(&o->error, "%s", "bad control path");
		sr_error_recoverable(&o->error);
		return -1;
	}
	srctl *match = NULL;
	int rc = sr_ctlmatch(&ctls[0], token, &match);
	if (srunlikely(rc == -1)) {
		uint32_t percent = atoi(token);
		if (percent > 100) {
			sr_error(&o->error, "%s", "bad control path");
			sr_error_recoverable(&o->error);
			return -1;
		}
		sizone z;
		memset(&z, 0, sizeof(z));
		z.enable = 1;
		si_zonemap_set(&o->ctl.zones, percent, &z);
		return so_ctlzone_set(o, &z, path, args);
	}
	int type = match->type & ~SR_CTLRO;
	if (type == SR_CTLSUB) {
		return so_ctlzone_set(o, match->v, path, args);
	}
	rc = sr_ctlset(match, &o->a, o, args);
	if (srunlikely(rc == -1)) {
		sr_error_recoverable(&o->error);
		return -1;
	}
	return rc;
}

static void*
so_ctlcompaction_get(soctl *c, char *path, va_list args srunused)
{
	so *e = c->e;
	srctl ctls[30];
	so_ctlcompaction_prepare(&ctls[0], &e->ctl);
	srctl *match = NULL;
	int rc = sr_ctlget(&ctls[0], &path, &match);
	if (srunlikely(rc ==  1))
		return NULL; /* self */
	if (srunlikely(rc == -1)) {
		sr_error(&e->error, "%s", "bad control path");
		sr_error_recoverable(&e->error);
		return NULL;
	}
	int type = match->type & ~SR_CTLRO;
	if (type == SR_CTLSUB) {
		return so_ctlzone_get(c, match->v, path, args);
	}
	return so_ctlreturn(match, e);
}

static int
so_ctlcompaction_dump(soctl *c, srbuf *dump)
{
	so *e = c->e;
	srctl ctls[30];
	so_ctlcompaction_prepare(&ctls[0], &e->ctl);
	char prefix[64];
	snprintf(prefix, sizeof(prefix), "compaction.");
	int rc = sr_ctlserialize(&ctls[0], &e->a, prefix, dump);
	if (srunlikely(rc == -1)) {
		sr_error(&e->error, "%s", "memory allocation failed");
		sr_error_recoverable(&e->error);
		return -1;
	}
	int i = 0;
	while (i < 11) {
		sizone *z = &c->zones.zones[i];
		if (z->enable) {
			rc = so_ctlzone_dump(c, z, z->name, dump);
			if (srunlikely(rc == -1))
				return -1;
		}
		i++;
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
	if (strcmp(token, "compaction") == 0)
		return so_ctlcompaction_set(e, ptr, args);
	else
	if (strcmp(token, "scheduler") == 0)
		return so_schedulerctl_set(&e->o, ptr, args);
	else
	if (strcmp(token, "log") == 0)
		return so_ctllog_set(e, ptr, args);
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
	if (strcmp(token, "compaction") == 0)
		return so_ctlcompaction_get(c, ptr, args);
	else
	if (strcmp(token, "scheduler") == 0)
		return so_schedulerctl_get(&e->o, ptr, args);
	else
	if (strcmp(token, "log") == 0)
		return so_ctllog_get(c, ptr, args);
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
	rc = so_ctlcompaction_dump(c, dump);
	if (srunlikely(rc == -1))
		return -1;
	rc = so_schedulerctl_dump(c->e, dump);
	if (srunlikely(rc == -1))
		return -1;
	rc = so_ctllog_dump(c, dump);
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
	so *o = e;
	so_objinit(&c->o, SOCTL, &soctlif, e);

	c->path         = NULL;
	c->memory_limit = 0;
	c->node_size    = 128 * 1024 * 1024;
	c->page_size    = 128 * 1024;

	sizone unlimited = {
		.enable        = 1,
		.mode          = 3, /* branch + compact */
		.compact_wm    = 1,
		.branch_prio   = 1,
		.branch_wm     = 10 * 1024 * 1024,
		.branch_ttl    = 40,
		.branch_ttl_wm = 1 * 1024 * 1024
	};

	sizone redzone = {
		.enable        = 1,
		.mode          = 2, /* checkpoint */
		.compact_wm    = 0,
		.branch_prio   = 0,
		.branch_wm     = 0,
		.branch_ttl    = 0,
		.branch_ttl_wm = 0
	};

	si_zonemap_set(&o->ctl.zones,  0, &unlimited);
	si_zonemap_set(&o->ctl.zones, 80, &redzone);

#if 0
	c->z0.mode            = 3; /* compact_index */
	c->z0.compact_wm      = 1; /* 0 */
	c->z0.branch_prio     = 1; /* 0 */
	c->z0.branch_wm       = 10 * 1024 * 1024;
	c->z0.branch_ttl      = 40;
	c->z0.branch_ttl_wm   = 1 * 1024 * 1024;
	/* <= 50 */
	c->za.mode            = 3; /* compact index + branching */
	c->za.compact_wm      = 1;
	c->za.branch_prio     = 1;
	c->za.branch_wm       = 10 * 1024 * 1024;
	c->za.branch_ttl      = 40;
	c->za.branch_ttl_wm   = 2 * 1024 * 1024;
	/* <= 60 */
	c->zb.mode            = 3; /* branch + compact */
	c->zb.compact_wm      = 1;
	c->zb.branch_prio     = 1;
	c->zb.branch_wm       = 9 * 1024 * 1024;
	c->zb.branch_ttl      = 30;
	c->zb.branch_ttl_wm   = 1 * 1024 * 1024;
	/* <= 70 */
	c->zc.mode            = 3; /* branch + compact */
	c->zc.compact_wm      = 1;
	c->zc.branch_prio     = 2;
	c->zc.branch_wm       = 6 * 1024 * 1024;
	c->zc.branch_ttl      = 20;
	c->zc.branch_ttl_wm   = 1 * 1024 * 1024;
	/* <= 80 */
	c->zd.mode            = 3; /* branch + compact */
	c->zd.compact_wm      = 2;
	c->zd.branch_prio     = 3;
	c->zd.branch_wm       = 4 * 1024 * 1024;
	c->zd.branch_ttl      = 15;
	c->zd.branch_ttl_wm   = 1 * 1024 * 1024;
	/*  > 80 */
	c->ze.mode            = 2; /* checkpoint */
	c->ze.compact_wm      = 3;
	c->ze.branch_prio     = 4;
	c->ze.branch_wm       = 1; /* any > 0 */
	c->ze.branch_ttl      = 10;
	c->ze.branch_ttl_wm   = 1 * 1024 * 1024;
#endif

	c->threads            = 5;
	c->log_enable         = 1;
	c->log_path           = NULL;
	c->log_rotate_wm      = 500000;
	c->log_sync           = 0;
	c->log_rotate_sync    = 1;
	c->two_phase_recover  = 0;
	c->commit_lsn         = 0;
	c->e                  = e;
}

void so_ctlfree(soctl *c)
{
	if (c->log_path) {
		sr_free(&((so*)c->e)->a, c->log_path);
		c->log_path = NULL;
	}
}

int so_ctlvalidate(soctl *c)
{
	so *e = c->e;
	if (c->path == NULL) {
		sr_error(&e->error, "%s", "repository path is not set");
		sr_error_recoverable(&e->error);
		return -1;
	}
	char path[1024];
	if (c->log_path == NULL) {
		snprintf(path, sizeof(path), "%s/log", c->path);
		c->log_path = sr_strdup(&e->a, path);
		if (srunlikely(c->log_path == NULL)) {
			sr_error(&e->error, "%s", "memory allocation failed");
			sr_error_recoverable(&e->error);
			return -1;
		}
	}
	return 0;
}
