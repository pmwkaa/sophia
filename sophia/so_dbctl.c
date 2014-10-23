
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
	if (srunlikely(c->name == NULL)) {
		sr_error(&o->e->error, "%s", "memory allocation failed");
		sr_error_recoverable(&o->e->error);
		return -1;
	}
	c->parent            = db;
	c->log_dircreate     = 1;
	c->log_dirwrite      = 1;
	c->log_rotate_wm     = 500000;
	c->log_sync          = 0;
	c->log_rotate_sync   = 1;
	c->two_phase_recover = 0;
	c->dir_create        = 1;
	c->dir_created       = 0;
	c->dir_write         = 1;
	c->dir_sync          = 1;
	c->cmp.cmp           = sr_cmpstring;
	c->cmp.cmparg        = NULL;
	c->commit_lsn        = 0;
	c->memory_limit      = 0;
	c->node_size         = 128 * 1024 * 1024;
	c->node_page_size    = 128 * 1024;
	c->node_branch_wm    = 10 * 1024 * 1024;
	c->node_merge_wm     = 1;
	c->threads_merge     = 4;
	c->threads_branch    = 1;
	c->threads           = 5;
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
	if (c->log_dir) {
		sr_free(&o->e->a, c->log_dir);
		c->log_dir = NULL;
	}
	return 0;
}

int so_dbctl_validate(sodbctl *c)
{
	sodb *o = c->parent;
	if (c->dir == NULL) {
		sr_error(&o->e->error, "%s", "database directory is not set");
		sr_error_recoverable(&o->e->error);
		return -1;
	}
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
		uint64_t lsvn = sm_lsvn(&db->mvcc);
		rc = si_branch(&db->index, &db->r, &dc, lsvn, db->ctl.node_branch_wm);
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
		uint64_t lsvn = sm_lsvn(&db->mvcc);
		rc = si_merge(&db->index, &db->r, &dc, lsvn, db->ctl.node_merge_wm);
		if (srunlikely(rc <= 0))
			break;
	}
	sd_cfree(&dc, &db->r);
	return rc;
}

static int
so_dbctl_lockdetect(srctl *c srunused, void *arg, va_list args)
{
	sodb *db = arg;
	sotx *tx = va_arg(args, sotx*);
	if (srunlikely(tx->db != db)) {
		sr_error(&db->e->error, "%s", "transaction does not match a parent db object");
		sr_error_recoverable(&db->e->error);
		return -1;
	}
	int rc = sm_deadlock(&tx->t);
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

typedef struct sodbctlinfo sodbctlinfo;

struct sodbctlinfo {
	char *status;
};

static inline void
so_dbctl_info(sodb *db, sodbctlinfo *info)
{
	info->status = so_statusof(&db->status);
}

static inline void
so_dbctl_prepare(srctl *t, sodbctl *c, sodbctlinfo *info)
{
	sodb *db = c->parent;
	so_dbctl_info(db, info);
	srctl *p = t;
	p = sr_ctladd(p, "name",               SR_CTLSTRING|SR_CTLRO, c->name,               NULL);
	p = sr_ctladd(p, "status",             SR_CTLSTRING|SR_CTLRO, info->status,          NULL);
	p = sr_ctladd(p, "dir",                SR_CTLSTRINGREF,       &c->dir,               NULL);
	p = sr_ctladd(p, "dir_write",          SR_CTLINT,             &c->dir_write,         NULL);
	p = sr_ctladd(p, "dir_create",         SR_CTLINT,             &c->dir_create,        NULL);
	p = sr_ctladd(p, "dir_sync",           SR_CTLINT,             &c->dir_sync,          NULL);
	p = sr_ctladd(p, "log_dir",            SR_CTLSTRINGREF,       &c->log_dir,           NULL);
	p = sr_ctladd(p, "log_dirwrite",       SR_CTLINT,             &c->log_dirwrite,      NULL);
	p = sr_ctladd(p, "log_dircreate",      SR_CTLINT,             &c->log_dircreate,     NULL);
	p = sr_ctladd(p, "log_sync",           SR_CTLINT,             &c->log_sync,          NULL);
	p = sr_ctladd(p, "log_rotate_wm",      SR_CTLINT,             &c->log_rotate_wm,     NULL);
	p = sr_ctladd(p, "log_rotate_sync",    SR_CTLINT,             &c->log_rotate_sync,   NULL);
	p = sr_ctladd(p, "two_phase_recover",  SR_CTLINT,             &c->two_phase_recover, NULL);
	p = sr_ctladd(p, "commit_lsn",         SR_CTLINT,             &c->commit_lsn,        NULL);
	p = sr_ctladd(p, "node_size",          SR_CTLINT,             &c->node_size,         NULL);
	p = sr_ctladd(p, "node_page_size",     SR_CTLINT,             &c->node_page_size,    NULL);
	p = sr_ctladd(p, "node_branch_wm",     SR_CTLINT,             &c->node_branch_wm,    NULL);
	p = sr_ctladd(p, "node_merge_wm",      SR_CTLINT,             &c->node_merge_wm,     NULL);
	p = sr_ctladd(p, "threads",            SR_CTLINT,             &c->threads,           NULL);
	p = sr_ctladd(p, "memory_limit",       SR_CTLU64,             &c->memory_limit,      NULL);
	p = sr_ctladd(p, "cmp",                SR_CTLTRIGGER,         NULL,                  so_dbctl_cmp);
	p = sr_ctladd(p, "cmp_arg",            SR_CTLTRIGGER,         NULL,                  so_dbctl_cmparg);
	p = sr_ctladd(p, "run_branch",         SR_CTLTRIGGER,         NULL,                  so_dbctl_branch);
	p = sr_ctladd(p, "run_merge",          SR_CTLTRIGGER,         NULL,                  so_dbctl_merge);
	p = sr_ctladd(p, "run_logrotate",      SR_CTLTRIGGER,         NULL,                  so_dbctl_logrotate);
	p = sr_ctladd(p, "run_lockdetect",     SR_CTLTRIGGER,         NULL,                  so_dbctl_lockdetect);
	p = sr_ctladd(p, "profiler",           SR_CTLSUB,             NULL,                  NULL);
	p = sr_ctladd(p, "error_injection",    SR_CTLSUB,             NULL,                  NULL);
	p = sr_ctladd(p,  NULL,                0,                     NULL,                  NULL);
}

static inline void
so_dbprofiler_prepare(srctl *t, siprofiler *pf, srpager *pager)
{
	srctl *p = t;
	p = sr_ctladd(p, "pager_pool_size",    SR_CTLU32|SR_CTLRO,    &pager->pool_size,        NULL);
	p = sr_ctladd(p, "pager_page_size",    SR_CTLU32|SR_CTLRO,    &pager->page_size,        NULL);
	p = sr_ctladd(p, "pager_pools",        SR_CTLINT|SR_CTLRO,    &pager->pools,            NULL);
	p = sr_ctladd(p, "index_node_count",   SR_CTLU32|SR_CTLRO,    &pf->total_node_count,    NULL);
	p = sr_ctladd(p, "index_node_size",    SR_CTLU64|SR_CTLRO,    &pf->total_node_size,     NULL);
	p = sr_ctladd(p, "index_branch_count", SR_CTLU32|SR_CTLRO,    &pf->total_branch_count,  NULL);
	p = sr_ctladd(p, "index_branch_avg",   SR_CTLU32|SR_CTLRO,    &pf->total_branch_avg,    NULL);
	p = sr_ctladd(p, "index_branch_max",   SR_CTLU32|SR_CTLRO,    &pf->total_branch_max,    NULL);
	p = sr_ctladd(p, "index_branch_size",  SR_CTLU64|SR_CTLRO,    &pf->total_branch_size,   NULL);
	p = sr_ctladd(p, "index_memory_used",  SR_CTLU64|SR_CTLRO,    &pf->memory_used,         NULL);
	p = sr_ctladd(p, "index_count",        SR_CTLU64|SR_CTLRO,    &pf->count,               NULL);
	p = sr_ctladd(p, "seq_nsn",            SR_CTLU32|SR_CTLRO,    &pf->seq.nsn,             NULL);
	p = sr_ctladd(p, "seq_lsn",            SR_CTLU64|SR_CTLRO,    &pf->seq.lsn,             NULL);
	p = sr_ctladd(p, "seq_lfsn",           SR_CTLU32|SR_CTLRO,    &pf->seq.lfsn,            NULL);
	p = sr_ctladd(p, "seq_tsn",            SR_CTLU32|SR_CTLRO,    &pf->seq.tsn,             NULL);
	p = sr_ctladd(p, "histogram_branch",   SR_CTLSTRING|SR_CTLRO, pf->histogram_branch_ptr, NULL);
	p = sr_ctladd(p,  NULL,                0,                     NULL,                     NULL);
}

static inline void
so_dbei_prepare(srctl *t, srinjection *i)
{
	srctl *p = t;
	p = sr_ctladd(p, "si_branch_0", SR_CTLINT, &i->e[0], NULL);
	p = sr_ctladd(p, "si_branch_1", SR_CTLINT, &i->e[1], NULL);
	p = sr_ctladd(p, "si_merge_0",  SR_CTLINT, &i->e[2], NULL);
	p = sr_ctladd(p, "si_merge_1",  SR_CTLINT, &i->e[3], NULL);
	p = sr_ctladd(p, "si_merge_2",  SR_CTLINT, &i->e[4], NULL);
	p = sr_ctladd(p, "si_merge_3",  SR_CTLINT, &i->e[5], NULL);
	p = sr_ctladd(p, "si_merge_4",  SR_CTLINT, &i->e[6], NULL);
	p = sr_ctladd(p,  NULL,         0,          NULL,    NULL);
}

static void*
so_dbprofiler_get(sodb *db, char *path)
{
	siprofiler pf;
	si_profilerbegin(&pf, &db->index);
	si_profiler(&pf, &db->r);
	si_profilerend(&pf);
	srctl ctls[30];
	so_dbprofiler_prepare(&ctls[0], &pf, &db->e->pager);
	srctl *match = NULL;
	int rc = sr_ctlget(&ctls[0], &path, &match);
	if (srunlikely(rc == 1 || rc == -1)) {
		sr_error(&db->e->error, "%s", "bad control path");
		sr_error_recoverable(&db->e->error);
		return NULL;
	}
	return so_ctlreturn(match, db->e);
}

static int
so_dbprofiler_dump(sodb *db, srbuf *dump)
{
	siprofiler pf;
	si_profilerbegin(&pf, &db->index);
	si_profiler(&pf, &db->r);
	si_profilerend(&pf);
	srctl ctls[30];
	so_dbprofiler_prepare(&ctls[0], &pf, &db->e->pager);
	char prefix[64];
	snprintf(prefix, sizeof(prefix), "db.%s.profiler.", db->ctl.name);
	int rc = sr_ctlserialize(&ctls[0], &db->e->a, prefix, dump);
	if (srunlikely(rc == -1)) {
		sr_error(&db->e->error, "%s", "memory allocation failed");
		sr_error_recoverable(&db->e->error);
		return -1;
	}
	return 0;
}

static int
so_dbei_set(sodb *db, char *path, va_list args)
{
	srctl ctls[30];
	so_dbei_prepare(&ctls[0], &db->ei);
	srctl *match = NULL;
	int rc = sr_ctlget(&ctls[0], &path, &match);
	if (srunlikely(rc == 1 || rc == -1)) {
		sr_error(&db->e->error, "%s", "bad control path");
		sr_error_recoverable(&db->e->error);
		return -1;
	}
	rc = sr_ctlset(match, db->r.a, db, args);
	if (srunlikely(rc == -1)) {
		sr_error_recoverable(&db->e->error);
		return -1;
	}
	return rc;
}

static void*
so_dbei_get(sodb *db, char *path)
{
	srctl ctls[30];
	so_dbei_prepare(&ctls[0], &db->ei);
	srctl *match = NULL;
	int rc = sr_ctlget(&ctls[0], &path, &match);
	if (srunlikely(rc == 1 || rc == -1)) {
		sr_error(&db->e->error, "%s", "bad control path");
		sr_error_recoverable(&db->e->error);
		return NULL;
	}
	return so_ctlreturn(match, db->e);
}

#if 0
static int
so_dbei_dump(sodb *db, srbuf *dump)
{
	srctl ctls[30];
	so_dbei_prepare(&ctls[0], &db->ei);
	char prefix[64];
	snprintf(prefix, sizeof(prefix), "db.%s.error_injection.", db->ctl.name);
	int rc = sr_ctlserialize(&ctls[0], &db->e->a, prefix, dump);
	if (srunlikely(rc == -1)) {
		sr_error(&db->e->error, "%s", "memory allocation failed");
		sr_error_recoverable(&db->e->error);
		return -1;
	}
	return 0;
}
#endif

int so_dbctl_set(sodbctl *c, char *path, va_list args)
{
	sodb *db = c->parent;
	srctl ctls[30];
	sodbctlinfo info;
	so_dbctl_prepare(&ctls[0], c, &info);
	srctl *match = NULL;
	int rc = sr_ctlget(&ctls[0], &path, &match);
	if (srunlikely(rc ==  1))
		return  0; /* self */
	if (srunlikely(rc == -1)) {
		sr_error(&db->e->error, "%s", "bad control path");
		sr_error_recoverable(&db->e->error);
		return -1;
	}
	int type = match->type & ~SR_CTLRO;
	if (type == SR_CTLSUB) {
		if (strcmp(match->name, "error_injection") == 0)
			return so_dbei_set(db, path, args);
	}
	if (so_dbactive(db) && (type != SR_CTLTRIGGER)) {
		sr_error(&db->e->error, "%s", "failed to set control path");
		sr_error_recoverable(&db->e->error);
		return -1;
	}
	rc = sr_ctlset(match, db->r.a, db, args);
	if (srunlikely(rc == -1)) {
		sr_error_recoverable(&db->e->error);
		return -1;
	}
	return rc;
}

void *so_dbctl_get(sodbctl *c, char *path, va_list args srunused)
{
	sodb *db = c->parent;
	srctl ctls[30];
	sodbctlinfo info;
	so_dbctl_prepare(&ctls[0], c, &info);
	srctl *match = NULL;
	int rc = sr_ctlget(&ctls[0], &path, &match);
	if (srunlikely(rc ==  1))
		return &db->o; /* self */
	if (srunlikely(rc == -1)) {
		sr_error(&db->e->error, "%s", "bad control path");
		sr_error_recoverable(&db->e->error);
		return NULL;
	}
	int type = match->type & ~SR_CTLRO;
	if (type == SR_CTLSUB) {
		if (strcmp(match->name, "profiler") == 0)
			return so_dbprofiler_get(db, path);
		else
		if (strcmp(match->name, "error_injection") == 0)
			return so_dbei_get(db, path);
		sr_error(&db->e->error, "%s", "unknown control path");
		sr_error_recoverable(&db->e->error);
		return NULL;
	}
	return so_ctlreturn(match, db->e);
}

int so_dbctl_dump(sodbctl *c, srbuf *dump)
{
	sodb *db = c->parent;
	srctl ctls[30];
	sodbctlinfo info;
	so_dbctl_prepare(&ctls[0], c, &info);
	char prefix[64];
	snprintf(prefix, sizeof(prefix), "db.%s.", c->name);
	int rc = sr_ctlserialize(&ctls[0], &db->e->a, prefix, dump);
	if (srunlikely(rc == -1)) {
		sr_error(&db->e->error, "%s", "memory allocation failed");
		sr_error_recoverable(&db->e->error);
		return -1;
	}
	rc = so_dbprofiler_dump(db, dump);
	if (srunlikely(rc == -1))
		return -1;
	return 0;
}
