
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

void *so_ctlreturn(src *c, void *o)
{
	so *e = o;
	int size = 0;
	int type = c->flags & ~SR_CRO;
	char *value = NULL;
	char function_sz[] = "function";
	char integer[64];
	switch (type) {
	case SR_CU32:
		size = snprintf(integer, sizeof(integer), "%"PRIu32, *(uint32_t*)c->value);
		value = integer;
		break;
	case SR_CU64:
		size = snprintf(integer, sizeof(integer), "%"PRIu64, *(uint64_t*)c->value);
		value = integer;
		break;
	case SR_CSZREF:
		value = *(char**)c->value;
		if (value)
			size = strlen(value);
		break;
	case SR_CSZ:
		value = c->value;
		if (value)
			size = strlen(value);
		break;
	case SR_CVOID: {
		value = function_sz;
		size = sizeof(function_sz) - 1;
		break;
	}
	case SR_CC: assert(0);
		break;
	}
	if (sslikely(value))
		size++;
	sfv fv;
	fv.key      = c->name;
	fv.r.size   = strlen(c->name) + 1;
	fv.r.offset = 0;
	svv *v = sv_vbuild(&e->r, &fv, 1, value, size);
	if (ssunlikely(v == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	sov *result = (sov*)so_vnew(e, NULL);
	if (ssunlikely(result == NULL)) {
		sv_vfree(&e->a, v);
		sr_oom(&e->error);
		return NULL;
	}
	sv vp;
	sv_init(&vp, &sv_vif, v, NULL);
	return so_vput(result, &vp);
}

static inline int
so_ctlv(src *c, srcstmt *s, va_list args)
{
	switch (s->op) {
	case SR_CGET: {
		void *ret = so_ctlreturn(c, s->ptr);
		if ((ssunlikely(ret == NULL)))
			return -1;
		*s->result = ret;
		return 0;
	}
	case SR_CSERIALIZE:
		return sr_cserialize(c, s);
	case SR_CSET: {
		char *arg = va_arg(args, char*);
		return sr_cset(c, s, arg);
	}
	}
	assert(0);
	return -1;
}

static inline int
so_ctlv_offline(src *c, srcstmt *s, va_list args)
{
	so *e = s->ptr;
	if (ssunlikely(s->op == SR_CSET && so_statusactive(&e->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	return so_ctlv(c, s, args);
}

static inline int
so_ctlsophia_error(src *c, srcstmt *s, va_list args ssunused)
{
	so *e = s->ptr;
	char *errorp;
	char  error[128];
	error[0] = 0;
	int len = sr_errorcopy(&e->error, error, sizeof(error));
	if (sslikely(len == 0))
		errorp = NULL;
	else
		errorp = error;
	src ctl = {
		.name     = c->name,
		.flags    = c->flags,
		.function = NULL,
		.value    = errorp,
		.next     = NULL
	};
	return so_ctlv(&ctl, s, args);
}

static inline src*
so_ctlsophia(so *e, soctlrt *rt, src **pc)
{
	src *sophia = *pc;
	src *p = NULL;
	sr_clink(&p, sr_c(pc, so_ctlv,            "version",     SR_CSZ|SR_CRO, rt->version));
	sr_clink(&p, sr_c(pc, so_ctlv,            "build",       SR_CSZ|SR_CRO, SR_VERSION_COMMIT));
	sr_clink(&p, sr_c(pc, so_ctlsophia_error, "error",       SR_CSZ|SR_CRO, NULL));
	sr_clink(&p, sr_c(pc, so_ctlv_offline,    "path",        SR_CSZREF,     &e->ctl.path));
	sr_clink(&p, sr_c(pc, so_ctlv_offline,    "path_create", SR_CU32,       &e->ctl.path_create));
	return sr_c(pc, NULL, "sophia", SR_CC, sophia);
}

static inline src*
so_ctlmemory(so *e, soctlrt *rt, src **pc)
{
	src *memory = *pc;
	src *p = NULL;
	sr_clink(&p, sr_c(pc, so_ctlv_offline, "limit", SR_CU64,        &e->ctl.memory_limit));
	sr_clink(&p, sr_c(pc, so_ctlv,         "used",  SR_CU64|SR_CRO, &rt->memory_used));
	return sr_c(pc, NULL, "memory", SR_CC, memory);
}

static inline int
so_ctlcompaction_set(src *c ssunused, srcstmt *s, va_list args)
{
	so *e = s->ptr;
	if (s->op != SR_CSET) {
		sr_error(&e->error, "%s", "bad operation");
		return -1;
	}
	if (ssunlikely(so_statusactive(&e->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	/* validate argument */
	char *arg = va_arg(args, char*);
	uint32_t percent = atoi(arg);
	if (percent > 100) {
		sr_error(&e->error, "%s", "bad argument");
		return -1;
	}
	srzone z;
	memset(&z, 0, sizeof(z));
	z.enable = 1;
	sr_zonemap_set(&e->ctl.zones, percent, &z);
	return 0;
}

static inline src*
so_ctlcompaction(so *e, soctlrt *rt ssunused, src **pc)
{
	src *compaction = *pc;
	src *prev;
	src *p = NULL;
	sr_clink(&p, sr_c(pc, so_ctlv_offline, "node_size", SR_CU32, &e->ctl.node_size));
	sr_clink(&p, sr_c(pc, so_ctlv_offline, "page_size", SR_CU32, &e->ctl.page_size));
	sr_clink(&p, sr_c(pc, so_ctlv_offline, "page_checksum", SR_CU32, &e->ctl.page_checksum));
	prev = p;
	int i = 0;
	for (; i < 11; i++) {
		srzone *z = &e->ctl.zones.zones[i];
		if (! z->enable)
			continue;
		src *zone = *pc;
		p = NULL;
		sr_clink(&p,    sr_c(pc, so_ctlv_offline, "mode",              SR_CU32, &z->mode));
		sr_clink(&p,    sr_c(pc, so_ctlv_offline, "compact_wm",        SR_CU32, &z->compact_wm));
		sr_clink(&p,    sr_c(pc, so_ctlv_offline, "branch_prio",       SR_CU32, &z->branch_prio));
		sr_clink(&p,    sr_c(pc, so_ctlv_offline, "branch_wm",         SR_CU32, &z->branch_wm));
		sr_clink(&p,    sr_c(pc, so_ctlv_offline, "branch_age",        SR_CU32, &z->branch_age));
		sr_clink(&p,    sr_c(pc, so_ctlv_offline, "branch_age_period", SR_CU32, &z->branch_age_period));
		sr_clink(&p,    sr_c(pc, so_ctlv_offline, "branch_age_wm",     SR_CU32, &z->branch_age_wm));
		sr_clink(&p,    sr_c(pc, so_ctlv_offline, "backup_prio",       SR_CU32, &z->backup_prio));
		sr_clink(&p,    sr_c(pc, so_ctlv_offline, "gc_wm",             SR_CU32, &z->gc_wm));
		sr_clink(&p,    sr_c(pc, so_ctlv_offline, "gc_db_prio",        SR_CU32, &z->gc_db_prio));
		sr_clink(&p,    sr_c(pc, so_ctlv_offline, "gc_prio",           SR_CU32, &z->gc_prio));
		sr_clink(&p,    sr_c(pc, so_ctlv_offline, "gc_period",         SR_CU32, &z->gc_period));
		sr_clink(&p,    sr_c(pc, so_ctlv_offline, "async",             SR_CU32, &z->async));
		sr_clink(&prev, sr_c(pc, NULL, z->name, SR_CC, zone));
	}
	return sr_c(pc, so_ctlcompaction_set, "compaction", SR_CC, compaction);
}

static inline int
so_ctlscheduler_trace(src *c, srcstmt *s, va_list args ssunused)
{
	soworker *w = c->value;
	char tracesz[128];
	char *trace;
	int tracelen = ss_tracecopy(&w->trace, tracesz, sizeof(tracesz));
	if (sslikely(tracelen == 0))
		trace = NULL;
	else
		trace = tracesz;
	src ctl = {
		.name     = c->name,
		.flags    = c->flags,
		.function = NULL,
		.value    = trace,
		.next     = NULL
	};
	return so_ctlv(&ctl, s, args);
}

static inline int
so_ctlscheduler_checkpoint(src *c, srcstmt *s, va_list args)
{
	if (s->op != SR_CSET)
		return so_ctlv(c, s, args);
	so *e = s->ptr;
	return so_scheduler_checkpoint(e);
}

static inline int
so_ctlscheduler_on_event(src *c, srcstmt *s, va_list args)
{
	so *e = s->ptr;
	if (s->op != SR_CSET)
		return so_ctlv(c, s, args);
	if (ssunlikely(so_statusactive(&e->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	char *v   = va_arg(args, char*);
	char *arg = va_arg(args, char*);
	int rc = ss_triggerset(&e->ctl.on_event, v);
	if (ssunlikely(rc == -1))
		return -1;
	if (arg) {
		rc = ss_triggersetarg(&e->ctl.on_event, arg);
		if (ssunlikely(rc == -1))
			return -1;
	}
	return 0;
}

static inline int
so_ctlscheduler_gc(src *c, srcstmt *s, va_list args)
{
	if (s->op != SR_CSET)
		return so_ctlv(c, s, args);
	so *e = s->ptr;
	return so_scheduler_gc(e);
}

static inline int
so_ctlscheduler_run(src *c, srcstmt *s, va_list args)
{
	if (s->op != SR_CSET)
		return so_ctlv(c, s, args);
	so *e = s->ptr;
	return so_scheduler_call(e);
}

static inline src*
so_ctlscheduler(so *e, soctlrt *rt, src **pc)
{
	src *scheduler = *pc;
	src *prev;
	src *p = NULL;
	sr_clink(&p, sr_c(pc, so_ctlv_offline,     "threads",             SR_CU32,        &e->ctl.threads));
	sr_clink(&p, sr_c(pc, so_ctlv,             "zone",                SR_CSZ|SR_CRO,  rt->zone));
	sr_clink(&p, sr_c(pc, so_ctlv,             "checkpoint_active",   SR_CU32|SR_CRO, &rt->checkpoint_active));
	sr_clink(&p, sr_c(pc, so_ctlv,             "checkpoint_lsn",      SR_CU64|SR_CRO, &rt->checkpoint_lsn));
	sr_clink(&p, sr_c(pc, so_ctlv,             "checkpoint_lsn_last", SR_CU64|SR_CRO, &rt->checkpoint_lsn_last));
	sr_clink(&p, sr_c(pc, so_ctlscheduler_checkpoint, "checkpoint",   SR_CVOID,       NULL));
	sr_clink(&p, sr_c(pc, so_ctlscheduler_on_event, "on_event",       SR_CVOID,       NULL));
	sr_clink(&p, sr_c(pc, so_ctlv_offline,     "event_on_backup",     SR_CU32,        &e->ctl.event_on_backup));
	sr_clink(&p, sr_c(pc, so_ctlv,             "gc_active",           SR_CU32|SR_CRO, &rt->gc_active));
	sr_clink(&p, sr_c(pc, so_ctlscheduler_gc,  "gc",                  SR_CVOID,       NULL));
	sr_clink(&p, sr_c(pc, so_ctlv,             "reqs",                SR_CU32|SR_CRO, &rt->reqs));
	sr_clink(&p, sr_c(pc, so_ctlscheduler_run, "run",                 SR_CVOID,       NULL));
	prev = p;
	sslist *i;
	ss_listforeach(&e->sched.workers.list, i) {
		soworker *w = sscast(i, soworker, link);
		src *worker = *pc;
		p = NULL;
		sr_clink(&p,    sr_c(pc, so_ctlscheduler_trace, "trace", SR_CSZ|SR_CRO, w));
		sr_clink(&prev, sr_c(pc, NULL, w->name, SR_CC, worker));
	}
	return sr_c(pc, NULL, "scheduler", SR_CC, scheduler);
}

static inline int
so_ctllog_rotate(src *c, srcstmt *s, va_list args)
{
	if (s->op != SR_CSET)
		return so_ctlv(c, s, args);
	so *e = s->ptr;
	return sl_poolrotate(&e->lp);
}

static inline int
so_ctllog_gc(src *c, srcstmt *s, va_list args)
{
	if (s->op != SR_CSET)
		return so_ctlv(c, s, args);
	so *e = s->ptr;
	return sl_poolgc(&e->lp);
}

static inline src*
so_ctllog(so *e, soctlrt *rt, src **pc)
{
	src *log = *pc;
	src *p = NULL;
	sr_clink(&p, sr_c(pc, so_ctlv_offline,  "enable",            SR_CU32,        &e->ctl.log_enable));
	sr_clink(&p, sr_c(pc, so_ctlv_offline,  "path",              SR_CSZREF,      &e->ctl.log_path));
	sr_clink(&p, sr_c(pc, so_ctlv_offline,  "sync",              SR_CU32,        &e->ctl.log_sync));
	sr_clink(&p, sr_c(pc, so_ctlv_offline,  "rotate_wm",         SR_CU32,        &e->ctl.log_rotate_wm));
	sr_clink(&p, sr_c(pc, so_ctlv_offline,  "rotate_sync",       SR_CU32,        &e->ctl.log_rotate_sync));
	sr_clink(&p, sr_c(pc, so_ctllog_rotate, "rotate",            SR_CVOID,       NULL));
	sr_clink(&p, sr_c(pc, so_ctllog_gc,     "gc",                SR_CVOID,       NULL));
	sr_clink(&p, sr_c(pc, so_ctlv,          "files",             SR_CU32|SR_CRO, &rt->log_files));
	sr_clink(&p, sr_c(pc, so_ctlv_offline,  "two_phase_recover", SR_CU32,        &e->ctl.two_phase_recover));
	sr_clink(&p, sr_c(pc, so_ctlv_offline,  "commit_lsn",        SR_CU32,        &e->ctl.commit_lsn));
	return sr_c(pc, NULL, "log", SR_CC, log);
}

static inline src*
so_ctlmetric(so *e ssunused, soctlrt *rt, src **pc)
{
	src *metric = *pc;
	src *p = NULL;
	sr_clink(&p, sr_c(pc, so_ctlv, "dsn",  SR_CU32|SR_CRO, &rt->seq.dsn));
	sr_clink(&p, sr_c(pc, so_ctlv, "nsn",  SR_CU32|SR_CRO, &rt->seq.nsn));
	sr_clink(&p, sr_c(pc, so_ctlv, "bsn",  SR_CU32|SR_CRO, &rt->seq.bsn));
	sr_clink(&p, sr_c(pc, so_ctlv, "lsn",  SR_CU64|SR_CRO, &rt->seq.lsn));
	sr_clink(&p, sr_c(pc, so_ctlv, "lfsn", SR_CU32|SR_CRO, &rt->seq.lfsn));
	sr_clink(&p, sr_c(pc, so_ctlv, "tsn",  SR_CU32|SR_CRO, &rt->seq.tsn));
	return sr_c(pc, NULL, "metric", SR_CC, metric);
}

static inline int
so_ctldb_set(src *c ssunused, srcstmt *s, va_list args)
{
	/* set(db) */
	so *e = s->ptr;
	if (s->op != SR_CSET) {
		sr_error(&e->error, "%s", "bad operation");
		return -1;
	}
	char *name = va_arg(args, char*);
	sodb *db = (sodb*)so_dbmatch(e, name);
	if (ssunlikely(db)) {
		sr_error(&e->error, "database '%s' exists", name);
		return -1;
	}
	db = (sodb*)so_dbnew(e, name);
	if (ssunlikely(db == NULL))
		return -1;
	sr_objlist_add(&e->db, &db->o);
	return 0;
}

static inline int
so_ctldb_get(src *c, srcstmt *s, va_list args ssunused)
{
	/* get(db.name) */
	so *e = s->ptr;
	if (s->op != SR_CGET) {
		sr_error(&e->error, "%s", "bad operation");
		return -1;
	}
	assert(c->ptr != NULL);
	sodb *db = c->ptr;
	so_dbref(db, 0);
	*s->result = db;
	return 0;
}

static inline int
so_ctldb_update(src *c, srcstmt *s, va_list args)
{
	if (s->op != SR_CSET)
		return so_ctlv(c, s, args);
	sodb *db = c->value;
	if (ssunlikely(so_statusactive(&db->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	char *v = va_arg(args, char*);
	/* set update function */
	sfupdatef update = (sfupdatef)(uintptr_t)ss_triggerpointer_of(v);
	if (ssunlikely(update)) {
		v = va_arg(args, char*);
		void *arg = NULL;
		if (v) {
			arg = ss_triggerpointer_of(v);
			if (ssunlikely(arg == NULL))
				return -1;
		}
		sf_updateset(&db->scheme.fmt_update, update, arg);
		return 0;
	}
	return -1;
}

static inline int
so_ctldb_cmp(src *c, srcstmt *s, va_list args)
{
	if (s->op != SR_CSET)
		return so_ctlv(c, s, args);
	sodb *db = c->value;
	so *e = so_of(&db->o);
	if (ssunlikely(so_statusactive(&db->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	char *v = va_arg(args, char*);
	/* set custom comparator */
	srcmpf cmp  = (srcmpf)(uintptr_t)ss_triggerpointer_of(v);
	if (ssunlikely(cmp)) {
		v = va_arg(args, char*);
		void *arg = NULL;
		if (v) {
			arg = ss_triggerpointer_of(v);
			if (ssunlikely(arg == NULL))
				return -1;
		}
		sr_schemesetcmp(&db->scheme.scheme, cmp, arg);
		return 0;
	}
	/* set key type */
	return sr_keyset(sr_schemeof(&db->scheme.scheme, 0), &e->a, v);
}

static inline int
so_ctldb_cmpprefix(src *c, srcstmt *s, va_list args)
{
	if (s->op != SR_CSET)
		return so_ctlv(c, s, args);
	sodb *db = c->value;
	if (ssunlikely(so_statusactive(&db->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	char *v = va_arg(args, char*);
	srcmpf cmp = (srcmpf)(uintptr_t)ss_triggerpointer_of(v);
	if (ssunlikely(cmp == NULL))
		return -1;
	v = va_arg(args, char*);
	void *arg = NULL;
	if (v) {
		arg = ss_triggerpointer_of(v);
		if (ssunlikely(arg == NULL))
			return -1;
	}
	sr_schemesetcmp_prefix(&db->scheme.scheme, cmp, arg);
	return 0;
}

static inline int
so_ctldb_status(src *c, srcstmt *s, va_list args)
{
	sodb *db = c->value;
	char *status = so_statusof(&db->status);
	src ctl = {
		.name     = c->name,
		.flags    = c->flags,
		.function = NULL,
		.value    = status,
		.next     = NULL
	};
	return so_ctlv(&ctl, s, args);
}

static inline int
so_ctldb_branch(src *c, srcstmt *s, va_list args)
{
	if (s->op != SR_CSET)
		return so_ctlv(c, s, args);
	sodb *db = c->value;
	return so_scheduler_branch(db);
}

static inline int
so_ctldb_compact(src *c, srcstmt *s, va_list args)
{
	if (s->op != SR_CSET)
		return so_ctlv(c, s, args);
	sodb *db = c->value;
	return so_scheduler_compact(db);
}

static inline int
so_ctldb_lockdetect(src *c, srcstmt *s, va_list args)
{
	if (s->op != SR_CSET)
		return so_ctlv(c, s, args);
	sotx *tx = va_arg(args, sotx*);
	int rc = sx_deadlock(&tx->t);
	return rc;
}

static inline int
so_ctlv_dboffline(src *c, srcstmt *s, va_list args)
{
	sodb *db = c->ptr;
	if (ssunlikely(s->op == SR_CSET && so_statusactive(&db->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	return so_ctlv(c, s, args);
}

static inline int
so_ctldb_index(src *c ssunused, srcstmt *s, va_list args)
{
	/* set(index, key) */
	sodb *db = c->ptr;
	so *e = so_of(&db->o);
	if (s->op != SR_CSET) {
		sr_error(&e->error, "%s", "bad operation");
		return -1;
	}
	if (ssunlikely(so_statusactive(&db->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	char *name = va_arg(args, char*);
	srkey *part = sr_schemefind(&db->scheme.scheme, name);
	if (ssunlikely(part)) {
		sr_error(&e->error, "keypart '%s' exists", name);
		return -1;
	}
	/* create new key-part */
	part = sr_schemeadd(&db->scheme.scheme, &e->a);
	if (ssunlikely(part == NULL))
		return -1;
	int rc = sr_keysetname(part, &e->a, name);
	if (ssunlikely(rc == -1))
		goto error;
	rc = sr_keyset(part, &e->a, "string");
	if (ssunlikely(rc == -1))
		goto error;
	return 0;
error:
	sr_schemedelete(&db->scheme.scheme, &e->a, part->pos);
	return -1;
}

static inline int
so_ctldb_key(src *c, srcstmt *s, va_list args)
{
	sodb *db = c->ptr;
	so *e = so_of(&db->o);
	if (s->op != SR_CSET)
		return so_ctlv(c, s, args);
	if (ssunlikely(so_statusactive(&db->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	char *path = va_arg(args, char*);
	/* update key-part path */
	srkey *part = sr_schemefind(&db->scheme.scheme, c->name);
	assert(part != NULL);
	return sr_keyset(part, &e->a, path);
}

static inline src*
so_ctldb(so *e, soctlrt *rt ssunused, src **pc)
{
	src *db = NULL;
	src *prev = NULL;
	src *p;
	sslist *i;
	ss_listforeach(&e->db.list, i)
	{
		sodb *o = (sodb*)sscast(i, srobj, link);
		si_profilerbegin(&o->ctl.rtp, &o->index);
		si_profiler(&o->ctl.rtp);
		si_profilerend(&o->ctl.rtp);
		/* database index */
		src *index = *pc;
		p = NULL;
		sr_clink(&p, sr_c(pc, so_ctlv,            "memory_used",      SR_CU64|SR_CRO, &o->ctl.rtp.memory_used));
		sr_clink(&p, sr_c(pc, so_ctlv,            "node_count",       SR_CU32|SR_CRO, &o->ctl.rtp.total_node_count));
		sr_clink(&p, sr_c(pc, so_ctlv,            "node_size",        SR_CU64|SR_CRO, &o->ctl.rtp.total_node_size));
		sr_clink(&p, sr_c(pc, so_ctlv,            "node_origin_size", SR_CU64|SR_CRO, &o->ctl.rtp.total_node_origin_size));
		sr_clink(&p, sr_c(pc, so_ctlv,            "count",            SR_CU64|SR_CRO, &o->ctl.rtp.count));
		sr_clink(&p, sr_c(pc, so_ctlv,            "count_dup",        SR_CU64|SR_CRO, &o->ctl.rtp.count_dup));
		sr_clink(&p, sr_c(pc, so_ctlv,            "read_disk",        SR_CU64|SR_CRO, &o->ctl.rtp.read_disk));
		sr_clink(&p, sr_c(pc, so_ctlv,            "read_cache",       SR_CU64|SR_CRO, &o->ctl.rtp.read_cache));
		sr_clink(&p, sr_c(pc, so_ctlv,            "branch_count",     SR_CU32|SR_CRO, &o->ctl.rtp.total_branch_count));
		sr_clink(&p, sr_c(pc, so_ctlv,            "branch_avg",       SR_CU32|SR_CRO, &o->ctl.rtp.total_branch_avg));
		sr_clink(&p, sr_c(pc, so_ctlv,            "branch_max",       SR_CU32|SR_CRO, &o->ctl.rtp.total_branch_max));
		sr_clink(&p, sr_c(pc, so_ctlv,            "branch_histogram", SR_CSZ|SR_CRO,  o->ctl.rtp.histogram_branch_ptr));
		sr_clink(&p, sr_c(pc, so_ctlv,            "page_count",       SR_CU32|SR_CRO, &o->ctl.rtp.total_page_count));
		sr_clink(&p, sr_c(pc, so_ctldb_cmp,       "cmp",              SR_CVOID,       o));
		sr_clink(&p, sr_c(pc, so_ctldb_cmpprefix, "cmp_prefix",       SR_CVOID,       o));
		sr_clink(&p, sr_c(pc, so_ctldb_update,    "update",           SR_CVOID,       o));
		/* index keys */
		int i = 0;
		while (i < o->scheme.scheme.count) {
			srkey *part = sr_schemeof(&o->scheme.scheme, i);
			sr_clink(&p,  sr_cptr(sr_c(pc, so_ctldb_key, part->name, SR_CSZ, part->path), o));
			i++;
		}
		/* database */
		src *database = *pc;
		p = NULL;
		sr_clink(&p,          sr_c(pc, so_ctlv,              "name",            SR_CSZ|SR_CRO, o->scheme.name));
		sr_clink(&p,  sr_cptr(sr_c(pc, so_ctlv,              "id",              SR_CU32,       &o->scheme.id), o));
		sr_clink(&p,          sr_c(pc, so_ctldb_status,      "status",          SR_CSZ|SR_CRO, o));
		sr_clink(&p,  sr_cptr(sr_c(pc, so_ctlv_dboffline,    "format",          SR_CSZREF,     &o->scheme.fmt_sz), o));
		sr_clink(&p,  sr_cptr(sr_c(pc, so_ctlv_dboffline,    "path",            SR_CSZREF,     &o->scheme.path), o));
		sr_clink(&p,  sr_cptr(sr_c(pc, so_ctlv_dboffline,    "sync",            SR_CU32,       &o->scheme.sync), o));
		sr_clink(&p,  sr_cptr(sr_c(pc, so_ctlv_dboffline,    "compression_key", SR_CU32,       &o->scheme.compression_key), o));
		sr_clink(&p,  sr_cptr(sr_c(pc, so_ctlv_dboffline,    "compression",     SR_CSZREF,     &o->scheme.compression_sz), o));
		sr_clink(&p,          sr_c(pc, so_ctldb_branch,      "branch",          SR_CVOID,      o));
		sr_clink(&p,          sr_c(pc, so_ctldb_compact,     "compact",         SR_CVOID,      o));
		sr_clink(&p,          sr_c(pc, so_ctldb_lockdetect,  "lockdetect",      SR_CVOID,      NULL));
		sr_clink(&p,  sr_cptr(sr_c(pc, so_ctldb_index,       "index",           SR_CC,         index), o));
		sr_clink(&prev, sr_cptr(sr_c(pc, so_ctldb_get, o->scheme.name, SR_CC, database), o));
		if (db == NULL)
			db = prev;
	}
	return sr_c(pc, so_ctldb_set, "db", SR_CC, db);
}

static inline int
so_ctlsnapshot_set(src *c, srcstmt *s, va_list args)
{
	if (s->op != SR_CSET)
		return so_ctlv(c, s, args);
	so *e = s->ptr;
	char *name = va_arg(args, char*);
	uint64_t lsn = sr_seq(&e->seq, SR_LSN);
	/* create snapshot object */
	sosnapshot *snapshot =
		(sosnapshot*)so_snapshotnew(e, lsn, name);
	if (ssunlikely(snapshot == NULL))
		return -1;
	sr_objlist_add(&e->snapshot, &snapshot->o);
	return 0;
}

static inline int
so_ctlsnapshot_setlsn(src *c, srcstmt *s, va_list args)
{
	int rc = so_ctlv(c, s, args);
	if (ssunlikely(rc == -1))
		return -1;
	if (s->op != SR_CSET)
		return  0;
	sosnapshot *snapshot = c->ptr;
	so_snapshotupdate(snapshot);
	return 0;
}

static inline int
so_ctlsnapshot_get(src *c, srcstmt *s, va_list args ssunused)
{
	/* get(snapshot.name) */
	so *e = s->ptr;
	if (s->op != SR_CGET) {
		sr_error(&e->error, "%s", "bad operation");
		return -1;
	}
	assert(c->ptr != NULL);
	*s->result = c->ptr;
	return 0;
}

static inline src*
so_ctlsnapshot(so *e, soctlrt *rt ssunused, src **pc)
{
	src *snapshot = NULL;
	src *prev = NULL;
	sslist *i;
	ss_listforeach(&e->snapshot.list, i)
	{
		sosnapshot *s = (sosnapshot*)sscast(i, srobj, link);
		src *p = sr_cptr(sr_c(pc, so_ctlsnapshot_setlsn, "lsn", SR_CU64, &s->vlsn), s);
		sr_clink(&prev, sr_cptr(sr_c(pc, so_ctlsnapshot_get, s->name, SR_CC, p), s));
		if (snapshot == NULL)
			snapshot = prev;
	}
	return sr_c(pc, so_ctlsnapshot_set, "snapshot", SR_CC, snapshot);
}

static inline int
so_ctlbackup_run(src *c, srcstmt *s, va_list args)
{
	if (s->op != SR_CSET)
		return so_ctlv(c, s, args);
	so *e = s->ptr;
	return so_scheduler_backup(e);
}


static inline src*
so_ctlbackup(so *e, soctlrt *rt, src **pc)
{
	src *backup = *pc;
	src *p = NULL;
	sr_clink(&p, sr_c(pc, so_ctlv_offline, "path", SR_CSZREF, &e->ctl.backup_path));
	sr_clink(&p, sr_c(pc, so_ctlbackup_run, "run", SR_CVOID, NULL));
	sr_clink(&p, sr_c(pc, so_ctlv, "active", SR_CU32|SR_CRO, &rt->backup_active));
	sr_clink(&p, sr_c(pc, so_ctlv, "last", SR_CU32|SR_CRO, &rt->backup_last));
	sr_clink(&p, sr_c(pc, so_ctlv, "last_complete", SR_CU32|SR_CRO, &rt->backup_last_complete));
	return sr_c(pc, NULL, "backup", SR_CC, backup);
}

static inline src*
so_ctldebug(so *e, soctlrt *rt ssunused, src **pc)
{
	src *prev = NULL;
	src *p = NULL;
	prev = p;
	src *ei = *pc;
	sr_clink(&p, sr_c(pc, so_ctlv, "sd_build_0",      SR_CU32, &e->ei.e[0]));
	sr_clink(&p, sr_c(pc, so_ctlv, "sd_build_1",      SR_CU32, &e->ei.e[1]));
	sr_clink(&p, sr_c(pc, so_ctlv, "si_branch_0",     SR_CU32, &e->ei.e[2]));
	sr_clink(&p, sr_c(pc, so_ctlv, "si_compaction_0", SR_CU32, &e->ei.e[3]));
	sr_clink(&p, sr_c(pc, so_ctlv, "si_compaction_1", SR_CU32, &e->ei.e[4]));
	sr_clink(&p, sr_c(pc, so_ctlv, "si_compaction_2", SR_CU32, &e->ei.e[5]));
	sr_clink(&p, sr_c(pc, so_ctlv, "si_compaction_3", SR_CU32, &e->ei.e[6]));
	sr_clink(&p, sr_c(pc, so_ctlv, "si_compaction_4", SR_CU32, &e->ei.e[7]));
	sr_clink(&p, sr_c(pc, so_ctlv, "si_recover_0",    SR_CU32, &e->ei.e[8]));
	sr_clink(&prev, sr_c(pc, so_ctldb_set, "error_injection", SR_CC, ei));
	src *debug = prev;
	return sr_c(pc, NULL, "debug", SR_CC, debug);
}

static src*
so_ctlprepare(so *e, soctlrt *rt, src *c, int serialize)
{
	/* sophia */
	src *pc = c;
	src *sophia     = so_ctlsophia(e, rt, &pc);
	src *memory     = so_ctlmemory(e, rt, &pc);
	src *compaction = so_ctlcompaction(e, rt, &pc);
	src *scheduler  = so_ctlscheduler(e, rt, &pc);
	src *metric     = so_ctlmetric(e, rt, &pc);
	src *log        = so_ctllog(e, rt, &pc);
	src *snapshot   = so_ctlsnapshot(e, rt, &pc);
	src *backup     = so_ctlbackup(e, rt, &pc);
	src *db         = so_ctldb(e, rt, &pc);
	src *debug      = so_ctldebug(e, rt, &pc);

	sophia->next     = memory;
	memory->next     = compaction;
	compaction->next = scheduler;
	scheduler->next  = metric;
	metric->next     = log;
	log->next        = snapshot;
	snapshot->next   = backup;
	backup->next     = db;
	if (! serialize)
		db->next = debug;
	return sophia;
}

static int
so_ctlrt(so *e, soctlrt *rt)
{
	/* sophia */
	snprintf(rt->version, sizeof(rt->version),
	         "%d.%d.%d",
	         SR_VERSION_A - '0',
	         SR_VERSION_B - '0',
	         SR_VERSION_C - '0');

	/* memory */
	rt->memory_used = ss_quotaused(&e->quota);

	/* scheduler */
	ss_mutexlock(&e->sched.lock);
	rt->checkpoint_active    = e->sched.checkpoint;
	rt->checkpoint_lsn_last  = e->sched.checkpoint_lsn_last;
	rt->checkpoint_lsn       = e->sched.checkpoint_lsn;
	rt->backup_active        = e->sched.backup;
	rt->backup_last          = e->sched.backup_last;
	rt->backup_last_complete = e->sched.backup_last_complete;
	rt->gc_active            = e->sched.gc;
	ss_mutexunlock(&e->sched.lock);

	/* requests */
	rt->reqs = so_requestcount(e);

	ss_mutexlock(&e->reqlock);
	rt->reqs = e->req.n + e->reqactive.n + e->reqready.n;
	ss_mutexunlock(&e->reqlock);

	int v = ss_quotaused_percent(&e->quota);
	srzone *z = sr_zonemap(&e->ctl.zones, v);
	memcpy(rt->zone, z->name, sizeof(rt->zone));

	/* log */
	rt->log_files = sl_poolfiles(&e->lp);

	/* metric */
	sr_seqlock(&e->seq);
	rt->seq = e->seq;
	sr_sequnlock(&e->seq);
	return 0;
}

static int
so_ctlset(srobj *obj, va_list args)
{
	so *e = so_of(obj);
	soctlrt rt;
	so_ctlrt(e, &rt);
	src ctl[1024];
	src *root;
	root = so_ctlprepare(e, &rt, ctl, 0);
	char *path = va_arg(args, char*);
	srcstmt stmt = {
		.op        = SR_CSET,
		.path      = path,
		.serialize = NULL,
		.result    = NULL,
		.ptr       = e,
		.r         = &e->r
	};
	return sr_cexecv(root, &stmt, args);
}

static void*
so_ctlget(srobj *obj, va_list args)
{
	so *e = so_of(obj);
	soctlrt rt;
	so_ctlrt(e, &rt);
	src ctl[1024];
	src *root;
	root = so_ctlprepare(e, &rt, ctl, 0);
	char *path   = va_arg(args, char*);
	void *result = NULL;
	srcstmt stmt = {
		.op        = SR_CGET,
		.path      = path,
		.serialize = NULL,
		.result    = &result,
		.ptr       = e,
		.r         = &e->r
	};
	int rc = sr_cexecv(root, &stmt, args);
	if (ssunlikely(rc == -1))
		return NULL;
	return result;
}

int so_ctlserialize(soctl *c, ssbuf *buf)
{
	so *e = so_of(&c->o);
	soctlrt rt;
	so_ctlrt(e, &rt);
	src ctl[1024];
	src *root;
	root = so_ctlprepare(e, &rt, ctl, 1);
	srcstmt stmt = {
		.op        = SR_CSERIALIZE,
		.path      = NULL,
		.serialize = buf,
		.result    = NULL,
		.ptr       = e,
		.r         = &e->r
	};
	return sr_cexec(root, &stmt);
}

static void*
so_ctlcursor(srobj *o, va_list args ssunused)
{
	so *e = so_of(o);
	return so_ctlcursor_new(e);
}

static void*
so_ctltype(srobj *o ssunused, va_list args ssunused) {
	return "ctl";
}

static srobjif soctlif =
{
	.ctl      = NULL,
	.async    = NULL,
	.open     = NULL,
	.destroy  = NULL,
	.error    = NULL,
	.set      = so_ctlset,
	.update   = NULL,
	.del      = NULL,
	.get      = so_ctlget,
	.poll     = NULL,
	.drop     = NULL,
	.begin    = NULL,
	.prepare  = NULL,
	.commit   = NULL,
	.cursor   = so_ctlcursor,
	.object   = NULL,
	.type     = so_ctltype
};

void so_ctlinit(soctl *c, void *e)
{
	so *o = e;
	sr_objinit(&c->o, SOCTL, &soctlif, e);
	sr_schemeinit(&c->ctlscheme);
	srkey *part = sr_schemeadd(&c->ctlscheme, &o->a);
	sr_keysetname(part, &o->a, "key");
	sr_keyset(part, &o->a, "string");
	c->path                = NULL;
	c->path_create         = 1;
	c->memory_limit        = 0;
	c->node_size           = 64 * 1024 * 1024;
	c->page_size           = 64 * 1024;
	c->page_checksum       = 1;
	c->threads             = 6;
	c->log_enable          = 1;
	c->log_path            = NULL;
	c->log_rotate_wm       = 500000;
	c->log_sync            = 0;
	c->log_rotate_sync     = 1;
	c->two_phase_recover   = 0;
	c->commit_lsn          = 0;
	ss_triggerinit(&c->on_event);
	c->event_on_backup     = 0;
	srzone def = {
		.enable        = 1,
		.mode          = 3, /* branch + compact */
		.compact_wm    = 2,
		.branch_prio   = 1,
		.branch_wm     = 10 * 1024 * 1024,
		.branch_age    = 40,
		.branch_age_period = 40,
		.branch_age_wm = 1 * 1024 * 1024,
		.backup_prio   = 1,
		.gc_db_prio    = 1,
		.gc_prio       = 1,
		.gc_period     = 60,
		.gc_wm         = 30,
		.async         = 2 /* do not own thread */
	};
	srzone redzone = {
		.enable        = 1,
		.mode          = 2, /* checkpoint */
		.compact_wm    = 4,
		.branch_prio   = 0,
		.branch_wm     = 0,
		.branch_age    = 0,
		.branch_age_period = 0,
		.branch_age_wm = 0,
		.backup_prio   = 0,
		.gc_db_prio    = 0,
		.gc_prio       = 0,
		.gc_period     = 0,
		.gc_wm         = 0,
		.async         = 2
	};
	sr_zonemap_set(&o->ctl.zones,  0, &def);
	sr_zonemap_set(&o->ctl.zones, 80, &redzone);
	c->backup_path = NULL;
}

void so_ctlfree(soctl *c)
{
	so *e = so_of(&c->o);
	if (c->path) {
		ss_free(&e->a, c->path);
		c->path = NULL;
	}
	if (c->log_path) {
		ss_free(&e->a, c->log_path);
		c->log_path = NULL;
	}
	if (c->backup_path) {
		ss_free(&e->a, c->backup_path);
		c->backup_path = NULL;
	}
	sr_schemefree(&c->ctlscheme, &e->a);
}

int so_ctlvalidate(soctl *c)
{
	so *e = so_of(&c->o);
	if (c->path == NULL) {
		sr_error(&e->error, "%s", "repository path is not set");
		return -1;
	}
	char path[1024];
	if (c->log_path == NULL) {
		snprintf(path, sizeof(path), "%s/log", c->path);
		c->log_path = ss_strdup(&e->a, path);
		if (ssunlikely(c->log_path == NULL)) {
			return sr_oom(&e->error);
		}
	}
	int i = 0;
	for (; i < 11; i++) {
		srzone *z = &e->ctl.zones.zones[i];
		if (! z->enable)
			continue;
		if (z->compact_wm <= 1) {
			sr_error(&e->error, "bad %d.compact_wm value", i * 10);
			return -1;
		}
	}
	return 0;
}
