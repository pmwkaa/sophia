
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsx.h>
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
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
		size = sizeof(function_sz);
		break;
	}
	case SR_CC: assert(0);
		break;
	}
	if (value)
		size++;
	svlocal l;
	l.lsn       = 0;
	l.flags     = 0;
	l.keysize   = strlen(c->name) + 1;
	l.key       = c->name;
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
	sov *result = (sov*)so_vnew(e, NULL);
	if (srunlikely(result == NULL)) {
		sv_vfree(&e->a, v);
		sr_error(&e->error, "%s", "memory allocation failed");
		sr_error_recoverable(&e->error);
		return NULL;
	}
	svinit(&vp, &sv_vif, v, NULL);
	return so_vput(result, &vp);
}

static inline int
so_ctlv(src *c, srcstmt *s, va_list args)
{
	switch (s->op) {
	case SR_CGET: {
		void *ret = so_ctlreturn(c, s->ptr);
		if ((srunlikely(ret == NULL)))
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
	if (srunlikely(s->op == SR_CSET && so_statusactive(&e->status))) {
		sr_error(s->r->e, "write to %s is offline-only",
		         s->path);
		sr_error_recoverable(s->r->e);
		return -1;
	}
	return so_ctlv(c, s, args);
}

static inline int
so_ctlsophia_error(src *c, srcstmt *s, va_list args srunused)
{
	so *e = s->ptr;
	char *errorp;
	char  error[128];
	error[0] = 0;
	int len = sr_errorcopy(&e->error, error, sizeof(error));
	if (srlikely(len == 0))
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
	sr_clink(&p, sr_c(pc, so_ctlv,            "version", SR_CSZ|SR_CRO, rt->version));
	sr_clink(&p, sr_c(pc, so_ctlv,            "build",   SR_CSZ|SR_CRO, SR_VERSION_COMMIT));
	sr_clink(&p, sr_c(pc, so_ctlsophia_error, "error",   SR_CSZ|SR_CRO, NULL));
	sr_clink(&p, sr_c(pc, so_ctlv_offline,    "path",    SR_CSZREF,     &e->ctl.path));
	return sr_c(pc, NULL, "sophia", SR_CC, sophia);
}

static inline src*
so_ctlmemory(so *e, soctlrt *rt, src **pc)
{
	src *memory = *pc;
	src *p = NULL;
	sr_clink(&p, sr_c(pc, so_ctlv_offline, "limit",           SR_CU64,        &e->ctl.memory_limit));
	sr_clink(&p, sr_c(pc, so_ctlv,         "used",            SR_CU64|SR_CRO, &rt->memory_used));
	sr_clink(&p, sr_c(pc, so_ctlv,         "pager_pool_size", SR_CU32|SR_CRO, &e->pager.pool_size));
	sr_clink(&p, sr_c(pc, so_ctlv,         "pager_page_size", SR_CU32|SR_CRO, &e->pager.page_size));
	sr_clink(&p, sr_c(pc, so_ctlv,         "pager_pools",     SR_CU32|SR_CRO, &e->pager.pools));
	return sr_c(pc, NULL, "memory", SR_CC, memory);
}

static inline int
so_ctlcompaction_set(src *c srunused, srcstmt *s, va_list args)
{
	so *e = s->ptr;
	if (s->op != SR_CSET) {
		sr_error(&e->error, "%s", "bad operation");
		sr_error_recoverable(&e->error);
		return -1;
	}
	if (srunlikely(so_statusactive(&e->status))) {
		sr_error(s->r->e, "write to %s is offline-only",
		         s->path);
		sr_error_recoverable(s->r->e);
		return -1;
	}
	/* validate argument */
	char *arg = va_arg(args, char*);
	uint32_t percent = atoi(arg);
	if (percent > 100) {
		sr_error(&e->error, "%s", "bad argument");
		sr_error_recoverable(&e->error);
		return -1;
	}
	sizone z;
	memset(&z, 0, sizeof(z));
	z.enable = 1;
	si_zonemap_set(&e->ctl.zones, percent, &z);
	return 0;
}

static inline src*
so_ctlcompaction(so *e, soctlrt *rt srunused, src **pc)
{
	src *compaction = *pc;
	src *prev;
	src *p = NULL;
	sr_clink(&p, sr_c(pc, so_ctlv_offline, "node_size", SR_CU32, &e->ctl.node_size));
	sr_clink(&p, sr_c(pc, so_ctlv_offline, "page_size", SR_CU32, &e->ctl.page_size));
	prev = p;
	int i = 0;
	for (; i < 11; i++) {
		sizone *z = &e->ctl.zones.zones[i];
		if (! z->enable)
			continue;
		src *zone = *pc;
		p = NULL;
		sr_clink(&p,    sr_c(pc, so_ctlv_offline, "mode",          SR_CU32, &z->mode));
		sr_clink(&p,    sr_c(pc, so_ctlv_offline, "compact_wm",    SR_CU32, &z->compact_wm));
		sr_clink(&p,    sr_c(pc, so_ctlv_offline, "branch_prio",   SR_CU32, &z->branch_prio));
		sr_clink(&p,    sr_c(pc, so_ctlv_offline, "branch_wm",     SR_CU32, &z->branch_wm));
		sr_clink(&p,    sr_c(pc, so_ctlv_offline, "branch_ttl",    SR_CU32, &z->branch_ttl));
		sr_clink(&p,    sr_c(pc, so_ctlv_offline, "branch_ttl_wm", SR_CU32, &z->branch_ttl_wm));
		sr_clink(&prev, sr_c(pc, NULL, z->name, SR_CC, zone));
	}
	return sr_c(pc, so_ctlcompaction_set, "compaction", SR_CC, compaction);
}

static inline int
so_ctlscheduler_trace(src *c, srcstmt *s, va_list args srunused)
{
	soworker *w = c->value;
	char tracesz[128];
	char *trace;
	int tracelen = sr_tracecopy(&w->trace, tracesz, sizeof(tracesz));
	if (srlikely(tracelen == 0))
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
	sr_clink(&p, sr_c(pc, so_ctlv_offline, "threads",             SR_CU32,        &e->ctl.threads));
	sr_clink(&p, sr_c(pc, so_ctlv,         "zone",                SR_CSZ|SR_CRO,  rt->zone));
	sr_clink(&p, sr_c(pc, so_ctlv,         "checkpoint_active",   SR_CU32|SR_CRO, &rt->checkpoint_active));
	sr_clink(&p, sr_c(pc, so_ctlv,         "checkpoint_lsn",      SR_CU64|SR_CRO, &rt->checkpoint_lsn));
	sr_clink(&p, sr_c(pc, so_ctlv,         "checkpoint_lsn_last", SR_CU64|SR_CRO, &rt->checkpoint_lsn_last));
	sr_clink(&p, sr_c(pc, so_ctlscheduler_checkpoint, "checkpoint", SR_CVOID, NULL));
	sr_clink(&p, sr_c(pc, so_ctlscheduler_run,        "run",        SR_CVOID, NULL));
	prev = p;
	srlist *i;
	sr_listforeach(&e->sched.workers.list, i) {
		soworker *w = srcast(i, soworker, link);
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

static inline int
so_ctldb_set(src *c srunused, srcstmt *s, va_list args)
{
	/* set(db) */
	so *e = s->ptr;
	if (s->op != SR_CSET) {
		sr_error(&e->error, "%s", "bad operation");
		sr_error_recoverable(&e->error);
		return -1;
	}
	char *name = va_arg(args, char*);
	sodb *db = (sodb*)so_dbmatch(e, name);
	if (srunlikely(db)) {
		sr_error(&e->error, "database '%s' is exists", name);
		sr_error_recoverable(&e->error);
		return -1;
	}
	db = (sodb*)so_dbnew(e, name);
	if (srunlikely(db == NULL))
		return -1;
	so_objindex_register(&e->db, &db->o);
	return 0;
}

static inline int
so_ctldb_get(src *c, srcstmt *s, va_list args srunused)
{
	/* get(db.name) */
	so *e = s->ptr;
	if (s->op != SR_CGET) {
		sr_error(&e->error, "%s", "bad operation");
		sr_error_recoverable(&e->error);
		return -1;
	}
	assert(c->ptr != NULL);
	*s->result = c->ptr;
	return 0;
}

static inline int
so_ctldb_cmp(src *c, srcstmt *s, va_list args)
{
	if (s->op != SR_CSET)
		return so_ctlv(c, s, args);
	sodb *db = c->value;
	if (srunlikely(so_statusactive(&db->status))) {
		sr_error(s->r->e, "write to %s is offline-only",
		         s->path);
		sr_error_recoverable(s->r->e);
		return -1;
	}
	db->ctl.cmp.cmp = va_arg(args, srcmpf);
	return 0;
}

static inline int
so_ctldb_cmparg(src *c, srcstmt *s, va_list args)
{
	if (s->op != SR_CSET)
		return so_ctlv(c, s, args);
	sodb *db = c->value;
	if (srunlikely(so_statusactive(&db->status))) {
		sr_error(s->r->e, "write to %s is offline-only",
		         s->path);
		sr_error_recoverable(s->r->e);
		return -1;
	}
	db->ctl.cmp.cmparg = va_arg(args, void*);
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
	if (srunlikely(s->op == SR_CSET && so_statusactive(&db->status))) {
		sr_error(s->r->e, "write to %s is offline-only",
		         s->path);
		sr_error_recoverable(s->r->e);
		return -1;
	}
	return so_ctlv(c, s, args);
}

static inline src*
so_ctldb(so *e, soctlrt *rt srunused, src **pc)
{
	src *db = NULL;
	src *prev = NULL;
	src *p;
	srlist *i;
	sr_listforeach(&e->db.list, i)
	{
		sodb *o = (sodb*)srcast(i, soobj, link);
		si_profilerbegin(&o->ctl.rtp, &o->index);
		si_profiler(&o->ctl.rtp, &o->r);
		si_profilerend(&o->ctl.rtp);
		src *index = *pc;
		p = NULL;
		sr_clink(&p, sr_c(pc, so_ctldb_cmp,    "cmp",              SR_CVOID,       o));
		sr_clink(&p, sr_c(pc, so_ctldb_cmparg, "cmp_arg",          SR_CVOID,       o));
		sr_clink(&p, sr_c(pc, so_ctlv,         "node_count",       SR_CU32|SR_CRO, &o->ctl.rtp.total_node_count));
		sr_clink(&p, sr_c(pc, so_ctlv,         "node_size",        SR_CU64|SR_CRO, &o->ctl.rtp.total_node_size));
		sr_clink(&p, sr_c(pc, so_ctlv,         "branch_count",     SR_CU32|SR_CRO, &o->ctl.rtp.total_branch_count));
		sr_clink(&p, sr_c(pc, so_ctlv,         "branch_avg",       SR_CU32|SR_CRO, &o->ctl.rtp.total_branch_avg));
		sr_clink(&p, sr_c(pc, so_ctlv,         "branch_max",       SR_CU32|SR_CRO, &o->ctl.rtp.total_branch_max));
		sr_clink(&p, sr_c(pc, so_ctlv,         "memory_used",      SR_CU64|SR_CRO, &o->ctl.rtp.memory_used));
		sr_clink(&p, sr_c(pc, so_ctlv,         "read_disk",        SR_CU64|SR_CRO, &o->ctl.rtp.read_disk));
		sr_clink(&p, sr_c(pc, so_ctlv,         "read_cache",       SR_CU64|SR_CRO, &o->ctl.rtp.read_cache));
		sr_clink(&p, sr_c(pc, so_ctlv,         "count",            SR_CU64|SR_CRO, &o->ctl.rtp.count));
		sr_clink(&p, sr_c(pc, so_ctlv,         "seq_dsn",          SR_CU32|SR_CRO, &o->ctl.rtp.seq.dsn));
		sr_clink(&p, sr_c(pc, so_ctlv,         "seq_nsn",          SR_CU32|SR_CRO, &o->ctl.rtp.seq.nsn));
		sr_clink(&p, sr_c(pc, so_ctlv,         "seq_lsn",          SR_CU64|SR_CRO, &o->ctl.rtp.seq.lsn));
		sr_clink(&p, sr_c(pc, so_ctlv,         "seq_lfsn",         SR_CU32|SR_CRO, &o->ctl.rtp.seq.lfsn));
		sr_clink(&p, sr_c(pc, so_ctlv,         "seq_tsn",          SR_CU32|SR_CRO, &o->ctl.rtp.seq.tsn));
		sr_clink(&p, sr_c(pc, so_ctlv,         "histogram_branch", SR_CSZ|SR_CRO,  o->ctl.rtp.histogram_branch_ptr));
		src *database = *pc;
		p = NULL;
		sr_clink(&p,          sr_c(pc, so_ctlv,             "name",       SR_CSZ|SR_CRO,  o->ctl.name));
		sr_clink(&p,  sr_cptr(sr_c(pc, so_ctlv,             "id",         SR_CU32,        &o->ctl.id), o));
		sr_clink(&p,          sr_c(pc, so_ctldb_status,     "status",     SR_CSZ|SR_CRO,  o));
		sr_clink(&p,  sr_cptr(sr_c(pc, so_ctlv_dboffline,   "path",       SR_CSZREF,      &o->ctl.path), o));
		sr_clink(&p,  sr_cptr(sr_c(pc, so_ctlv_dboffline,   "sync",       SR_CU32,        &o->ctl.sync), o));
		sr_clink(&p,          sr_c(pc, so_ctldb_branch,     "branch",     SR_CVOID,       o));
		sr_clink(&p,          sr_c(pc, so_ctldb_compact,    "compact",    SR_CVOID,       o));
		sr_clink(&p,          sr_c(pc, so_ctldb_lockdetect, "lockdetect", SR_CVOID,       NULL));
		sr_clink(&p,          sr_c(pc, NULL,                "index",      SR_CC,          index));
		sr_clink(&prev, sr_cptr(sr_c(pc, so_ctldb_get, o->ctl.name, SR_CC, database), o));
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
	if (srunlikely(snapshot == NULL))
		return -1;
	so_objindex_register(&e->snapshot, &snapshot->o);
	return 0;
}

static inline int
so_ctlsnapshot_setlsn(src *c, srcstmt *s, va_list args)
{
	int rc = so_ctlv(c, s, args);
	if (srunlikely(rc == -1))
		return -1;
	if (s->op != SR_CSET)
		return  0;
	sosnapshot *snapshot = c->ptr;
	so_snapshotupdate(snapshot);
	return 0;
}

static inline int
so_ctlsnapshot_get(src *c, srcstmt *s, va_list args srunused)
{
	/* get(snapshot.name) */
	so *e = s->ptr;
	if (s->op != SR_CGET) {
		sr_error(&e->error, "%s", "bad operation");
		sr_error_recoverable(&e->error);
		return -1;
	}
	assert(c->ptr != NULL);
	*s->result = c->ptr;
	return 0;
}

static inline src*
so_ctlsnapshot(so *e, soctlrt *rt srunused, src **pc)
{
	src *snapshot = NULL;
	src *prev = NULL;
	srlist *i;
	sr_listforeach(&e->snapshot.list, i)
	{
		sosnapshot *s = (sosnapshot*)srcast(i, soobj, link);
		src *p = sr_cptr(sr_c(pc, so_ctlsnapshot_setlsn, "lsn", SR_CU64, &s->vlsn), s);
		sr_clink(&prev, sr_cptr(sr_c(pc, so_ctlsnapshot_get, s->name, SR_CC, p), s));
		if (snapshot == NULL)
			snapshot = prev;
	}
	return sr_c(pc, so_ctlsnapshot_set, "snapshot", SR_CC, snapshot);
}

static inline src*
so_ctldebug(so *e, soctlrt *rt srunused, src **pc)
{
	src *debug = *pc;
	src *prev = NULL;
	src *p = NULL;
	sr_clink(&p, sr_c(pc, so_ctlv_offline, "disable_snapshot", SR_CU32, &e->ctl.disable_snapshot));
	prev = p;
	p = NULL;
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
	sr_clink(&p, sr_c(pc, so_ctlv, "se_snapshot_0",   SR_CU32, &e->ei.e[9]));
	sr_clink(&p, sr_c(pc, so_ctlv, "se_snapshot_1",   SR_CU32, &e->ei.e[10]));
	sr_clink(&p, sr_c(pc, so_ctlv, "se_snapshot_2",   SR_CU32, &e->ei.e[11]));
	sr_clink(&p, sr_c(pc, so_ctlv, "se_snapshot_3",   SR_CU32, &e->ei.e[12]));
	sr_clink(&prev, sr_c(pc, so_ctldb_set, "error_injection", SR_CC, ei));
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
	src *log        = so_ctllog(e, rt, &pc);
	src *snapshot   = so_ctlsnapshot(e, rt, &pc);
	src *db         = so_ctldb(e, rt, &pc);
	src *debug      = so_ctldebug(e, rt, &pc);

	sophia->next     = memory;
	memory->next     = compaction;
	compaction->next = scheduler;
	scheduler->next  = log;
	log->next        = snapshot;
	snapshot->next   = db;
	if (! serialize)
		db->next = debug;
	return sophia;
}

static int
so_ctlrt(so *e, soctlrt *rt)
{
	/* sophia */
	snprintf(rt->version, sizeof(rt->version),
	         "%d.%d",
	         SR_VERSION_MAJOR - '0',
	         SR_VERSION_MINOR - '0');

	/* memory */
	rt->memory_used = sr_quotaused(&e->quota);

	/* scheduler */
	sr_mutexlock(&e->sched.lock);
	rt->checkpoint_active   = e->sched.checkpoint;
	rt->checkpoint_lsn_last = e->sched.checkpoint_lsn_last;
	rt->checkpoint_lsn      = e->sched.checkpoint_lsn;
	sr_mutexunlock(&e->sched.lock);

	int v = sr_quotaused_percent(&e->quota);
	sizone *z = si_zonemap(&e->ctl.zones, v);
	memcpy(rt->zone, z->name, sizeof(rt->zone));

	/* log */
	rt->log_files = sl_poolfiles(&e->lp);
	return 0;
}

static int
so_ctlset(soobj *obj, va_list args)
{
	soctl *c = (soctl*)obj;
	so *e = c->e;
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
so_ctlget(soobj *obj, va_list args)
{
	soctl *c = (soctl*)obj;
	so *e = c->e;
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
	if (srunlikely(rc == -1))
		return NULL;
	return result;
}

int so_ctlserialize(soctl *c, srbuf *buf)
{
	so *e = c->e;
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

	c->path              = NULL;
	c->memory_limit      = 0;
	c->node_size         = 64 * 1024 * 1024;
	c->page_size         = 64 * 1024;
	c->threads           = 5;
	c->log_enable        = 1;
	c->log_path          = NULL;
	c->log_rotate_wm     = 500000;
	c->log_sync          = 0;
	c->log_rotate_sync   = 1;
	c->two_phase_recover = 0;
	c->commit_lsn        = 0;
	c->disable_snapshot  = 0;
	c->e                 = e;
	sizone def = {
		.enable        = 1,
		.mode          = 3, /* branch + compact */
		.compact_wm    = 2,
		.branch_prio   = 1,
		.branch_wm     = 10 * 1024 * 1024,
		.branch_ttl    = 40,
		.branch_ttl_wm = 1 * 1024 * 1024
	};
	sizone redzone = {
		.enable        = 1,
		.mode          = 2, /* checkpoint */
		.compact_wm    = 4,
		.branch_prio   = 0,
		.branch_wm     = 0,
		.branch_ttl    = 0,
		.branch_ttl_wm = 0
	};
	si_zonemap_set(&o->ctl.zones,  0, &def);
	si_zonemap_set(&o->ctl.zones, 80, &redzone);
}

void so_ctlfree(soctl *c)
{
	so *e = c->e;
	if (c->path) {
		sr_free(&e->a, c->path);
		c->path = NULL;
	}
	if (c->log_path) {
		sr_free(&e->a, c->log_path);
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
	int i = 0;
	for (; i < 11; i++) {
		sizone *z = &e->ctl.zones.zones[i];
		if (! z->enable)
			continue;
		if (z->compact_wm <= 1) {
			sr_error(&e->error, "bad %d.compact_wm value", i * 10);
			sr_error_recoverable(&e->error);
			return -1;
		}
	}
	return 0;
}
