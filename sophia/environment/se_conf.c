
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

static inline int
se_confv(srconf *c, srconfstmt *s)
{
	switch (s->op) {
	case SR_SERIALIZE: return sr_conf_serialize(c, s);
	case SR_READ:      return sr_conf_read(c, s);
	case SR_WRITE:     return sr_conf_write(c, s);
	}
	assert(0);
	return -1;
}

static inline int
se_confv_offline(srconf *c, srconfstmt *s)
{
	se *e = s->ptr;
	if (s->op == SR_WRITE) {
		if (sr_status(&e->status)) {
			sr_error(s->r->e, "write to %s is offline-only", s->path);
			return -1;
		}
	}
	return se_confv(c, s);
}

static inline int
se_confsophia_error(srconf *c, srconfstmt *s)
{
	se *e = s->ptr;
	char *errorp;
	char  error[128];
	error[0] = 0;
	int len = sr_errorcopy(&e->error, error, sizeof(error));
	if (sslikely(len == 0))
		errorp = NULL;
	else
		errorp = error;
	srconf conf = {
		.key      = c->key,
		.flags    = c->flags,
		.type     = c->type,
		.function = NULL,
		.value    = errorp,
		.ptr      = NULL,
		.next     = NULL
	};
	return se_confv(&conf, s);
}

static inline srconf*
se_confsophia(se *e, seconfrt *rt, srconf **pc)
{
	srconf *sophia = *pc;
	srconf *p = NULL;
	sr_C(&p, pc, se_confv, "version", SS_STRING, rt->version, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "version_storage", SS_STRING, rt->version_storage, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "build", SS_STRING, rt->build, SR_RO, NULL);
	sr_C(&p, pc, se_confsophia_error, "error", SS_STRING, NULL, SR_RO, NULL);
	sr_c(&p, pc, se_confv_offline, "path", SS_STRINGPTR, &e->conf.path);
	sr_c(&p, pc, se_confv_offline, "path_create", SS_U32, &e->conf.path_create);
	sr_c(&p, pc, se_confv_offline, "recover", SS_U32, &e->conf.recover);
	return sr_C(NULL, pc, NULL, "sophia", SS_UNDEF, sophia, SR_NS, NULL);
}

static inline srconf*
se_confmemory(se *e, seconfrt *rt, srconf **pc)
{
	srconf *memory = *pc;
	srconf *p = NULL;
	sr_c(&p, pc, se_confv_offline, "limit", SS_U64, &e->conf.memory_limit);
	sr_C(&p, pc, se_confv, "used", SS_U64, &rt->memory_used, SR_RO, NULL);
	sr_c(&p, pc, se_confv_offline, "anticache", SS_U64, &e->conf.anticache);
	sr_C(&p, pc, se_confv, "pager_pools", SS_U32, &rt->pager_pools, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "pager_pool_size", SS_U32, &rt->pager_pool_size, SR_RO, NULL);
	return sr_C(NULL, pc, NULL, "memory", SS_UNDEF, memory, SR_NS, NULL);
}

static inline int
se_confcompaction_set(srconf *c ssunused, srconfstmt *s)
{
	se *e = s->ptr;
	if (s->op != SR_WRITE) {
		sr_error(&e->error, "%s", "bad operation");
		return -1;
	}
	if (ssunlikely(sr_statusactive(&e->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	/* validate argument */
	uint32_t percent = sscastu32(s->value);
	if (percent > 100) {
		sr_error(&e->error, "%s", "bad argument");
		return -1;
	}
	srzone z;
	memset(&z, 0, sizeof(z));
	z.enable = 1;
	sr_zonemap_set(&e->conf.zones, percent, &z);
	return 0;
}

static inline srconf*
se_confcompaction(se *e, seconfrt *rt ssunused, srconf **pc)
{
	srconf *compaction = NULL;
	srconf *prev = NULL;
	srconf *p;
	int i = 0;
	for (; i < 11; i++) {
		srzone *z = &e->conf.zones.zones[i];
		if (! z->enable)
			continue;
		srconf *zone = *pc;
		p = NULL;
		sr_c(&p, pc, se_confv_offline, "mode", SS_U32, &z->mode);
		sr_c(&p, pc, se_confv_offline, "compact_wm", SS_U32, &z->compact_wm);
		sr_c(&p, pc, se_confv_offline, "compact_mode", SS_U32, &z->compact_mode);
		sr_c(&p, pc, se_confv_offline, "branch_prio", SS_U32, &z->branch_prio);
		sr_c(&p, pc, se_confv_offline, "branch_wm", SS_U32, &z->branch_wm);
		sr_c(&p, pc, se_confv_offline, "branch_age", SS_U32, &z->branch_age);
		sr_c(&p, pc, se_confv_offline, "branch_age_period", SS_U32, &z->branch_age_period);
		sr_c(&p, pc, se_confv_offline, "branch_age_wm", SS_U32, &z->branch_age_wm);
		sr_c(&p, pc, se_confv_offline, "anticache_period", SS_U32, &z->anticache_period);
		sr_c(&p, pc, se_confv_offline, "snapshot_period", SS_U32, &z->snapshot_period);
		sr_c(&p, pc, se_confv_offline, "backup_prio", SS_U32, &z->backup_prio);
		sr_c(&p, pc, se_confv_offline, "gc_wm", SS_U32, &z->gc_wm);
		sr_c(&p, pc, se_confv_offline, "gc_prio", SS_U32, &z->gc_prio);
		sr_c(&p, pc, se_confv_offline, "gc_period", SS_U32, &z->gc_period);
		sr_c(&p, pc, se_confv_offline, "lru_prio", SS_U32, &z->lru_prio);
		sr_c(&p, pc, se_confv_offline, "lru_period", SS_U32, &z->lru_period);
		sr_c(&p, pc, se_confv_offline, "async", SS_U32, &z->async);
		prev = sr_C(&prev, pc, NULL, z->name, SS_UNDEF, zone, SR_NS, NULL);
		if (compaction == NULL)
			compaction = prev;
	}
	return sr_C(NULL, pc, se_confcompaction_set, "compaction", SS_U32,
	            compaction, SR_NS, NULL);
}

static inline int
se_confscheduler_trace(srconf *c, srconfstmt *s)
{
	scworker *w = c->value;
	char tracesz[128];
	char *trace;
	int tracelen = ss_tracecopy(&w->trace, tracesz, sizeof(tracesz));
	if (sslikely(tracelen == 0))
		trace = NULL;
	else
		trace = tracesz;
	srconf conf = {
		.key      = c->key,
		.flags    = c->flags,
		.type     = c->type,
		.function = NULL,
		.value    = trace,
		.ptr      = NULL,
		.next     = NULL
	};
	return se_confv(&conf, s);
}

static inline int
se_confscheduler_checkpoint(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	se *e = s->ptr;
	return sc_ctl_checkpoint(&e->scheduler);
}

static inline int
se_confscheduler_snapshot(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	se *e = s->ptr;
	return sc_ctl_snapshot(&e->scheduler);
}

static inline int
se_confscheduler_anticache(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	se *e = s->ptr;
	return sc_ctl_anticache(&e->scheduler);
}

static inline int
se_confscheduler_on_recover(srconf *c, srconfstmt *s)
{
	se *e = s->ptr;
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	if (ssunlikely(sr_statusactive(&e->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	e->conf.on_recover.function =
		(serecovercbf)(uintptr_t)s->value;
	return 0;
}

static inline int
se_confscheduler_on_recover_arg(srconf *c, srconfstmt *s)
{
	se *e = s->ptr;
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	if (ssunlikely(sr_statusactive(&e->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	e->conf.on_recover.arg = s->value;
	return 0;
}

static inline int
se_confscheduler_on_event(srconf *c, srconfstmt *s)
{
	se *e = s->ptr;
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	if (ssunlikely(sr_statusactive(&e->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	ss_triggerset(&e->conf.on_event, s->value);
	return 0;
}

static inline int
se_confscheduler_on_event_arg(srconf *c, srconfstmt *s)
{
	se *e = s->ptr;
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	if (ssunlikely(sr_statusactive(&e->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	ss_triggerset_arg(&e->conf.on_event, s->value);
	return 0;
}

static inline int
se_confscheduler_gc(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	se *e = s->ptr;
	return sc_ctl_gc(&e->scheduler);
}

static inline int
se_confscheduler_lru(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	se *e = s->ptr;
	return sc_ctl_lru(&e->scheduler);
}

static inline int
se_confscheduler_run(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	se *e = s->ptr;
	uint64_t vlsn = sx_vlsn(&e->xm);
	return sc_ctl_call(&e->scheduler, vlsn);
}

static inline srconf*
se_confscheduler(se *e, seconfrt *rt, srconf **pc)
{
	srconf *scheduler = *pc;
	srconf *prev;
	srconf *p = NULL;
	sr_c(&p, pc, se_confv_offline, "threads", SS_U32, &e->conf.threads);
	sr_C(&p, pc, se_confv, "zone", SS_STRING, rt->zone, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "checkpoint_active", SS_U32, &rt->checkpoint_active, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "checkpoint_lsn", SS_U64, &rt->checkpoint_lsn, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "checkpoint_lsn_last", SS_U64, &rt->checkpoint_lsn_last, SR_RO, NULL);
	sr_c(&p, pc, se_confscheduler_checkpoint, "checkpoint",  SS_FUNCTION, NULL);
	sr_C(&p, pc, se_confv, "anticache_active", SS_U32, &rt->anticache_active, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "anticache_asn", SS_U64, &rt->anticache_asn, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "anticache_asn_last", SS_U64, &rt->anticache_asn_last, SR_RO, NULL);
	sr_c(&p, pc, se_confscheduler_anticache, "anticache", SS_FUNCTION, NULL);
	sr_C(&p, pc, se_confv, "snapshot_active", SS_U32, &rt->snapshot_active, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "snapshot_ssn", SS_U64, &rt->snapshot_ssn, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "snapshot_ssn_last", SS_U64, &rt->snapshot_ssn_last, SR_RO, NULL);
	sr_c(&p, pc, se_confscheduler_snapshot, "snapshot", SS_FUNCTION, NULL);
	sr_c(&p, pc, se_confscheduler_on_recover, "on_recover", SS_STRING, NULL);
	sr_c(&p, pc, se_confscheduler_on_recover_arg, "on_recover_arg", SS_STRING, NULL);
	sr_c(&p, pc, se_confscheduler_on_event, "on_event", SS_STRING, NULL);
	sr_c(&p, pc, se_confscheduler_on_event_arg, "on_event_arg", SS_STRING, NULL);
	sr_c(&p, pc, se_confv_offline, "event_on_backup", SS_U32, &e->conf.event_on_backup);
	sr_C(&p, pc, se_confv, "gc_active", SS_U32, &rt->gc_active, SR_RO, NULL);
	sr_c(&p, pc, se_confscheduler_gc, "gc", SS_FUNCTION, NULL);
	sr_C(&p, pc, se_confv, "lru_active", SS_U32, &rt->lru_active, SR_RO, NULL);
	sr_c(&p, pc, se_confscheduler_lru, "lru", SS_FUNCTION, NULL);
	sr_c(&p, pc, se_confscheduler_run, "run", SS_FUNCTION, NULL);
	prev = p;
	sslist *i;
	ss_listforeach(&e->scheduler.wp.list, i) {
		scworker *w = sscast(i, scworker, link);
		srconf *worker = *pc;
		p = NULL;
		sr_C(&p, pc, se_confscheduler_trace, "trace", SS_STRING, w, SR_RO, NULL);
		sr_C(&prev, pc, NULL, w->name, SS_UNDEF, worker, SR_NS, NULL);
	}
	return sr_C(NULL, pc, NULL, "scheduler", SS_UNDEF, scheduler, SR_NS, NULL);
}

static inline int
se_conflog_rotate(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	se *e = s->ptr;
	return sl_poolrotate(&e->lp);
}

static inline int
se_conflog_gc(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	se *e = s->ptr;
	return sl_poolgc(&e->lp);
}

static inline srconf*
se_conflog(se *e, seconfrt *rt, srconf **pc)
{
	srconf *log = *pc;
	srconf *p = NULL;
	sr_c(&p, pc, se_confv_offline, "enable", SS_U32, &e->conf.log_enable);
	sr_c(&p, pc, se_confv_offline, "path", SS_STRINGPTR, &e->conf.log_path);
	sr_c(&p, pc, se_confv_offline, "sync", SS_U32, &e->conf.log_sync);
	sr_c(&p, pc, se_confv_offline, "rotate_wm", SS_U32, &e->conf.log_rotate_wm);
	sr_c(&p, pc, se_confv_offline, "rotate_sync", SS_U32, &e->conf.log_rotate_sync);
	sr_c(&p, pc, se_conflog_rotate, "rotate", SS_FUNCTION, NULL);
	sr_c(&p, pc, se_conflog_gc, "gc", SS_FUNCTION, NULL);
	sr_C(&p, pc, se_confv, "files", SS_U32, &rt->log_files, SR_RO, NULL);
	return sr_C(NULL, pc, NULL, "log", SS_UNDEF, log, SR_NS, NULL);
}

static inline srconf*
se_confperformance(se *e ssunused, seconfrt *rt, srconf **pc)
{
	srconf *perf = *pc;
	srconf *p = NULL;
	sr_C(&p, pc, se_confv, "documents", SS_U64, &rt->stat.v_count, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "documents_used", SS_U64, &rt->stat.v_allocated, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "key", SS_STRING, rt->stat.key.sz, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "value", SS_STRING, rt->stat.value.sz, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "set", SS_U64, &rt->stat.set, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "set_latency", SS_STRING, rt->stat.set_latency.sz, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "delete", SS_U64, &rt->stat.del, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "delete_latency", SS_STRING, rt->stat.del_latency.sz, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "upsert", SS_U64, &rt->stat.upsert, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "upsert_latency", SS_STRING, rt->stat.upsert_latency.sz, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "get", SS_U64, &rt->stat.get, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "get_latency", SS_STRING, rt->stat.get_latency.sz, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "get_read_disk", SS_STRING, rt->stat.get_read_disk.sz, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "get_read_cache", SS_STRING, rt->stat.get_read_cache.sz, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "tx_active_rw", SS_U32, &rt->tx_rw, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "tx_active_ro", SS_U32, &rt->tx_ro, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "tx", SS_U64, &rt->stat.tx, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "tx_rollback", SS_U64, &rt->stat.tx_rlb, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "tx_conflict", SS_U64, &rt->stat.tx_conflict, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "tx_lock", SS_U64, &rt->stat.tx_lock, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "tx_latency", SS_STRING, rt->stat.tx_latency.sz, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "tx_ops", SS_STRING, rt->stat.tx_stmts.sz, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "tx_gc_queue", SS_U32, &rt->tx_gc_queue, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "cursor", SS_U64, &rt->stat.cursor, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "cursor_latency", SS_STRING, rt->stat.cursor_latency.sz, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "cursor_read_disk", SS_STRING, rt->stat.cursor_read_disk.sz, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "cursor_read_cache", SS_STRING, rt->stat.cursor_read_cache.sz, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "cursor_ops", SS_STRING, rt->stat.cursor_ops.sz, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "req_queue", SS_U32, &rt->req_queue, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "req_ready", SS_U32, &rt->req_ready, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "req_active", SS_U32, &rt->req_active, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "reqs", SS_U32, &rt->reqs, SR_RO, NULL);
	return sr_C(NULL, pc, NULL, "performance", SS_UNDEF, perf, SR_NS, NULL);
}

static inline srconf*
se_confmetric(se *e ssunused, seconfrt *rt, srconf **pc)
{
	srconf *metric = *pc;
	srconf *p = NULL;
	sr_C(&p, pc, se_confv, "lsn",  SS_U64, &rt->seq.lsn, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "tsn",  SS_U64, &rt->seq.tsn, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "nsn",  SS_U64, &rt->seq.nsn, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "ssn",  SS_U64, &rt->seq.ssn, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "asn",  SS_U64, &rt->seq.asn, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "dsn",  SS_U32, &rt->seq.dsn, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "bsn",  SS_U32, &rt->seq.bsn, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "lfsn", SS_U64, &rt->seq.lfsn, SR_RO, NULL);
	return sr_C(NULL, pc, NULL, "metric", SS_UNDEF, metric, SR_NS, NULL);
}

static inline int
se_confdb_set(srconf *c ssunused, srconfstmt *s)
{
	/* set(db) */
	se *e = s->ptr;
	if (s->op == SR_WRITE) {
		char *name = s->value;
		sedb *db = (sedb*)se_dbmatch(e, name);
		if (ssunlikely(db)) {
			sr_error(&e->error, "database '%s' already exists", name);
			return -1;
		}
		db = (sedb*)se_dbnew(e, name);
		if (ssunlikely(db == NULL))
			return -1;
		so_listadd(&e->db, &db->o);
		return 0;
	}

	/* get() */
	if (s->op == SR_READ) {
		uint64_t txn = sr_seq(&e->seq, SR_TSN);
		so *c = se_viewdb_new(e, txn);
		if (ssunlikely(c == NULL))
			return -1;
		*(void**)s->value = c;
		return 0;
	}

	sr_error(&e->error, "%s", "bad operation");
	return -1;
}

static inline int
se_confdb_get(srconf *c, srconfstmt *s)
{
	/* get(db.name) */
	se *e = s->ptr;
	if (s->op != SR_READ) {
		sr_error(&e->error, "%s", "bad operation");
		return -1;
	}
	assert(c->ptr != NULL);
	sedb *db = c->ptr;
	si_ref(&db->index, SI_REFFE);
	*(void**)s->value = db;
	return 0;
}

static inline int
se_confdb_upsert(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	sedb *db = c->ptr;
	if (ssunlikely(se_dbactive(db))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	/* set upsert function */
	sfupsertf upsert = (sfupsertf)(uintptr_t)s->value;
	sf_upsertset(&db->scheme.fmt_upsert, upsert);
	return 0;
}

static inline int
se_confdb_upsertarg(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	sedb *db = c->ptr;
	if (ssunlikely(se_dbactive(db))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	sf_upsertset_arg(&db->scheme.fmt_upsert, s->value);
	return 0;
}

static inline int
se_confdb_status(srconf *c, srconfstmt *s)
{
	sedb *db = c->value;
	char *status = sr_statusof(&db->index.status);
	srconf conf = {
		.key      = c->key,
		.flags    = c->flags,
		.type     = c->type,
		.function = NULL,
		.value    = status,
		.ptr      = NULL,
		.next     = NULL
	};
	return se_confv(&conf, s);
}

static inline int
se_confdb_branch(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	sedb *db = c->value;
	se *e = se_of(&db->o);
	uint64_t vlsn = sx_vlsn(&e->xm);
	return sc_ctl_branch(&e->scheduler, vlsn, &db->index);
}

static inline int
se_confdb_compact(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	sedb *db = c->value;
	se *e = se_of(&db->o);
	uint64_t vlsn = sx_vlsn(&e->xm);
	return sc_ctl_compact(&e->scheduler, vlsn, &db->index);
}

static inline int
se_confdb_compact_index(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	sedb *db = c->value;
	se *e = se_of(&db->o);
	uint64_t vlsn = sx_vlsn(&e->xm);
	return sc_ctl_compact_index(&e->scheduler, vlsn, &db->index);
}

static inline int
se_confv_dboffline(srconf *c, srconfstmt *s)
{
	sedb *db = c->ptr;
	if (s->op == SR_WRITE) {
		if (se_dbactive(db)) {
			sr_error(s->r->e, "write to %s is offline-only", s->path);
			return -1;
		}
	}
	return se_confv(c, s);
}

static inline int
se_confdb_index(srconf *c ssunused, srconfstmt *s)
{
	/* set(index, key) */
	sedb *db = c->ptr;
	se *e = se_of(&db->o);
	if (s->op != SR_WRITE) {
		sr_error(&e->error, "%s", "bad operation");
		return -1;
	}
	if (ssunlikely(se_dbactive(db))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	char *name = s->value;
	srkey *part = sr_schemefind(&db->scheme.scheme, name);
	if (ssunlikely(part)) {
		sr_error(&e->error, "keypart '%s' already exists", name);
		return -1;
	}
	/* create new key-part */
	part = sr_schemeadd(&db->scheme.scheme);
	if (ssunlikely(part == NULL)) {
		sr_error(&e->error, "number of index parts reached limit (%d limit)",
		         SR_SCHEME_MAXKEY);
		return -1;
	}
	int rc = sr_keysetname(part, &e->a, name);
	if (ssunlikely(rc == -1))
		goto error;
	rc = sr_keyset(part, &e->a, "string");
	if (ssunlikely(rc == -1))
		goto error;
	return 0;
error:
	sr_schemepop(&db->scheme.scheme, &e->a);
	return -1;
}

static inline int
se_confdb_key(srconf *c, srconfstmt *s)
{
	sedb *db = c->ptr;
	se *e = se_of(&db->o);
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	if (ssunlikely(se_dbactive(db))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	char *path = s->value;
	/* update key-part path */
	srkey *part = sr_schemefind(&db->scheme.scheme, c->key);
	assert(part != NULL);
	return sr_keyset(part, &e->a, path);
}

static inline srconf*
se_confdb(se *e, seconfrt *rt ssunused, srconf **pc)
{
	srconf *db = NULL;
	srconf *prev = NULL;
	srconf *p;
	sslist *i;
	ss_listforeach(&e->db.list, i)
	{
		sedb *o = (sedb*)sscast(i, so, link);
		si_profilerbegin(&o->rtp, &o->index);
		si_profiler(&o->rtp);
		si_profilerend(&o->rtp);
		/* database index */
		srconf *index = *pc;
		p = NULL;
		sr_C(&p, pc, se_confv, "memory_used", SS_U64, &o->rtp.memory_used, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "size", SS_U64, &o->rtp.total_node_size, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "size_uncompressed", SS_U64, &o->rtp.total_node_origin_size, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "size_snapshot", SS_U64, &o->rtp.total_snapshot_size, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "size_amqf", SS_U64, &o->rtp.total_amqf_size, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "count", SS_U64, &o->rtp.count, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "count_dup", SS_U64, &o->rtp.count_dup, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "read_disk", SS_U64, &o->rtp.read_disk, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "read_cache", SS_U64, &o->rtp.read_cache, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "temperature_avg", SS_U32, &o->rtp.temperature_avg, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "temperature_min", SS_U32, &o->rtp.temperature_min, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "temperature_max", SS_U32, &o->rtp.temperature_max, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "temperature_histogram", SS_STRINGPTR, &o->rtp.histogram_temperature_ptr, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "node_count", SS_U32, &o->rtp.total_node_count, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "branch_count", SS_U32, &o->rtp.total_branch_count, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "branch_avg", SS_U32, &o->rtp.total_branch_avg, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "branch_max", SS_U32, &o->rtp.total_branch_max, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "branch_histogram", SS_STRINGPTR, &o->rtp.histogram_branch_ptr, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "page_count", SS_U32, &o->rtp.total_page_count, SR_RO, NULL);
		sr_C(&p, pc, se_confdb_upsert, "upsert", SS_STRING, NULL, 0, o);
		sr_C(&p, pc, se_confdb_upsertarg, "upsert_arg", SS_STRING, NULL, 0, o);
		/* index keys */
		int i = 0;
		while (i < o->scheme.scheme.count) {
			srkey *part = sr_schemeof(&o->scheme.scheme, i);
			sr_C(&p, pc, se_confdb_key, part->name, SS_STRING, part->path, 0, o);
			i++;
		}
		/* database */
		srconf *database = *pc;
		p = NULL;
		sr_C(&p, pc, se_confv, "name", SS_STRINGPTR, &o->scheme.name, SR_RO, NULL);
		sr_C(&p, pc, se_confv_dboffline, "id", SS_U32, &o->scheme.id, 0, o);
		sr_C(&p, pc, se_confdb_status,   "status", SS_STRING, o, SR_RO, NULL);
		sr_C(&p, pc, se_confv_dboffline, "storage", SS_STRINGPTR, &o->scheme.storage_sz, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "format", SS_STRINGPTR, &o->scheme.fmt_sz, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "amqf", SS_U32, &o->scheme.amqf, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "path", SS_STRINGPTR, &o->scheme.path, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "path_fail_on_exists", SS_U32, &o->scheme.path_fail_on_exists, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "path_fail_on_drop", SS_U32, &o->scheme.path_fail_on_drop, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "cache_mode", SS_U32, &o->scheme.cache_mode, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "cache", SS_STRINGPTR, &o->scheme.cache_sz, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "mmap", SS_U32, &o->scheme.mmap, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "sync", SS_U32, &o->scheme.sync, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "node_preload", SS_U32, &o->scheme.node_compact_load, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "node_size", SS_U64, &o->scheme.node_size, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "page_size", SS_U32, &o->scheme.node_page_size, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "page_checksum", SS_U32, &o->scheme.node_page_checksum, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "compression_key", SS_U32, &o->scheme.compression_key, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "compression_branch", SS_STRINGPTR, &o->scheme.compression_branch_sz, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "compression", SS_STRINGPTR, &o->scheme.compression_sz, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "lru", SS_U64, &o->scheme.lru, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "lru_step", SS_U32, &o->scheme.lru_step, 0, o);
		sr_c(&p, pc, se_confdb_branch, "branch", SS_FUNCTION, o);
		sr_c(&p, pc, se_confdb_compact, "compact", SS_FUNCTION, o);
		sr_c(&p, pc, se_confdb_compact_index, "compact_index", SS_FUNCTION, o);
		sr_C(&p, pc, se_confdb_index, "index", SS_UNDEF, index, SR_NS, o);
		sr_C(&prev, pc, se_confdb_get, o->scheme.name, SS_STRING, database, SR_NS, o);
		if (db == NULL)
			db = prev;
	}
	return sr_C(NULL, pc, se_confdb_set, "db", SS_STRING, db, SR_NS, NULL);
}

static inline int
se_confview_set(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	se *e = s->ptr;
	char *name = s->value;
	uint64_t lsn = sr_seq(&e->seq, SR_LSN);
	/* create view object */
	seview *view = (seview*)se_viewnew(e, lsn, name);
	if (ssunlikely(view == NULL))
		return -1;
	so_listadd(&e->view, &view->o);
	return 0;
}

static inline int
se_confview_lsn(srconf *c, srconfstmt *s)
{
	int rc = se_confv(c, s);
	if (ssunlikely(rc == -1))
		return -1;
	if (s->op != SR_WRITE)
		return 0;
	seview *view  = c->ptr;
	se_viewupdate(view);
	return 0;
}

static inline int
se_confview_get(srconf *c, srconfstmt *s)
{
	/* get(view.name) */
	se *e = s->ptr;
	if (s->op != SR_READ) {
		sr_error(&e->error, "%s", "bad operation");
		return -1;
	}
	assert(c->ptr != NULL);
	*(void**)s->value = c->ptr;
	return 0;
}

static inline srconf*
se_confview(se *e, seconfrt *rt ssunused, srconf **pc)
{
	srconf *view = NULL;
	srconf *prev = NULL;
	sslist *i;
	ss_listforeach(&e->view.list, i)
	{
		seview *s = (seview*)sscast(i, so, link);
		srconf *p = sr_C(NULL, pc, se_confview_lsn, "lsn", SS_U64, &s->vlsn, 0, s);
		sr_C(&prev, pc, se_confview_get, s->name, SS_STRING, p, SR_NS, s);
		if (view == NULL)
			view = prev;
	}
	return sr_C(NULL, pc, se_confview_set, "view", SS_STRING,
	            view, SR_NS, NULL);
}


static inline int
se_confbackup_run(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	se *e = s->ptr;
	return sc_ctl_backup(&e->scheduler);
}

static inline srconf*
se_confbackup(se *e, seconfrt *rt, srconf **pc)
{
	srconf *backup = *pc;
	srconf *p = NULL;
	sr_c(&p, pc, se_confv_offline, "path", SS_STRINGPTR, &e->conf.backup_path);
	sr_c(&p, pc, se_confbackup_run, "run", SS_FUNCTION, NULL);
	sr_C(&p, pc, se_confv, "active", SS_U32, &rt->backup_active, SR_RO, NULL);
	sr_c(&p, pc, se_confv, "last", SS_U32, &rt->backup_last);
	sr_c(&p, pc, se_confv, "last_complete", SS_U32, &rt->backup_last_complete);
	return sr_C(NULL, pc, NULL, "backup", 0, backup, SR_NS, NULL);
}

static inline int
se_confdebug_oom(srconf *c, srconfstmt *s)
{
	se *e = s->ptr;
	assert(e->ei.oom == 0);
	int rc = se_confv(c, s);
	if (ssunlikely(rc == -1))
		return rc;

	ss_aclose(&e->a);
	ss_aclose(&e->a_document);
	ss_aclose(&e->a_cursor);
	ss_aclose(&e->a_viewdb);
	ss_aclose(&e->a_view);
	ss_aclose(&e->a_cachebranch);
	ss_aclose(&e->a_cache);
	ss_aclose(&e->a_confcursor);
	ss_aclose(&e->a_confkv);
	ss_aclose(&e->a_tx);
	ss_aclose(&e->a_sxv);

	ss_aopen(&e->a_oom, &ss_ooma, e->ei.oom);
	e->a = e->a_oom;
	e->a_document = e->a_oom;
	e->a_cursor = e->a_oom;
	e->a_viewdb = e->a_oom;
	e->a_view = e->a_oom;
	e->a_cachebranch = e->a_oom;
	e->a_cache = e->a_oom;
	e->a_confkv = e->a_oom;
	e->a_tx = e->a_oom;
	e->a_sxv = e->a_oom;
	return 0;
}

static inline int
se_confdebug_io(srconf *c, srconfstmt *s)
{
	se *e = s->ptr;
	assert(e->ei.io == 0);
	int rc = se_confv(c, s);
	if (ssunlikely(rc == -1))
		return rc;
	ss_vfsfree(&e->vfs);
	ss_vfsinit(&e->vfs, &ss_testvfs, e->ei.io);
	return 0;
}

static inline srconf*
se_confdebug(se *e, seconfrt *rt ssunused, srconf **pc)
{
	srconf *prev = NULL;
	srconf *p = NULL;
	prev = p;
	srconf *ei = *pc;
	sr_c(&p, pc, se_confdebug_oom, "oom",     SS_U32, &e->ei.oom);
	sr_c(&p, pc, se_confdebug_io, "io",       SS_U32, &e->ei.io);
	sr_c(&p, pc, se_confv, "sd_build_0",      SS_U32, &e->ei.e[0]);
	sr_c(&p, pc, se_confv, "sd_build_1",      SS_U32, &e->ei.e[1]);
	sr_c(&p, pc, se_confv, "si_branch_0",     SS_U32, &e->ei.e[2]);
	sr_c(&p, pc, se_confv, "si_compaction_0", SS_U32, &e->ei.e[3]);
	sr_c(&p, pc, se_confv, "si_compaction_1", SS_U32, &e->ei.e[4]);
	sr_c(&p, pc, se_confv, "si_compaction_2", SS_U32, &e->ei.e[5]);
	sr_c(&p, pc, se_confv, "si_compaction_3", SS_U32, &e->ei.e[6]);
	sr_c(&p, pc, se_confv, "si_compaction_4", SS_U32, &e->ei.e[7]);
	sr_c(&p, pc, se_confv, "si_recover_0",    SS_U32, &e->ei.e[8]);
	sr_c(&p, pc, se_confv, "si_snapshot_0",   SS_U32, &e->ei.e[9]);
	sr_c(&p, pc, se_confv, "si_snapshot_1",   SS_U32, &e->ei.e[10]);
	sr_c(&p, pc, se_confv, "si_snapshot_2",   SS_U32, &e->ei.e[11]);
	sr_C(&prev, pc, NULL, "error_injection", SS_UNDEF, ei, SR_NS, NULL);
	srconf *debug = prev;
	return sr_C(NULL, pc, NULL, "debug", SS_UNDEF, debug, SR_NS, NULL);
}

static srconf*
se_confprepare(se *e, seconfrt *rt, srconf *c, int serialize)
{
	/* sophia */
	srconf *pc = c;
	srconf *sophia     = se_confsophia(e, rt, &pc);
	srconf *memory     = se_confmemory(e, rt, &pc);
	srconf *compaction = se_confcompaction(e, rt, &pc);
	srconf *scheduler  = se_confscheduler(e, rt, &pc);
	srconf *perf       = se_confperformance(e, rt, &pc);
	srconf *metric     = se_confmetric(e, rt, &pc);
	srconf *log        = se_conflog(e, rt, &pc);
	srconf *view       = se_confview(e, rt, &pc);
	srconf *backup     = se_confbackup(e, rt, &pc);
	srconf *db         = se_confdb(e, rt, &pc);
	srconf *debug      = se_confdebug(e, rt, &pc);

	sophia->next     = memory;
	memory->next     = compaction;
	compaction->next = scheduler;
	scheduler->next  = perf;
	perf->next       = metric;
	metric->next     = log;
	log->next        = view;
	view->next       = backup;
	backup->next     = db;
	if (! serialize)
		db->next = debug;
	return sophia;
}

static int
se_confrt(se *e, seconfrt *rt)
{
	/* sophia */
	snprintf(rt->version, sizeof(rt->version),
	         "%d.%d.%d",
	         SR_VERSION_A - '0',
	         SR_VERSION_B - '0',
	         SR_VERSION_C - '0');
	snprintf(rt->version_storage, sizeof(rt->version_storage),
	         "%d.%d.%d",
	         SR_VERSION_STORAGE_A - '0',
	         SR_VERSION_STORAGE_B - '0',
	         SR_VERSION_STORAGE_C - '0');
	snprintf(rt->build, sizeof(rt->build), "%s",
	         SR_VERSION_COMMIT);

	/* memory */
	rt->memory_used     = ss_quotaused(&e->quota);
	rt->pager_pools     = e->pager.pools;
	rt->pager_pool_size = e->pager.pool_size;

	/* scheduler */
	ss_mutexlock(&e->scheduler.lock);
	rt->checkpoint_active    = e->scheduler.checkpoint;
	rt->checkpoint_lsn_last  = e->scheduler.checkpoint_lsn_last;
	rt->checkpoint_lsn       = e->scheduler.checkpoint_lsn;
	rt->snapshot_active      = e->scheduler.snapshot;
	rt->snapshot_ssn         = e->scheduler.snapshot_ssn;
	rt->snapshot_ssn_last    = e->scheduler.snapshot_ssn_last;
	rt->anticache_active     = e->scheduler.anticache;
	rt->anticache_asn        = e->scheduler.anticache_asn;
	rt->anticache_asn_last   = e->scheduler.anticache_asn_last;
	rt->backup_active        = e->scheduler.backup;
	rt->backup_last          = e->scheduler.backup_bsn_last;
	rt->backup_last_complete = e->scheduler.backup_bsn_last_complete;
	rt->gc_active            = e->scheduler.gc;
	rt->lru_active           = e->scheduler.lru;
	ss_mutexunlock(&e->scheduler.lock);

	int v = ss_quotaused_percent(&e->quota);
	srzone *z = sr_zonemap(&e->conf.zones, v);
	memcpy(rt->zone, z->name, sizeof(rt->zone));

	/* log */
	rt->log_files = sl_poolfiles(&e->lp);

	/* metric */
	sr_seqlock(&e->seq);
	rt->seq = e->seq;
	sr_sequnlock(&e->seq);

	/* performance */
	rt->tx_rw = e->xm.count_rw;
	rt->tx_ro = e->xm.count_rd;
	rt->tx_gc_queue = e->xm.count_gc;

	ss_mutexlock(&e->scheduler.rp.lock);
	rt->req_queue  = e->scheduler.rp.list.n;
	rt->req_ready  = e->scheduler.rp.list_ready.n;
	rt->req_active = e->scheduler.rp.list_active.n;
	rt->reqs = rt->req_queue + rt->req_ready + rt->req_active;
	ss_mutexunlock(&e->scheduler.rp.lock);

	ss_spinlock(&e->stat.lock);
	rt->stat = e->stat;
	ss_spinunlock(&e->stat.lock);
	sr_statprepare(&rt->stat);
	return 0;
}

static inline int
se_confensure(seconf *c)
{
	se *e = (se*)c->env;
	int confmax = 2048 + (e->db.n * 100) + (e->view.n * 10) +
	              c->threads;
	confmax *= sizeof(srconf);
	if (sslikely(confmax <= c->confmax))
		return 0;
	srconf *cptr = ss_malloc(&e->a, confmax);
	if (ssunlikely(cptr == NULL))
		return sr_oom(&e->error);
	ss_free(&e->a, c->conf);
	c->conf = cptr;
	c->confmax = confmax;
	return 0;
}

int se_confserialize(seconf *c, ssbuf *buf)
{
	int rc;
	rc = se_confensure(c);
	if (ssunlikely(rc == -1))
		return -1;
	se *e = (se*)c->env;
	seconfrt rt;
	se_confrt(e, &rt);
	srconf *conf = c->conf;
	srconf *root;
	root = se_confprepare(e, &rt, conf, 1);
	srconfstmt stmt = {
		.op        = SR_SERIALIZE,
		.path      = NULL,
		.value     = NULL,
		.valuesize = 0,
		.valuetype = SS_UNDEF,
		.serialize = buf,
		.ptr       = e,
		.r         = &e->r
	};
	return sr_confexec(root, &stmt);
}

static int
se_confquery(se *e, int op, const char *path,
             sstype valuetype, void *value, int valuesize,
             int *size)
{
	int rc;
	rc = se_confensure(&e->conf);
	if (ssunlikely(rc == -1))
		return -1;
	seconfrt rt;
	se_confrt(e, &rt);
	srconf *conf = e->conf.conf;
	srconf *root;
	root = se_confprepare(e, &rt, conf, 0);
	srconfstmt stmt = {
		.op        = op,
		.path      = path,
		.value     = value,
		.valuesize = valuesize,
		.valuetype = valuetype,
		.serialize = NULL,
		.ptr       = e,
		.r         = &e->r
	};
	rc = sr_confexec(root, &stmt);
	if (size)
		*size = stmt.valuesize;
	return rc;
}

int se_confset_string(so *o, const char *path, void *string, int size)
{
	se *e = se_of(o);
	if (string && size == 0)
		size = strlen(string) + 1;
	return se_confquery(e, SR_WRITE, path, SS_STRING,
	                   string, size, NULL);
}

int se_confset_int(so *o, const char *path, int64_t v)
{
	se *e = se_of(o);
	return se_confquery(e, SR_WRITE, path, SS_I64,
	                    &v, sizeof(v), NULL);
}

void *se_confget_object(so *o, const char *path)
{
	se *e = se_of(o);
	if (path == NULL)
		return se_confcursor_new(o);
	void *result = NULL;
	int rc = se_confquery(e, SR_READ, path, SS_OBJECT,
	                      &result, sizeof(void*), NULL);
	if (ssunlikely(rc == -1))
		return NULL;
	return result;
}

void *se_confget_string(so *o, const char *path, int *size)
{
	se *e = se_of(o);
	void *result = NULL;
	int rc = se_confquery(e, SR_READ, path, SS_STRING,
	                      &result, sizeof(void*), size);
	if (ssunlikely(rc == -1))
		return NULL;
	return result;
}

int64_t se_confget_int(so *o, const char *path)
{
	se *e = se_of(o);
	int64_t result = 0;
	int rc = se_confquery(e, SR_READ, path, SS_I64,
	                      &result, sizeof(void*), NULL);
	if (ssunlikely(rc == -1))
		return -1;
	return result;
}

int se_confinit(seconf *c, so *e)
{
	se *o = se_of(e);
	c->confmax = 2048;
	c->conf = ss_malloc(&o->a, sizeof(srconf) * c->confmax);
	if (ssunlikely(c->conf == NULL))
		return -1;
	sr_schemeinit(&c->scheme);
	srkey *part = sr_schemeadd(&c->scheme);
	sr_keysetname(part, &o->a, "key");
	sr_keyset(part, &o->a, "string");
	c->env                 = e;
	c->path                = NULL;
	c->path_create         = 1;
	c->recover             = 1;
	c->memory_limit        = 0;
	c->anticache           = 0;
	c->threads             = 6;
	c->log_enable          = 1;
	c->log_path            = NULL;
	c->log_rotate_wm       = 500000;
	c->log_sync            = 0;
	c->log_rotate_sync     = 1;
	c->on_recover.function = NULL;
	c->on_recover.arg      = NULL;
	ss_triggerinit(&c->on_event);
	c->event_on_backup     = 0;
	srzone def = {
		.enable            = 1,
		.mode              = 3, /* branch + compact */
		.compact_wm        = 2,
		.compact_mode      = 0, /* branch priority */
		.branch_prio       = 1,
		.branch_wm         = 10 * 1024 * 1024,
		.branch_age        = 40,
		.branch_age_period = 40,
		.branch_age_wm     = 1 * 1024 * 1024,
		.anticache_period  = 0,
		.snapshot_period   = 0,
		.backup_prio       = 1,
		.gc_prio           = 1,
		.gc_period         = 60,
		.gc_wm             = 30,
		.lru_prio          = 0,
		.lru_period        = 0,
		.async             = 2 /* do not own thread */
	};
	srzone redzone = {
		.enable            = 1,
		.mode              = 2, /* checkpoint */
		.compact_wm        = 4,
		.compact_mode      = 0,
		.branch_prio       = 0,
		.branch_wm         = 0,
		.branch_age        = 0,
		.branch_age_period = 0,
		.branch_age_wm     = 0,
		.anticache_period  = 0,
		.snapshot_period   = 0,
		.backup_prio       = 0,
		.gc_prio           = 0,
		.gc_period         = 0,
		.gc_wm             = 0,
		.lru_prio          = 0,
		.lru_period        = 0,
		.async             = 2
	};
	sr_zonemap_set(&o->conf.zones,  0, &def);
	sr_zonemap_set(&o->conf.zones, 80, &redzone);
	c->backup_path = NULL;
	return 0;
}

void se_conffree(seconf *c)
{
	se *e = (se*)c->env;
	if (c->conf) {
		ss_free(&e->a, c->conf);
		c->conf = NULL;
	}
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
	sr_schemefree(&c->scheme, &e->a);
}

int se_confvalidate(seconf *c)
{
	se *e = (se*)c->env;
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
		srzone *z = &e->conf.zones.zones[i];
		if (! z->enable)
			continue;
		if (z->compact_wm <= 1) {
			sr_error(&e->error, "bad %d.compact_wm value", i * 10);
			return -1;
		}
	}
	return 0;
}
