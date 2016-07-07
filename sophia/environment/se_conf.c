
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
		if (ssunlikely(sr_online(&e->status))) {
			sr_error(s->r->e, "write to %s is offline-only", s->path);
			return -1;
		}
	}
	return se_confv(c, s);
}

static inline int
se_confsophia_status(srconf *c, srconfstmt *s)
{
	se *e = s->ptr;
	char *status = sr_statusof(&e->status);
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

static inline int
se_confsophia_on_log(srconf *c, srconfstmt *s)
{
	se *e = s->ptr;
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	if (ssunlikely(sr_online(&e->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	e->log.log = (srlogcbf)(uintptr_t)s->value;
	return 0;
}

static inline int
se_confsophia_on_log_arg(srconf *c, srconfstmt *s)
{
	se *e = s->ptr;
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	if (ssunlikely(sr_online(&e->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	e->log.arg = s->value;
	return 0;
}

static inline srconf*
se_confsophia(se *e, seconfrt *rt, srconf **pc)
{
	srconf *sophia = *pc;
	srconf *p = NULL;
	sr_C(&p, pc, se_confv, "version", SS_STRING, rt->version, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "version_storage", SS_STRING, rt->version_storage, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "build", SS_STRING, rt->build, SR_RO, NULL);
	sr_C(&p, pc, se_confsophia_status, "status", SS_STRING, NULL, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "errors", SS_U64, &rt->errors, SR_RO, NULL);
	sr_C(&p, pc, se_confsophia_error, "error", SS_STRING, NULL, SR_RO, NULL);
	sr_c(&p, pc, se_confv_offline, "path", SS_STRINGPTR, &e->rep_conf->path);
	sr_c(&p, pc, se_confsophia_on_log, "on_log", SS_STRING, NULL);
	sr_c(&p, pc, se_confsophia_on_log_arg, "on_log_arg", SS_STRING, NULL);
	return sr_C(NULL, pc, NULL, "sophia", SS_UNDEF, sophia, SR_NS, NULL);
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
se_confscheduler_run(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	se *e = s->ptr;
	uint64_t vlsn = sx_vlsn(&e->xm);
	return sc_ctl_call(&e->scheduler, vlsn);
}

static inline srconf*
se_confscheduler(se *e, srconf **pc, int serialize)
{
	srconf *scheduler = *pc;
	srconf *prev;
	srconf *p = NULL;
	sr_c(&p, pc, se_confv_offline, "threads", SS_U32, &e->conf.threads);
	if (! serialize)
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
	sr_c(&p, pc, se_confv_offline, "enable", SS_U32, &e->lp_conf->enable);
	sr_c(&p, pc, se_confv_offline, "path", SS_STRINGPTR, &e->lp_conf->path);
	sr_c(&p, pc, se_confv_offline, "sync", SS_U32, &e->lp_conf->sync_on_write);
	sr_c(&p, pc, se_confv_offline, "rotate_wm", SS_U32, &e->lp_conf->rotatewm);
	sr_c(&p, pc, se_confv_offline, "rotate_sync", SS_U32, &e->lp_conf->sync_on_rotate);
	sr_c(&p, pc, se_conflog_rotate, "rotate", SS_FUNCTION, NULL);
	sr_c(&p, pc, se_conflog_gc, "gc", SS_FUNCTION, NULL);
	sr_C(&p, pc, se_confv, "files", SS_U32, &rt->log_files, SR_RO, NULL);
	return sr_C(NULL, pc, NULL, "log", SS_UNDEF, log, SR_NS, NULL);
}

static inline srconf*
se_conftransaction(se *e ssunused, seconfrt *rt, srconf **pc)
{
	srconf *xm = *pc;
	srconf *p = NULL;
	sr_C(&p, pc, se_confv, "online_rw", SS_U32, &rt->tx_rw, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "online_ro", SS_U32, &rt->tx_ro, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "commit", SS_U64, &rt->tx_stat.tx, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "rollback", SS_U64, &rt->tx_stat.tx_rlb, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "conflict", SS_U64, &rt->tx_stat.tx_conflict, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "lock", SS_U64, &rt->tx_stat.tx_lock, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "latency", SS_STRING, rt->tx_stat.tx_latency.sz, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "log", SS_STRING, rt->tx_stat.tx_stmts.sz, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "gc", SS_U32, &rt->tx_gc, SR_RO, NULL);
	sr_C(&p, pc, se_confv, "gc_lsn", SS_U64, &rt->tx_vlsn, SR_RO, NULL);
	return sr_C(NULL, pc, NULL, "transaction", SS_UNDEF, xm, SR_NS, NULL);
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
	if (s->op != SR_WRITE) {
		sr_error(&e->error, "%s", "bad operation");
		return -1;
	}
	/* ensure that environment is offline */
	if (ssunlikely(sr_online(&e->status))) {
		sr_error(&e->error, "%s", "bad operation: environment is online");
		return -1;
	}
	/* define database */
	char *name = s->value;
	sedb *db = (sedb*)se_dbmatch(e, name);
	if (ssunlikely(db)) {
		sr_error(&e->error, "database '%s' already exists", name);
		return -1;
	}
	db = (sedb*)se_dbnew(e, name, s->valuesize);
	if (ssunlikely(db == NULL))
		return -1;
	so_listadd(&e->db, &db->o);
	return 0;
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
	*(void**)s->value = db;
	return 0;
}

static inline int
se_confdb_comparator(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	sedb *db = c->ptr;
	se *e = se_of(&db->o);
	if (ssunlikely(sr_online(&e->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	/* set compare function */
	sfcmpf compare = (sfcmpf)(uintptr_t)s->value;
	sf_schemeset_comparator(&db->scheme->scheme, compare);
	return 0;
}

static inline int
se_confdb_comparatorarg(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	sedb *db = c->ptr;
	se *e = se_of(&db->o);
	if (ssunlikely(sr_online(&e->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	/* set compare function argument */
	sf_schemeset_comparatorarg(&db->scheme->scheme, s->value);
	return 0;
}

static inline int
se_confdb_upsert(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	sedb *db = c->ptr;
	se *e = se_of(&db->o);
	if (ssunlikely(sr_online(&e->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	/* set upsert function */
	sfupsertf upsert = (sfupsertf)(uintptr_t)s->value;
	sf_upsertset(&db->scheme->upsert, upsert);
	return 0;
}

static inline int
se_confdb_upsertarg(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	sedb *db = c->ptr;
	se *e = se_of(&db->o);
	if (ssunlikely(sr_online(&e->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	sf_upsertset_arg(&db->scheme->upsert, s->value);
	return 0;
}

static inline int
se_confdb_branch(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	sedb *db = c->value;
	se *e = se_of(&db->o);
	uint64_t vlsn = sx_vlsn(&e->xm);
	return sc_ctl_branch(&e->scheduler, vlsn, db->index);
}

static inline int
se_confdb_compact(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	sedb *db = c->value;
	se *e = se_of(&db->o);
	uint64_t vlsn = sx_vlsn(&e->xm);
	return sc_ctl_compact(&e->scheduler, vlsn, db->index);
}

static inline int
se_confdb_compact_index(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	sedb *db = c->value;
	se *e = se_of(&db->o);
	uint64_t vlsn = sx_vlsn(&e->xm);
	return sc_ctl_compact_index(&e->scheduler, vlsn, db->index);
}

static inline int
se_confdb_checkpoint(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	sedb *db = c->value;
	se *e = se_of(&db->o);
	return sc_ctl_checkpoint(&e->scheduler, db->index);
}

static inline int
se_confdb_snapshot(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	sedb *db = c->value;
	se *e = se_of(&db->o);
	return sc_ctl_snapshot(&e->scheduler, db->index);
}

static inline int
se_confdb_gc(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	sedb *db = c->value;
	se *e = se_of(&db->o);
	return sc_ctl_gc(&e->scheduler, db->index);
}

static inline int
se_confdb_expire(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	sedb *db = c->value;
	se *e = se_of(&db->o);
	return sc_ctl_expire(&e->scheduler, db->index);
}

static inline int
se_confv_dboffline(srconf *c, srconfstmt *s)
{
	sedb *db = c->ptr;
	se *e = se_of(&db->o);
	if (s->op == SR_WRITE) {
		if (ssunlikely(sr_online(&e->status))) {
			sr_error(s->r->e, "write to %s is offline-only", s->path);
			return -1;
		}
	}
	return se_confv(c, s);
}

static inline int
se_confdb_scheme(srconf *c ssunused, srconfstmt *s)
{
	/* set(scheme, field) */
	sedb *db = c->ptr;
	se *e = se_of(&db->o);
	if (s->op != SR_WRITE) {
		sr_error(&e->error, "%s", "bad operation");
		return -1;
	}
	if (ssunlikely(sr_online(&e->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	if (ssunlikely(db->scheme->scheme.fields_count == 8)) {
		sr_error(s->r->e, "%s", "fields number limit reached");
		return -1;
	}
	char *name = s->value;
	sffield *field = sf_schemefind(&db->scheme->scheme, name);
	if (ssunlikely(field)) {
		sr_error(&e->error, "field '%s' is already set", name);
		return -1;
	}
	/* create new field */
	field = sf_fieldnew(&e->a, name);
	if (ssunlikely(field == NULL))
		return sr_oom(&e->error);
	int rc;
	rc = sf_fieldoptions(field, &e->a, "string");
	if (ssunlikely(rc == -1)) {
		sf_fieldfree(field, &e->a);
		return sr_oom(&e->error);
	}
	rc = sf_schemeadd(&db->scheme->scheme, &e->a, field);
	if (ssunlikely(rc == -1)) {
		sf_fieldfree(field, &e->a);
		return sr_oom(&e->error);
	}
	return 0;
}

static inline int
se_confdb_field(srconf *c, srconfstmt *s)
{
	sedb *db = c->ptr;
	se *e = se_of(&db->o);
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	if (ssunlikely(sr_online(&e->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	char *path = s->value;
	/* update key-part path */
	sffield *field = sf_schemefind(&db->scheme->scheme, c->key);
	assert(field != NULL);
	return sf_fieldoptions(field, &e->a, path);
}

static inline srconf*
se_confdb(se *e, seconfrt *rt ssunused, srconf **pc, int serialize)
{
	srconf *db = NULL;
	srconf *prev = NULL;
	srconf *p;
	sslist *i;
	ss_listforeach(&e->db.list, i)
	{
		sedb *o = (sedb*)sscast(i, so, link);
		sr_statcopy(&o->stat, &o->statrt);
		sr_statprepare(&o->statrt);
		sc_profiler(&e->scheduler, &o->scp, o->index);
		si_profilerbegin(&o->rtp, o->index);
		si_profiler(&o->rtp);
		si_profilerend(&o->rtp);

		/* compaction */
		srconf *compaction = *pc;
		p = NULL;
		sr_C(&p, pc, se_confv_dboffline, "node_size", SS_U64, &o->scheme->node_size, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "page_size", SS_U32, &o->scheme->node_page_size, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "page_checksum", SS_U32, &o->scheme->node_page_checksum, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "checkpoint_wm", SS_U32, &o->scheme->compaction.checkpoint_wm, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "compact_wm", SS_U32, &o->scheme->compaction.compact_wm, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "compact_mode", SS_U32, &o->scheme->compaction.compact_mode, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "branch_wm", SS_U32, &o->scheme->compaction.branch_wm, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "branch_age", SS_U32, &o->scheme->compaction.branch_age, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "branch_age_period", SS_U32, &o->scheme->compaction.branch_age_period, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "branch_age_wm", SS_U32, &o->scheme->compaction.branch_age_wm, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "snapshot_period", SS_U32, &o->scheme->compaction.snapshot_period, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "expire_period", SS_U32, &o->scheme->compaction.expire_period, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "gc_wm", SS_U32, &o->scheme->compaction.gc_wm, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "gc_period", SS_U32, &o->scheme->compaction.gc_period, 0, o);
		if (! serialize) {
			sr_c(&p, pc, se_confdb_branch, "branch", SS_FUNCTION, o);
			sr_c(&p, pc, se_confdb_compact, "compact", SS_FUNCTION, o);
			sr_c(&p, pc, se_confdb_compact_index, "compact_index", SS_FUNCTION, o);
			sr_c(&p, pc, se_confdb_checkpoint, "checkpoint", SS_FUNCTION, o);
			sr_c(&p, pc, se_confdb_snapshot, "snapshot", SS_FUNCTION, o);
			sr_c(&p, pc, se_confdb_gc, "gc", SS_FUNCTION, o);
			sr_c(&p, pc, se_confdb_expire, "expire", SS_FUNCTION, o);
		}

		/* stat */
		srconf *stat = *pc;
		p = NULL;
		sr_C(&p, pc, se_confv, "documents_used", SS_U64, &o->statrt.v_allocated, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "documents", SS_U64, &o->statrt.v_count, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "field", SS_STRING, o->statrt.field.sz, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "set", SS_U64, &o->statrt.set, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "set_latency", SS_STRING, o->statrt.set_latency.sz, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "delete", SS_U64, &o->statrt.del, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "delete_latency", SS_STRING, o->statrt.del_latency.sz, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "upsert", SS_U64, &o->statrt.upsert, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "upsert_latency", SS_STRING, o->statrt.upsert_latency.sz, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "get", SS_U64, &o->statrt.get, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "get_latency", SS_STRING, o->statrt.get_latency.sz, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "get_read_disk", SS_STRING, o->statrt.get_read_disk.sz, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "get_read_cache", SS_STRING, o->statrt.get_read_cache.sz, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "pread", SS_U64, &o->statrt.pread, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "pread_latency", SS_STRING, o->statrt.pread_latency.sz, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "cursor", SS_U64, &o->statrt.cursor, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "cursor_latency", SS_STRING, o->statrt.cursor_latency.sz, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "cursor_read_disk", SS_STRING, o->statrt.cursor_read_disk.sz, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "cursor_read_cache", SS_STRING, o->statrt.cursor_read_cache.sz, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "cursor_ops", SS_STRING, o->statrt.cursor_ops.sz, SR_RO, NULL);

		/* scheduler */
		srconf *scheduler = *pc;
		p = NULL;
		sr_C(&p, pc, se_confv, "checkpoint", SS_U32, &o->scp.state.checkpoint, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "checkpoint_lsn", SS_U64, &o->scp.state.checkpoint_lsn, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "checkpoint_lsn_last", SS_U64, &o->scp.state.checkpoint_lsn_last, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "snapshot", SS_U32, &o->scp.state.snapshot, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "snapshot_ssn", SS_U64, &o->scp.state.snapshot_ssn, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "snapshot_ssn_last", SS_U64, &o->scp.state.snapshot_ssn_last, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "gc", SS_U32, &o->scp.state.gc, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "expire", SS_U32, &o->scp.state.expire, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "backup", SS_U32, &o->scp.state.backup, SR_RO, NULL);

		/* index */
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

		/* scheme */
		srconf *scheme = *pc;
		p = NULL;
		int i = 0;
		while (i < o->scheme->scheme.fields_count) {
			sffield *field = o->scheme->scheme.fields[i];
			sr_C(&p, pc, se_confdb_field, field->name, SS_STRING, field->options, 0, o);
			i++;
		}

		/* database */
		srconf *database = *pc;
		p = NULL;
		sr_C(&p, pc, se_confv, "name", SS_STRINGPTR, &o->scheme->name, SR_RO, NULL);
		sr_C(&p, pc, se_confv, "id", SS_U32, &o->scheme->id, SR_RO, o);
		sr_C(&p, pc, se_confv_dboffline, "path", SS_STRINGPTR, &o->scheme->path, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "memory_limit", SS_U64, &o->scheme->memory_limit, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "mmap", SS_U32, &o->scheme->mmap, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "direct_io", SS_U32, &o->scheme->direct_io, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "sync", SS_U32, &o->scheme->sync, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "temperature", SS_U32, &o->scheme->temperature, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "amqf", SS_U32, &o->scheme->amqf, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "expire", SS_U32, &o->scheme->expire, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "compression_hot", SS_STRINGPTR, &o->scheme->compression_hot_sz, 0, o);
		sr_C(&p, pc, se_confv_dboffline, "compression_cold", SS_STRINGPTR, &o->scheme->compression_cold_sz, 0, o);
		sr_C(&p, pc, se_confdb_upsert, "comparator", SS_STRING, NULL, 0, o);
		sr_C(&p, pc, se_confdb_upsertarg, "comparator_arg", SS_STRING, NULL, 0, o);
		sr_C(&p, pc, se_confdb_upsert, "upsert", SS_STRING, NULL, 0, o);
		sr_C(&p, pc, se_confdb_upsertarg, "upsert_arg", SS_STRING, NULL, 0, o);

		/* .. */
		sr_C(&p, pc, NULL, "compaction", SS_UNDEF, compaction, SR_NS, o);
		sr_C(&p, pc, NULL, "stat", SS_UNDEF, stat, SR_NS, o);
		sr_C(&p, pc, NULL, "scheduler", SS_UNDEF, scheduler, SR_NS, o);
		sr_C(&p, pc, NULL, "index", SS_UNDEF, index, SR_NS, o);
		sr_C(&p, pc, se_confdb_scheme, "scheme", SS_UNDEF, scheme, SR_NS, o);
		sr_C(&prev, pc, se_confdb_get, o->scheme->name, SS_STRING, database, SR_NS, o);
		if (db == NULL)
			db = prev;
	}
	return sr_C(NULL, pc, se_confdb_set, "db", SS_STRING, db, SR_NS, NULL);
}

static inline int
se_confbackup_run(srconf *c, srconfstmt *s)
{
	if (s->op != SR_WRITE)
		return se_confv(c, s);
	se *e = s->ptr;
	if (ssunlikely(e->rep_conf->path_backup == NULL)) {
		sr_error(&e->error, "%s", "backup is not enabled");
		return -1;
	}
	return sc_ctl_backup(&e->scheduler);
}

static inline srconf*
se_confbackup(se *e, seconfrt *rt, srconf **pc)
{
	srconf *backup = *pc;
	srconf *p = NULL;
	sr_c(&p, pc, se_confv_offline, "path", SS_STRINGPTR, &e->rep_conf->path_backup);
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
	ss_aopen(&e->a_oom, &ss_ooma, e->ei.oom);
	e->a = e->a_oom;
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
	sr_c(&p, pc, se_confv, "si_branch_0",     SS_U32, &e->ei.e[1]);
	sr_c(&p, pc, se_confv, "si_compaction_0", SS_U32, &e->ei.e[2]);
	sr_c(&p, pc, se_confv, "si_compaction_1", SS_U32, &e->ei.e[3]);
	sr_c(&p, pc, se_confv, "si_compaction_2", SS_U32, &e->ei.e[4]);
	sr_c(&p, pc, se_confv, "si_compaction_3", SS_U32, &e->ei.e[5]);
	sr_c(&p, pc, se_confv, "si_compaction_4", SS_U32, &e->ei.e[6]);
	sr_c(&p, pc, se_confv, "si_recover_0",    SS_U32, &e->ei.e[7]);
	sr_c(&p, pc, se_confv, "si_snapshot_0",   SS_U32, &e->ei.e[8]);
	sr_c(&p, pc, se_confv, "si_snapshot_1",   SS_U32, &e->ei.e[9]);
	sr_c(&p, pc, se_confv, "si_snapshot_2",   SS_U32, &e->ei.e[10]);
	sr_C(&prev, pc, NULL, "error_injection", SS_UNDEF, ei, SR_NS, NULL);
	srconf *debug = prev;
	return sr_C(NULL, pc, NULL, "debug", SS_UNDEF, debug, SR_NS, NULL);
}

static srconf*
se_confprepare(se *e, seconfrt *rt, srconf *c, int serialize)
{
	srconf *pc = c;
	srconf *sophia      = se_confsophia(e, rt, &pc);
	srconf *backup      = se_confbackup(e, rt, &pc);
	srconf *scheduler   = se_confscheduler(e, &pc, serialize);
	srconf *transaction = se_conftransaction(e, rt, &pc);
	srconf *metric      = se_confmetric(e, rt, &pc);
	srconf *log         = se_conflog(e, rt, &pc);
	srconf *db          = se_confdb(e, rt, &pc, serialize);
	srconf *debug       = se_confdebug(e, rt, &pc);

	sophia->next      = backup;
	backup->next      = scheduler;
	scheduler->next   = transaction;
	transaction->next = metric;
	metric->next      = log;
	log->next         = db;
	if (! serialize)
		db->next = debug;
	return sophia;
}

static int
se_confrt(se *e, seconfrt *rt)
{
	/* sophia */
	snprintf(rt->version, sizeof(rt->version),
	         "%d.%d",
	         SR_VERSION_A - '0',
	         SR_VERSION_B - '0');
	snprintf(rt->version_storage, sizeof(rt->version_storage),
	         "%d.%d",
	         SR_VERSION_STORAGE_A - '0',
	         SR_VERSION_STORAGE_B - '0');
	snprintf(rt->build, sizeof(rt->build), "%s",
	         SR_VERSION_COMMIT);

	/* error */
	ss_spinlock(&e->error.lock);
	rt->errors = e->error.errors;
	ss_spinunlock(&e->error.lock);

	/* log */
	rt->log_files = sl_poolfiles(&e->lp);

	/* backup */
	ss_mutexlock(&e->scheduler.lock);
	rt->backup_active        = e->scheduler.backup;
	rt->backup_last          = e->scheduler.backup_bsn_last;
	rt->backup_last_complete = e->scheduler.backup_bsn_last_complete;
	ss_mutexunlock(&e->scheduler.lock);

	/* metric */
	sr_seqlock(&e->seq);
	rt->seq = e->seq;
	sr_sequnlock(&e->seq);

	/* transaction */
	sr_statxm_prepare(&e->xm_stat);
	rt->tx_stat = e->xm_stat;
	rt->tx_ro   = e->xm.count_rd;
	rt->tx_rw   = e->xm.count_rw;
	rt->tx_gc   = e->xm.count_gc;
	rt->tx_vlsn = sx_vlsn(&e->xm);
	return 0;
}

static inline int
se_confensure(seconf *c)
{
	se *e = (se*)c->env;
	int confmax = 2048 + (e->db.n * 100) + c->threads;
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
	sf_schemeinit(&c->scheme);
	c->env     = e;
	c->threads = 6;
	return 0;
}

void se_conffree(seconf *c)
{
	se *e = (se*)c->env;
	if (c->conf) {
		ss_free(&e->a, c->conf);
		c->conf = NULL;
	}
	sf_schemefree(&c->scheme, &e->a);
}

int se_confvalidate(seconf *c)
{
	se *e = (se*)c->env;
	if (e->rep_conf->path == NULL) {
		sr_error(&e->error, "%s", "repository path is not set");
		return -1;
	}
	char path[1024];
	if (e->lp_conf->path == NULL) {
		snprintf(path, sizeof(path), "%s/log", e->rep_conf->path);
		int rc = sl_confset_path(e->lp_conf, &e->a, path);
		if (ssunlikely(rc == -1))
			return sr_oom(&e->error);
	}
	if (e->db.n == 0) {
		sr_error(&e->error, "%s", "no databases are defined");
		return -1;
	}
	return 0;
}
