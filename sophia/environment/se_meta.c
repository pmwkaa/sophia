
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
#include <libse.h>

static inline int
se_metav(srmeta *c, srmetastmt *s)
{
	switch (s->op) {
	case SR_SERIALIZE: return sr_meta_serialize(c, s);
	case SR_READ:      return sr_meta_read(c, s);
	case SR_WRITE:     return sr_meta_write(c, s);
	}
	assert(0);
	return -1;
}

static inline int
se_metav_offline(srmeta *c, srmetastmt *s)
{
	se *e = s->ptr;
	if (s->op == SR_WRITE) {
		if (se_status(&e->status)) {
			sr_error(s->r->e, "write to %s is offline-only", s->path);
			return -1;
		}
	}
	return se_metav(c, s);
}

static inline int
se_metasophia_error(srmeta *c, srmetastmt *s)
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
	srmeta meta = {
		.key      = c->key,
		.flags    = c->flags,
		.type     = c->type,
		.function = NULL,
		.value    = errorp,
		.ptr      = NULL,
		.next     = NULL
	};
	return se_metav(&meta, s);
}

static inline srmeta*
se_metasophia(se *e, semetart *rt, srmeta **pc)
{
	srmeta *sophia = *pc;
	srmeta *p = NULL;
	sr_M(&p, pc, se_metav, "version", SS_STRING, rt->version, SR_RO, NULL);
	sr_M(&p, pc, se_metav, "build", SS_STRING, rt->build, SR_RO, NULL);
	sr_M(&p, pc, se_metasophia_error, "error", SS_STRING, NULL, SR_RO, NULL);
	sr_m(&p, pc, se_metav_offline, "path", SS_STRINGPTR, &e->meta.path);
	sr_m(&p, pc, se_metav_offline, "path_create", SS_U32, &e->meta.path_create);
	return sr_M(NULL, pc, NULL, "sophia", SS_UNDEF, sophia, SR_NS, NULL);
}

static inline srmeta*
se_metamemory(se *e, semetart *rt, srmeta **pc)
{
	srmeta *memory = *pc;
	srmeta *p = NULL;
	sr_m(&p, pc, se_metav_offline, "limit", SS_U64, &e->meta.memory_limit);
	sr_M(&p, pc, se_metav, "used", SS_U64, &rt->memory_used, SR_RO, NULL);
	return sr_M(NULL, pc, NULL, "memory", SS_UNDEF, memory, SR_NS, NULL);
}

static inline int
se_metacompaction_set(srmeta *c ssunused, srmetastmt *s)
{
	se *e = s->ptr;
	if (s->op != SR_WRITE) {
		sr_error(&e->error, "%s", "bad operation");
		return -1;
	}
	if (ssunlikely(se_statusactive(&e->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	/* validate argument */
	uint32_t percent = *(uint32_t*)s->value;
	if (percent > 100) {
		sr_error(&e->error, "%s", "bad argument");
		return -1;
	}
	srzone z;
	memset(&z, 0, sizeof(z));
	z.enable = 1;
	sr_zonemap_set(&e->meta.zones, percent, &z);
	return 0;
}

static inline srmeta*
se_metacompaction(se *e, semetart *rt ssunused, srmeta **pc)
{
	srmeta *compaction = *pc;
	srmeta *prev;
	srmeta *p = NULL;
	sr_m(&p, pc, se_metav_offline, "node_size", SS_U32, &e->meta.node_size);
	sr_m(&p, pc, se_metav_offline, "page_size", SS_U32, &e->meta.page_size);
	sr_m(&p, pc, se_metav_offline, "page_checksum", SS_U32, &e->meta.page_checksum);
	prev = p;
	int i = 0;
	for (; i < 11; i++) {
		srzone *z = &e->meta.zones.zones[i];
		if (! z->enable)
			continue;
		srmeta *zone = *pc;
		p = NULL;
		sr_m(&p, pc, se_metav_offline, "mode", SS_U32, &z->mode);
		sr_m(&p, pc, se_metav_offline, "compact_wm", SS_U32, &z->compact_wm);
		sr_m(&p, pc, se_metav_offline, "branch_prio", SS_U32, &z->branch_prio);
		sr_m(&p, pc, se_metav_offline, "branch_wm", SS_U32, &z->branch_wm);
		sr_m(&p, pc, se_metav_offline, "branch_age", SS_U32, &z->branch_age);
		sr_m(&p, pc, se_metav_offline, "branch_age_period", SS_U32, &z->branch_age_period);
		sr_m(&p, pc, se_metav_offline, "branch_age_wm", SS_U32, &z->branch_age_wm);
		sr_m(&p, pc, se_metav_offline, "backup_prio", SS_U32, &z->backup_prio);
		sr_m(&p, pc, se_metav_offline, "gc_wm", SS_U32, &z->gc_wm);
		sr_m(&p, pc, se_metav_offline, "gc_db_prio", SS_U32, &z->gc_db_prio);
		sr_m(&p, pc, se_metav_offline, "gc_prio", SS_U32, &z->gc_prio);
		sr_m(&p, pc, se_metav_offline, "gc_period", SS_U32, &z->gc_period);
		sr_m(&p, pc, se_metav_offline, "async", SS_U32, &z->async);
		sr_M(&prev, pc, NULL, z->name, SS_UNDEF, zone, SR_NS, NULL);
	}
	return sr_M(NULL, pc, se_metacompaction_set, "compaction", SS_U32,
	            compaction, SR_NS, NULL);
}

static inline int
se_metascheduler_trace(srmeta *c, srmetastmt *s)
{
	seworker *w = c->value;
	char tracesz[128];
	char *trace;
	int tracelen = ss_tracecopy(&w->trace, tracesz, sizeof(tracesz));
	if (sslikely(tracelen == 0))
		trace = NULL;
	else
		trace = tracesz;
	srmeta meta = {
		.key      = c->key,
		.flags    = c->flags,
		.type     = c->type,
		.function = NULL,
		.value    = trace,
		.ptr      = NULL,
		.next     = NULL
	};
	return se_metav(&meta, s);
}

static inline int
se_metascheduler_checkpoint(srmeta *c, srmetastmt *s)
{
	if (s->op != SR_WRITE)
		return se_metav(c, s);
	se *e = s->ptr;
	return se_scheduler_checkpoint(e);
}

static inline int
se_metascheduler_on_recover(srmeta *c, srmetastmt *s)
{
	se *e = s->ptr;
	if (s->op != SR_WRITE)
		return se_metav(c, s);
	if (ssunlikely(se_statusactive(&e->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	e->meta.on_recover.function =
		(serecovercbf)(uintptr_t)s->value;
	return 0;
}

static inline int
se_metascheduler_on_recover_arg(srmeta *c, srmetastmt *s)
{
	se *e = s->ptr;
	if (s->op != SR_WRITE)
		return se_metav(c, s);
	if (ssunlikely(se_statusactive(&e->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	e->meta.on_recover.arg = s->value;
	return 0;
}

static inline int
se_metascheduler_on_event(srmeta *c, srmetastmt *s)
{
	se *e = s->ptr;
	if (s->op != SR_WRITE)
		return se_metav(c, s);
	if (ssunlikely(se_statusactive(&e->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	ss_triggerset(&e->meta.on_event, s->value);
	return 0;
}

static inline int
se_metascheduler_on_event_arg(srmeta *c, srmetastmt *s)
{
	se *e = s->ptr;
	if (s->op != SR_WRITE)
		return se_metav(c, s);
	if (ssunlikely(se_statusactive(&e->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	ss_triggerset_arg(&e->meta.on_event, s->value);
	return 0;
}

static inline int
se_metascheduler_gc(srmeta *c, srmetastmt *s)
{
	if (s->op != SR_WRITE)
		return se_metav(c, s);
	se *e = s->ptr;
	return se_scheduler_gc(e);
}

static inline int
se_metascheduler_run(srmeta *c, srmetastmt *s)
{
	if (s->op != SR_WRITE)
		return se_metav(c, s);
	se *e = s->ptr;
	return se_scheduler_call(e);
}

static inline srmeta*
se_metascheduler(se *e, semetart *rt, srmeta **pc)
{
	srmeta *scheduler = *pc;
	srmeta *prev;
	srmeta *p = NULL;
	sr_m(&p, pc, se_metav_offline, "threads", SS_U32, &e->meta.threads);
	sr_M(&p, pc, se_metav, "zone", SS_STRING, rt->zone, SR_RO, NULL);
	sr_M(&p, pc, se_metav, "checkpoint_active", SS_U32, &rt->checkpoint_active, SR_RO, NULL);
	sr_M(&p, pc, se_metav, "checkpoint_lsn", SS_U64, &rt->checkpoint_lsn, SR_RO, NULL);
	sr_M(&p, pc, se_metav, "checkpoint_lsn_last", SS_U64, &rt->checkpoint_lsn_last, SR_RO, NULL);
	sr_m(&p, pc, se_metascheduler_checkpoint, "checkpoint",  SS_FUNCTION, NULL);
	sr_m(&p, pc, se_metascheduler_on_recover, "on_recover", SS_STRING, NULL);
	sr_m(&p, pc, se_metascheduler_on_recover_arg, "on_recover_arg", SS_STRING, NULL);
	sr_m(&p, pc, se_metascheduler_on_event, "on_event", SS_STRING, NULL);
	sr_m(&p, pc, se_metascheduler_on_event_arg, "on_event_arg", SS_STRING, NULL);
	sr_m(&p, pc, se_metav_offline, "event_on_backup", SS_U32, &e->meta.event_on_backup);
	sr_M(&p, pc, se_metav, "gc_active", SS_U32, &rt->gc_active, SR_RO, NULL);
	sr_m(&p, pc, se_metascheduler_gc, "gc", SS_FUNCTION, NULL);
	sr_M(&p, pc, se_metav, "reqs", SS_U32, &rt->reqs, SR_RO, NULL);
	sr_m(&p, pc, se_metascheduler_run, "run", SS_FUNCTION, NULL);
	prev = p;
	sslist *i;
	ss_listforeach(&e->sched.workers.list, i) {
		seworker *w = sscast(i, seworker, link);
		srmeta *worker = *pc;
		p = NULL;
		sr_M(&p, pc, se_metascheduler_trace, "trace", SS_STRING, w, SR_RO, NULL);
		sr_M(&prev, pc, NULL, w->name, SS_UNDEF, worker, SR_NS, NULL);
	}
	return sr_M(NULL, pc, NULL, "scheduler", SS_UNDEF, scheduler, SR_NS, NULL);
}

static inline int
se_metalog_rotate(srmeta *c, srmetastmt *s)
{
	if (s->op != SR_WRITE)
		return se_metav(c, s);
	se *e = s->ptr;
	return sl_poolrotate(&e->lp);
}

static inline int
se_metalog_gc(srmeta *c, srmetastmt *s)
{
	if (s->op != SR_WRITE)
		return se_metav(c, s);
	se *e = s->ptr;
	return sl_poolgc(&e->lp);
}

static inline srmeta*
se_metalog(se *e, semetart *rt, srmeta **pc)
{
	srmeta *log = *pc;
	srmeta *p = NULL;
	sr_m(&p, pc, se_metav_offline, "enable", SS_U32, &e->meta.log_enable);
	sr_m(&p, pc, se_metav_offline, "path", SS_STRINGPTR, &e->meta.log_path);
	sr_m(&p, pc, se_metav_offline, "sync", SS_U32, &e->meta.log_sync);
	sr_m(&p, pc, se_metav_offline, "rotate_wm", SS_U32, &e->meta.log_rotate_wm);
	sr_m(&p, pc, se_metav_offline, "rotate_sync", SS_U32, &e->meta.log_rotate_sync);
	sr_m(&p, pc, se_metalog_rotate, "rotate", SS_FUNCTION, NULL);
	sr_m(&p, pc, se_metalog_gc, "gc", SS_FUNCTION, NULL);
	sr_M(&p, pc, se_metav, "files", SS_U32, &rt->log_files, SR_RO, NULL);
	sr_m(&p, pc, se_metav_offline, "two_phase_recover", SS_U32, &e->meta.two_phase_recover);
	sr_m(&p, pc, se_metav_offline, "commit_lsn", SS_U32, &e->meta.commit_lsn);
	return sr_M(NULL, pc, NULL, "log", SS_UNDEF, log, SR_NS, NULL);
}

static inline srmeta*
se_metametric(se *e ssunused, semetart *rt, srmeta **pc)
{
	srmeta *metric = *pc;
	srmeta *p = NULL;
	sr_M(&p, pc, se_metav, "dsn",  SS_U32, &rt->seq.dsn, SR_RO, NULL);
	sr_M(&p, pc, se_metav, "nsn",  SS_U32, &rt->seq.nsn, SR_RO, NULL);
	sr_M(&p, pc, se_metav, "bsn",  SS_U32, &rt->seq.bsn, SR_RO, NULL);
	sr_M(&p, pc, se_metav, "lsn",  SS_U64, &rt->seq.lsn, SR_RO, NULL);
	sr_M(&p, pc, se_metav, "lfsn", SS_U32, &rt->seq.lfsn, SR_RO, NULL);
	sr_M(&p, pc, se_metav, "tsn",  SS_U32, &rt->seq.tsn, SR_RO, NULL);
	return sr_M(NULL, pc, NULL, "metric", SS_UNDEF, metric, SR_NS, NULL);
}

static inline int
se_metadb_set(srmeta *c ssunused, srmetastmt *s)
{
	/* set(db) */
	se *e = s->ptr;
	if (s->op != SR_WRITE) {
		sr_error(&e->error, "%s", "bad operation");
		return -1;
	}
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

static inline int
se_metadb_get(srmeta *c, srmetastmt *s)
{
	/* get(db.name) */
	se *e = s->ptr;
	if (s->op != SR_READ) {
		sr_error(&e->error, "%s", "bad operation");
		return -1;
	}
	assert(c->ptr != NULL);
	sedb *db = c->ptr;
	se_dbref(db, 0);
	*(void**)s->value = db;
	return 0;
}

static inline int
se_metadb_update(srmeta *c, srmetastmt *s)
{
	if (s->op != SR_WRITE)
		return se_metav(c, s);
	sedb *db = c->ptr;
	if (ssunlikely(se_statusactive(&db->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	/* set update function */
	sfupdatef update = (sfupdatef)(uintptr_t)s->value;
	sf_updateset(&db->scheme.fmt_update, update);
	return 0;
}

static inline int
se_metadb_updatearg(srmeta *c, srmetastmt *s)
{
	if (s->op != SR_WRITE)
		return se_metav(c, s);
	sedb *db = c->ptr;
	if (ssunlikely(se_statusactive(&db->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	sf_updateset_arg(&db->scheme.fmt_update, s->value);
	return 0;
}

static inline int
se_metadb_status(srmeta *c, srmetastmt *s)
{
	sedb *db = c->value;
	char *status = se_statusof(&db->status);
	srmeta meta = {
		.key      = c->key,
		.flags    = c->flags,
		.type     = c->type,
		.function = NULL,
		.value    = status,
		.ptr      = NULL,
		.next     = NULL
	};
	return se_metav(&meta, s);
}

static inline int
se_metadb_branch(srmeta *c, srmetastmt *s)
{
	if (s->op != SR_WRITE)
		return se_metav(c, s);
	sedb *db = c->value;
	return se_scheduler_branch(db);
}

static inline int
se_metadb_compact(srmeta *c, srmetastmt *s)
{
	if (s->op != SR_WRITE)
		return se_metav(c, s);
	sedb *db = c->value;
	return se_scheduler_compact(db);
}

static inline int
se_metadb_deadlock(srmeta *c, srmetastmt *s)
{
	if (s->op != SR_WRITE)
		return se_metav(c, s);
	if (s->valuetype != SS_OBJECT) {
		sr_error(s->r->e, "%s", "deadlock(transaction) expected");
		return -1;
	}
	setx *tx = se_cast(s->value, setx*, SETX);
	int rc = sx_deadlock(&tx->t);
	return rc;
}

static inline int
se_metav_dboffline(srmeta *c, srmetastmt *s)
{
	sedb *db = c->ptr;
	if (s->op == SR_WRITE) {
		if (se_status(&db->status)) {
			sr_error(s->r->e, "write to %s is offline-only", s->path);
			return -1;
		}
	}
	return se_metav(c, s);
}

static inline int
se_metadb_index(srmeta *c ssunused, srmetastmt *s)
{
	/* set(index, key) */
	sedb *db = c->ptr;
	se *e = se_of(&db->o);
	if (s->op != SR_WRITE) {
		sr_error(&e->error, "%s", "bad operation");
		return -1;
	}
	if (ssunlikely(se_statusactive(&db->status))) {
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
se_metadb_key(srmeta *c, srmetastmt *s)
{
	sedb *db = c->ptr;
	se *e = se_of(&db->o);
	if (s->op != SR_WRITE)
		return se_metav(c, s);
	if (ssunlikely(se_statusactive(&db->status))) {
		sr_error(s->r->e, "write to %s is offline-only", s->path);
		return -1;
	}
	char *path = s->value;
	/* update key-part path */
	srkey *part = sr_schemefind(&db->scheme.scheme, c->key);
	assert(part != NULL);
	return sr_keyset(part, &e->a, path);
}

static inline srmeta*
se_metadb(se *e, semetart *rt ssunused, srmeta **pc)
{
	srmeta *db = NULL;
	srmeta *prev = NULL;
	srmeta *p;
	sslist *i;
	ss_listforeach(&e->db.list, i)
	{
		sedb *o = (sedb*)sscast(i, so, link);
		si_profilerbegin(&o->rtp, &o->index);
		si_profiler(&o->rtp);
		si_profilerend(&o->rtp);
		/* database index */
		srmeta *index = *pc;
		p = NULL;
		sr_M(&p, pc, se_metav, "memory_used", SS_U64, &o->rtp.memory_used, SR_RO, NULL);
		sr_M(&p, pc, se_metav, "node_count", SS_U32, &o->rtp.total_node_count, SR_RO, NULL);
		sr_M(&p, pc, se_metav, "node_size", SS_U64, &o->rtp.total_node_size, SR_RO, NULL);
		sr_M(&p, pc, se_metav, "node_origin_size", SS_U64, &o->rtp.total_node_origin_size, SR_RO, NULL);
		sr_M(&p, pc, se_metav, "count", SS_U64, &o->rtp.count, SR_RO, NULL);
		sr_M(&p, pc, se_metav, "count_dup", SS_U64, &o->rtp.count_dup, SR_RO, NULL);
		sr_M(&p, pc, se_metav, "read_disk", SS_U64, &o->rtp.read_disk, SR_RO, NULL);
		sr_M(&p, pc, se_metav, "read_cache", SS_U64, &o->rtp.read_cache, SR_RO, NULL);
		sr_M(&p, pc, se_metav, "branch_count", SS_U32, &o->rtp.total_branch_count, SR_RO, NULL);
		sr_M(&p, pc, se_metav, "branch_avg", SS_U32, &o->rtp.total_branch_avg, SR_RO, NULL);
		sr_M(&p, pc, se_metav, "branch_max", SS_U32, &o->rtp.total_branch_max, SR_RO, NULL);
		sr_M(&p, pc, se_metav, "branch_histogram", SS_STRINGPTR, &o->rtp.histogram_branch_ptr, SR_RO, NULL);
		sr_M(&p, pc, se_metav, "page_count", SS_U32, &o->rtp.total_page_count, SR_RO, NULL);
		sr_M(&p, pc, se_metadb_update, "update", SS_STRING, NULL, 0, o);
		sr_M(&p, pc, se_metadb_updatearg, "update_arg", SS_STRING, NULL, 0, o);
		/* index keys */
		int i = 0;
		while (i < o->scheme.scheme.count) {
			srkey *part = sr_schemeof(&o->scheme.scheme, i);
			sr_M(&p, pc, se_metadb_key, part->name, SS_STRING, part->path, 0, o);
			i++;
		}
		/* database */
		srmeta *database = *pc;
		p = NULL;
		sr_M(&p, pc, se_metav, "name", SS_STRINGPTR, &o->scheme.name, SR_RO, NULL);
		sr_M(&p, pc, se_metav_dboffline, "id", SS_U32, &o->scheme.id, 0, o);
		sr_M(&p, pc, se_metadb_status,   "status", SS_STRING, o, SR_RO, NULL);
		sr_M(&p, pc, se_metav_dboffline, "format", SS_STRINGPTR, &o->scheme.fmt_sz, 0, o);
		sr_M(&p, pc, se_metav_dboffline, "path", SS_STRINGPTR, &o->scheme.path, 0, o);
		sr_M(&p, pc, se_metav_dboffline, "path_fail_on_exists", SS_U32, &o->scheme.path_fail_on_exists, 0, o);
		sr_M(&p, pc, se_metav_dboffline, "path_fail_on_drop", SS_U32, &o->scheme.path_fail_on_drop, 0, o);
		sr_M(&p, pc, se_metav_dboffline, "sync", SS_U32, &o->scheme.sync, 0, o);
		sr_M(&p, pc, se_metav_dboffline, "mmap", SS_U32, &o->scheme.mmap, 0, o);
		sr_M(&p, pc, se_metav_dboffline, "in_memory", SS_U32, &o->scheme.in_memory, 0, o);
		sr_M(&p, pc, se_metav_dboffline, "compression_key", SS_U32, &o->scheme.compression_key, 0, o);
		sr_M(&p, pc, se_metav_dboffline, "compression", SS_STRINGPTR, &o->scheme.compression_sz, 0, o);
		sr_m(&p, pc, se_metadb_branch, "branch", SS_FUNCTION, o);
		sr_m(&p, pc, se_metadb_compact, "compact", SS_FUNCTION, o);
		sr_m(&p, pc, se_metadb_deadlock, "deadlock", SS_FUNCTION, NULL);
		sr_M(&p, pc, se_metadb_index, "index", SS_UNDEF, index, SR_NS, o);
		sr_M(&prev, pc, se_metadb_get, o->scheme.name, SS_STRING, database, SR_NS, o);
		if (db == NULL)
			db = prev;
	}
	return sr_M(NULL, pc, se_metadb_set, "db", SS_STRING, db, SR_NS, NULL);
}

static inline int
se_metasnapshot_set(srmeta *c, srmetastmt *s)
{
	if (s->op != SR_WRITE)
		return se_metav(c, s);
	se *e = s->ptr;
	char *name = s->value;
	uint64_t lsn = sr_seq(&e->seq, SR_LSN);
	/* create snapshot object */
	sesnapshot *snapshot =
		(sesnapshot*)se_snapshotnew(e, lsn, name);
	if (ssunlikely(snapshot == NULL))
		return -1;
	so_listadd(&e->snapshot, &snapshot->o);
	return 0;
}

static inline int
se_metasnapshot_lsn(srmeta *c, srmetastmt *s)
{
	int rc = se_metav(c, s);
	if (ssunlikely(rc == -1))
		return -1;
	if (s->op != SR_WRITE)
		return 0;
	sesnapshot *snapshot = c->ptr;
	se_snapshotupdate(snapshot);
	return 0;
}

static inline int
se_metasnapshot_get(srmeta *c, srmetastmt *s)
{
	/* get(snapshot.name) */
	se *e = s->ptr;
	if (s->op != SR_READ) {
		sr_error(&e->error, "%s", "bad operation");
		return -1;
	}
	assert(c->ptr != NULL);
	*(void**)s->value = c->ptr;
	return 0;
}

static inline srmeta*
se_metasnapshot(se *e, semetart *rt ssunused, srmeta **pc)
{
	srmeta *snapshot = NULL;
	srmeta *prev = NULL;
	sslist *i;
	ss_listforeach(&e->snapshot.list, i)
	{
		sesnapshot *s = (sesnapshot*)sscast(i, so, link);
		srmeta *p = sr_M(NULL, pc, se_metasnapshot_lsn, "lsn", SS_U64, &s->vlsn, 0, s);
		sr_M(&prev, pc, se_metasnapshot_get, s->name, SS_STRING, p, SR_NS, s);
		if (snapshot == NULL)
			snapshot = prev;
	}
	return sr_M(NULL, pc, se_metasnapshot_set, "snapshot", SS_STRING,
	            snapshot, SR_NS, NULL);
}

static inline int
se_metabackup_run(srmeta *c, srmetastmt *s)
{
	if (s->op != SR_WRITE)
		return se_metav(c, s);
	se *e = s->ptr;
	return se_scheduler_backup(e);
}

static inline srmeta*
se_metabackup(se *e, semetart *rt, srmeta **pc)
{
	srmeta *backup = *pc;
	srmeta *p = NULL;
	sr_m(&p, pc, se_metav_offline, "path", SS_STRINGPTR, &e->meta.backup_path);
	sr_m(&p, pc, se_metabackup_run, "run", SS_FUNCTION, NULL);
	sr_M(&p, pc, se_metav, "active", SS_U32, &rt->backup_active, SR_RO, NULL);
	sr_m(&p, pc, se_metav, "last", SS_U32, &rt->backup_last);
	sr_m(&p, pc, se_metav, "last_complete", SS_U32, &rt->backup_last_complete);
	return sr_M(NULL, pc, NULL, "backup", 0, backup, SR_NS, NULL);
}

static inline srmeta*
se_metadebug(se *e, semetart *rt ssunused, srmeta **pc)
{
	srmeta *prev = NULL;
	srmeta *p = NULL;
	prev = p;
	srmeta *ei = *pc;
	sr_m(&p, pc, se_metav, "sd_build_0",      SS_U32, &e->ei.e[0]);
	sr_m(&p, pc, se_metav, "sd_build_1",      SS_U32, &e->ei.e[1]);
	sr_m(&p, pc, se_metav, "si_branch_0",     SS_U32, &e->ei.e[2]);
	sr_m(&p, pc, se_metav, "si_compaction_0", SS_U32, &e->ei.e[3]);
	sr_m(&p, pc, se_metav, "si_compaction_1", SS_U32, &e->ei.e[4]);
	sr_m(&p, pc, se_metav, "si_compaction_2", SS_U32, &e->ei.e[5]);
	sr_m(&p, pc, se_metav, "si_compaction_3", SS_U32, &e->ei.e[6]);
	sr_m(&p, pc, se_metav, "si_compaction_4", SS_U32, &e->ei.e[7]);
	sr_m(&p, pc, se_metav, "si_recover_0",    SS_U32, &e->ei.e[8]);
	sr_M(&prev, pc, se_metadb_set, "error_injection", SS_UNDEF, ei, SR_NS, NULL);
	srmeta *debug = prev;
	return sr_M(NULL, pc, NULL, "debug", SS_UNDEF, debug, SR_NS, NULL);
}

static srmeta*
se_metaprepare(se *e, semetart *rt, srmeta *c, int serialize)
{
	/* sophia */
	srmeta *pc = c;
	srmeta *sophia     = se_metasophia(e, rt, &pc);
	srmeta *memory     = se_metamemory(e, rt, &pc);
	srmeta *compaction = se_metacompaction(e, rt, &pc);
	srmeta *scheduler  = se_metascheduler(e, rt, &pc);
	srmeta *metric     = se_metametric(e, rt, &pc);
	srmeta *log        = se_metalog(e, rt, &pc);
	srmeta *snapshot   = se_metasnapshot(e, rt, &pc);
	srmeta *backup     = se_metabackup(e, rt, &pc);
	srmeta *db         = se_metadb(e, rt, &pc);
	srmeta *debug      = se_metadebug(e, rt, &pc);

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
se_metart(se *e, semetart *rt)
{
	/* sophia */
	snprintf(rt->version, sizeof(rt->version),
	         "%d.%d.%d",
	         SR_VERSION_A - '0',
	         SR_VERSION_B - '0',
	         SR_VERSION_C - '0');
	snprintf(rt->build, sizeof(rt->build), "%s",
	         SR_VERSION_COMMIT);

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
	rt->reqs = se_reqcount(e);

	ss_mutexlock(&e->reqlock);
	rt->reqs = e->req.n + e->reqactive.n + e->reqready.n;
	ss_mutexunlock(&e->reqlock);

	int v = ss_quotaused_percent(&e->quota);
	srzone *z = sr_zonemap(&e->meta.zones, v);
	memcpy(rt->zone, z->name, sizeof(rt->zone));

	/* log */
	rt->log_files = sl_poolfiles(&e->lp);

	/* metric */
	sr_seqlock(&e->seq);
	rt->seq = e->seq;
	sr_sequnlock(&e->seq);
	return 0;
}

int se_metaserialize(semeta *c, ssbuf *buf)
{
	se *e = (se*)c->env;
	semetart rt;
	se_metart(e, &rt);
	srmeta meta[1024];
	srmeta *root;
	root = se_metaprepare(e, &rt, meta, 1);
	srmetastmt stmt = {
		.op        = SR_SERIALIZE,
		.path      = NULL,
		.value     = NULL,
		.valuesize = 0,
		.valuetype = SS_UNDEF,
		.serialize = buf,
		.ptr       = e,
		.r         = &e->r
	};
	return sr_metaexec(root, &stmt);
}

static int
se_metaquery(se *e, int op, const char *path,
             sstype valuetype, void *value, int valuesize,
             int *size)
{
	semetart rt;
	se_metart(e, &rt);
	srmeta meta[1024];
	srmeta *root;
	root = se_metaprepare(e, &rt, meta, 0);
	srmetastmt stmt = {
		.op        = op,
		.path      = path,
		.value     = value,
		.valuesize = valuesize,
		.valuetype = valuetype,
		.serialize = NULL,
		.ptr       = e,
		.r         = &e->r
	};
	int rc = sr_metaexec(root, &stmt);
	if (size)
		*size = stmt.valuesize;
	return rc;
}

int se_metaset_object(so *o, const char *path, void *object)
{
	se *e = se_of(o);
	return se_metaquery(e, SR_WRITE, path, SS_OBJECT,
	                    object, sizeof(so*), NULL);
}

int se_metaset_string(so *o, const char *path, void *string, int size)
{
	se *e = se_of(o);
	if (string && size == 0)
		size = strlen(string) + 1;
	return se_metaquery(e, SR_WRITE, path, SS_STRING,
	                   string, size, NULL);
}

int se_metaset_int(so *o, const char *path, int64_t v)
{
	se *e = se_of(o);
	return se_metaquery(e, SR_WRITE, path, SS_I64,
	                    &v, sizeof(v), NULL);
}

void *se_metaget_object(so *o, const char *path)
{
	se *e = se_of(o);
	if (path == NULL)
		return se_metacursor_new(o);
	void *result = NULL;
	int rc = se_metaquery(e, SR_READ, path, SS_OBJECT,
	                      &result, sizeof(void*), NULL);
	if (ssunlikely(rc == -1))
		return NULL;
	return result;
}

void *se_metaget_string(so *o, const char *path, int *size)
{
	se *e = se_of(o);
	void *result = NULL;
	int rc = se_metaquery(e, SR_READ, path, SS_STRING,
	                      &result, sizeof(void*), size);
	if (ssunlikely(rc == -1))
		return NULL;
	return result;
}

int64_t se_metaget_int(so *o, const char *path)
{
	se *e = se_of(o);
	int64_t result = 0;
	int rc = se_metaquery(e, SR_READ, path, SS_I64,
	                      &result, sizeof(void*), NULL);
	if (ssunlikely(rc == -1))
		return -1;
	return result;
}

void se_metainit(semeta *c, so *e)
{
	se *o = (se*)e;
	sr_schemeinit(&c->scheme);
	srkey *part = sr_schemeadd(&c->scheme, &o->a);
	sr_keysetname(part, &o->a, "key");
	sr_keyset(part, &o->a, "string");
	c->env                 = e;
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
	c->on_recover.function = NULL;
	c->on_recover.arg      = NULL;
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
	sr_zonemap_set(&o->meta.zones,  0, &def);
	sr_zonemap_set(&o->meta.zones, 80, &redzone);
	c->backup_path = NULL;
}

void se_metafree(semeta *c)
{
	se *e = (se*)c->env;
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

int se_metavalidate(semeta *c)
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
		srzone *z = &e->meta.zones.zones[i];
		if (! z->enable)
			continue;
		if (z->compact_wm <= 1) {
			sr_error(&e->error, "bad %d.compact_wm value", i * 10);
			return -1;
		}
	}
	return 0;
}
