#ifndef SC_STEP_H_
#define SC_STEP_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

int sc_step(sc*, scworker*, uint64_t);

static inline scdb*
sc_current(sc *s) {
	return &s->i[s->rr];
}

static inline void
sc_next(sc *s) {
	s->rr++;
	if (s->rr >= s->count)
		s->rr = 0;
}

static inline void
sc_task_checkpoint(sc *s, scdb *db)
{
	db->checkpoint_lsn = sr_seq(s->r->seq, SR_LSN);
	db->checkpoint = 1;
}

static inline void
sc_task_checkpoint_done(scdb *db)
{
	db->checkpoint = 0;
	db->checkpoint_lsn_last = db->checkpoint_lsn;
	db->checkpoint_lsn = 0;
}

static inline void
sc_task_anticache(sc *s, scdb *db)
{
	db->anticache = 1;
	db->anticache_storage = db->anticache_limit;
	db->anticache_asn = sr_seq(s->r->seq, SR_ASNNEXT);
}

static inline void
sc_task_anticache_done(scdb *db, uint64_t now)
{
	db->anticache = 0;
	db->anticache_asn_last = db->anticache_asn;
	db->anticache_asn = 0;
	db->anticache_storage = 0;
	db->anticache_time = now;
}

static inline void
sc_task_snapshot(sc *s, scdb *db)
{
	db->snapshot = 1;
	db->snapshot_ssn = sr_seq(s->r->seq, SR_SSNNEXT);
}

static inline void
sc_task_snapshot_done(scdb *db, uint64_t now)
{
	db->snapshot = 0;
	db->snapshot_ssn_last = db->snapshot_ssn;
	db->snapshot_ssn = 0;
	db->snapshot_time = now;
}

static inline void
sc_task_expire(scdb *db)
{
	db->expire = 1;
}

static inline void
sc_task_expire_done(scdb *db, uint64_t now)
{
	db->expire = 0;
	db->expire_time = now;
}

static inline void
sc_task_gc(scdb *db)
{
	db->gc = 1;
}

static inline void
sc_task_gc_done(scdb *db, uint64_t now)
{
	db->gc = 0;
	db->gc_time = now;
}

static inline void
sc_task_lru(scdb *db)
{
	db->lru = 1;
}

static inline void
sc_task_lru_done(scdb *db, uint64_t now)
{
	db->lru = 0;
	db->lru_time = now;
}

static inline void
sc_task_age(scdb *db)
{
	db->age = 1;
}

static inline void
sc_task_age_done(scdb *db, uint64_t now)
{
	db->age = 0;
	db->age_time = now;
}

static inline void
sc_task_backup(scdb *db)
{
	db->backup = 1;
}

static inline void
sc_task_backup_done(scdb *db)
{
	db->backup = 0;
}

#endif
