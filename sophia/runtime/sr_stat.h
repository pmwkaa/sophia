#ifndef SR_STAT_H_
#define SR_STAT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srstatxm srstatxm;
typedef struct srstat srstat;

struct srstatxm {
	/* transaction */
	uint64_t tx;
	uint64_t tx_rlb;
	uint64_t tx_conflict;
	uint64_t tx_lock;
	ssavg    tx_latency;
	ssavg    tx_stmts;
};

struct srstat {
	ssspinlock lock;
	/* memory */
	uint64_t v_count;
	uint64_t v_allocated;
	/* field */
	ssavg    field;
	/* set */
	uint64_t set;
	ssavg    set_latency;
	/* delete */
	uint64_t del;
	ssavg    del_latency;
	/* upsert */
	uint64_t upsert;
	ssavg    upsert_latency;
	/* get */
	uint64_t get;
	ssavg    get_read_disk;
	ssavg    get_read_cache;
	ssavg    get_latency;
	/* pread */
	uint64_t pread;
	ssavg    pread_latency;
	/* cursor */
	uint64_t cursor;
	ssavg    cursor_latency;
	ssavg    cursor_read_disk;
	ssavg    cursor_read_cache;
	ssavg    cursor_ops;
};

static inline void
sr_statxm_init(srstatxm *s)
{
	memset(s, 0, sizeof(*s));
}

static inline void
sr_statxm_prepare(srstatxm *s)
{
	ss_avgprepare(&s->tx_latency);
	ss_avgprepare(&s->tx_stmts);
}

static inline void
sr_statxm(srstatxm *s, uint64_t start, uint32_t count,
          int rlb, int conflict)
{
	uint64_t diff = ss_utime() - start;
	s->tx++;
	s->tx_rlb += rlb;
	s->tx_conflict += conflict;
	ss_avgupdate(&s->tx_stmts, count);
	ss_avgupdate(&s->tx_latency, diff);
}

static inline void
sr_statxm_lock(srstatxm *s)
{
	s->tx_lock++;
}

static inline void
sr_statinit(srstat *s)
{
	memset(s, 0, sizeof(*s));
	ss_spinlockinit(&s->lock);
}

static inline void
sr_statfree(srstat *s) {
	ss_spinlockfree(&s->lock);
}

static inline void
sr_statprepare(srstat *s)
{
	ss_avgprepare(&s->field);
	ss_avgprepare(&s->set_latency);
	ss_avgprepare(&s->del_latency);
	ss_avgprepare(&s->upsert_latency);
	ss_avgprepare(&s->get_read_disk);
	ss_avgprepare(&s->get_read_cache);
	ss_avgprepare(&s->get_latency);
	ss_avgprepare(&s->pread_latency);
	ss_avgprepare(&s->cursor_latency);
	ss_avgprepare(&s->cursor_read_disk);
	ss_avgprepare(&s->cursor_read_cache);
	ss_avgprepare(&s->cursor_ops);
}

static inline void
sr_statfield(srstat *s, int size)
{
	ss_spinlock(&s->lock);
	ss_avgupdate(&s->field, size);
	ss_spinunlock(&s->lock);
}

static inline void
sr_statset(srstat *s, uint64_t start)
{
	uint64_t diff = ss_utime() - start;
	ss_spinlock(&s->lock);
	s->set++;
	ss_avgupdate(&s->set_latency, diff);
	ss_spinunlock(&s->lock);
}

static inline void
sr_statdelete(srstat *s, uint64_t start)
{
	uint64_t diff = ss_utime() - start;
	ss_spinlock(&s->lock);
	s->del++;
	ss_avgupdate(&s->del_latency, diff);
	ss_spinunlock(&s->lock);
}

static inline void
sr_statupsert(srstat *s, uint64_t start)
{
	uint64_t diff = ss_utime() - start;
	ss_spinlock(&s->lock);
	s->upsert++;
	ss_avgupdate(&s->upsert_latency, diff);
	ss_spinunlock(&s->lock);
}

static inline void
sr_statget(srstat *s, uint64_t diff, int read_disk, int read_cache)
{
	ss_spinlock(&s->lock);
	s->get++;
	ss_avgupdate(&s->get_read_disk, read_disk);
	ss_avgupdate(&s->get_read_cache, read_cache);
	ss_avgupdate(&s->get_latency, diff);
	ss_spinunlock(&s->lock);
}

static inline void
sr_statpread(srstat *s, uint64_t start)
{
	uint64_t diff = ss_utime() - start;
	ss_spinlock(&s->lock);
	s->pread++;
	ss_avgupdate(&s->pread_latency, diff);
	ss_spinunlock(&s->lock);
}

static inline void
sr_statcursor(srstat *s, uint64_t start, int read_disk, int read_cache, int ops)
{
	uint64_t diff = ss_utime() - start;
	ss_spinlock(&s->lock);
	s->cursor++;
	ss_avgupdate(&s->cursor_read_disk, read_disk);
	ss_avgupdate(&s->cursor_read_cache, read_cache);
	ss_avgupdate(&s->cursor_latency, diff);
	ss_avgupdate(&s->cursor_ops, ops);
	ss_spinunlock(&s->lock);
}

static inline void
sr_statcopy(srstat *s, srstat *dest)
{
	ss_spinlock(&s->lock);
	*dest = *s;
	ss_spinunlock(&s->lock);
}

#endif
