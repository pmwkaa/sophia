#ifndef SR_SEQ_H_
#define SR_SEQ_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef enum {
	SR_DSN,
	SR_DSNNEXT,
	SR_NSN,
	SR_NSNNEXT,
	SR_LSN,
	SR_LSNNEXT,
	SR_LFSN,
	SR_LFSNNEXT,
	SR_TSN,
	SR_TSNNEXT
} srseqop;

typedef struct {
	srspinlock lock;
	uint32_t dsn;
	uint32_t nsn;
	uint64_t lsn;
	uint32_t lfsn;
	uint32_t tsn;
} srseq;

static inline void
sr_seqinit(srseq *n) {
	memset(n, 0, sizeof(*n));
	sr_spinlockinit(&n->lock);
}

static inline void
sr_seqfree(srseq *n) {
	sr_spinlockfree(&n->lock);
}

static inline void
sr_seqlock(srseq *n) {
	sr_spinlock(&n->lock);
}

static inline void
sr_sequnlock(srseq *n) {
	sr_spinunlock(&n->lock);
}

static inline uint64_t
sr_seqdo(srseq *n, srseqop op)
{
	uint64_t v = 0;
	switch (op) {
	case SR_NSN:      v = n->nsn;
		break;
	case SR_NSNNEXT:  v = ++n->nsn;
		break;
	case SR_DSN:      v = n->dsn;
		break;
	case SR_DSNNEXT:  v = ++n->dsn;
		break;
	case SR_LSN:      v = n->lsn;
		break;
	case SR_LSNNEXT:  v = ++n->lsn;
		break;
	case SR_LFSN:     v = n->lfsn;
		break;
	case SR_LFSNNEXT: v = ++n->lfsn;
		break;
	case SR_TSN:      v = n->tsn;
		break;
	case SR_TSNNEXT:  v = ++n->tsn;
		break;
	}
	return v;
}

static inline uint64_t
sr_seq(srseq *n, srseqop op)
{
	sr_seqlock(n);
	uint64_t v = sr_seqdo(n, op);
	sr_sequnlock(n);
	return v;
}

#endif
