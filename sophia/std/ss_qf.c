
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

/*
 * Quotient Filter.
 *
 * Based on implementation made by Vedant Kumar <vsk@berkeley.edu>
*/

#include <libss.h>

#define ss_qflmask(n) ((1ULL << (n)) - 1ULL)

int ss_qfinit(ssqf *f)
{
	memset(f, 0, sizeof(*f));
	ss_bufinit(&f->qf_buf);
	return 0;
}

int ss_qfensure(ssqf *f, ssa *a, uint32_t count)
{
	int q = 6;
	int r = 1;
	while (q < 32) {
		if (count < (1UL << q))
			break;
		q++;
	}
	assert(! (q == 0 || r == 0 || q + r > 64));
	f->qf_qbits      = q;
	f->qf_rbits      = r;
	f->qf_elem_bits  = f->qf_rbits + 3;
	f->qf_index_mask = ss_qflmask(q);
	f->qf_rmask      = ss_qflmask(r);
	f->qf_elem_mask  = ss_qflmask(f->qf_elem_bits);
	f->qf_entries    = 0; 
	f->qf_max_size   = 1 << q;
	f->qf_table_size = ((1 << q) * (r + 3)) / 8;
	if (f->qf_table_size % 8)
		f->qf_table_size++;
	int rc = ss_bufensure(&f->qf_buf, a, f->qf_table_size);
	if (ssunlikely(rc == -1))
		return -1;
	ss_bufadvance(&f->qf_buf, f->qf_table_size);
	f->qf_table = (uint64_t*)f->qf_buf.s;
	memset(f->qf_table, 0, f->qf_table_size);
	return 0;
}

void ss_qffree(ssqf *f, ssa *a)
{
	if (f->qf_table) {
		ss_buffree(&f->qf_buf, a);
		f->qf_table = NULL;
	}
}

void ss_qfreset(ssqf *f)
{
	memset(f->qf_table, 0, f->qf_table_size);
	ss_bufreset(&f->qf_buf);
	f->qf_entries = 0;
}

static inline uint64_t
ss_qfincr(ssqf *f, uint64_t idx) {
	return (idx + 1) & f->qf_index_mask;
}

static inline uint64_t
ss_qfdecr(ssqf *f, uint64_t idx) {
	return (idx - 1) & f->qf_index_mask;
}

static inline int
ss_qfoccupied_is(uint64_t elt) {
	return elt & 1;
}

static inline uint64_t
ss_qfoccupied_set(uint64_t elt) {
	return elt | 1;
}

static inline uint64_t
ss_qfoccupied_clr(uint64_t elt) {
	return elt & ~1;
}

static inline int
ss_qfcontinuation_is(uint64_t elt) {
	return elt & 2;
}

static inline uint64_t
ss_qfcontinuation_set(uint64_t elt) {
	return elt | 2;
}

static inline int
ss_qfshifted_is(uint64_t elt) {
	return elt & 4;
}

static inline uint64_t
ss_qfshifted_set(uint64_t elt) {
	return elt | 4;
}

static inline uint64_t
ss_qfshifted_clr(uint64_t elt) {
	return elt & ~4;
}

static inline int
ss_qfremainder_of(uint64_t elt) {
	return elt >> 3;
}

static inline int
ss_qfis_empty(uint64_t elt) {
	return (elt & 7) == 0;
}

static inline uint64_t
ss_qfhash_to_q(ssqf *f, uint64_t h) {
	return (h >> f->qf_rbits) & f->qf_index_mask;
}

static inline uint64_t
ss_qfhash_to_r(ssqf *f, uint64_t h) {
	return h & f->qf_rmask;
}

static inline uint64_t
ss_qfget(ssqf *f, uint64_t idx)
{
	size_t bitpos  = f->qf_elem_bits * idx;
	size_t tabpos  = bitpos / 64;
	size_t slotpos = bitpos % 64;
	int spillbits  = (slotpos + f->qf_elem_bits) - 64;
	uint64_t elt;
	elt = (f->qf_table[tabpos] >> slotpos) & f->qf_elem_mask;
	if (spillbits > 0) {
		tabpos++;
		uint64_t x = f->qf_table[tabpos] & ss_qflmask(spillbits);
		elt |= x << (f->qf_elem_bits - spillbits);
	}
	return elt;
}

static inline void
ss_qfset(ssqf *f, uint64_t idx, uint64_t elt)
{
	size_t bitpos = f->qf_elem_bits * idx;
	size_t tabpos = bitpos / 64;
	size_t slotpos = bitpos % 64;
	int spillbits = (slotpos + f->qf_elem_bits) - 64;
	elt &= f->qf_elem_mask;
	f->qf_table[tabpos] &= ~(f->qf_elem_mask << slotpos);
	f->qf_table[tabpos] |= elt << slotpos;
	if (spillbits > 0) {
		tabpos++;
		f->qf_table[tabpos] &= ~ss_qflmask(spillbits);
		f->qf_table[tabpos] |= elt >> (f->qf_elem_bits - spillbits);
	}
}

static inline uint64_t
ss_qffind(ssqf *f, uint64_t fq)
{
	uint64_t b = fq;
	while (ss_qfshifted_is( ss_qfget(f, b)))
		b = ss_qfdecr(f, b);
	uint64_t s = b;
	while (b != fq) {
		do {
			s = ss_qfincr(f, s);
		} while (ss_qfcontinuation_is(ss_qfget(f, s)));
		do {
			b = ss_qfincr(f, b);
		} while (! ss_qfoccupied_is(ss_qfget(f, b)));
	}
	return s;
}

static inline void
ss_qfinsert(ssqf *f, uint64_t s, uint64_t elt)
{
	uint64_t prev;
	uint64_t curr = elt;
	int empty;
	do {
		prev = ss_qfget(f, s);
		empty = ss_qfis_empty(prev);
		if (! empty) {
			prev = ss_qfshifted_set(prev);
			if (ss_qfoccupied_is(prev)) {
				curr = ss_qfoccupied_set(curr);
				prev = ss_qfoccupied_clr(prev);
			}
		}
		ss_qfset(f, s, curr);
		curr = prev;
		s = ss_qfincr(f, s);
	} while (! empty);
}

static inline int
ss_qffull(ssqf *f) {
	return f->qf_entries >= f->qf_max_size;
}

void ss_qfadd(ssqf *f, uint64_t h)
{
	if (ssunlikely(ss_qffull(f)))
		return;

	uint64_t fq    = ss_qfhash_to_q(f, h);
	uint64_t fr    = ss_qfhash_to_r(f, h);
	uint64_t T_fq  = ss_qfget(f, fq);
	uint64_t entry = (fr << 3) & ~7;

	if (sslikely(ss_qfis_empty(T_fq))) {
		ss_qfset(f, fq, ss_qfoccupied_set(entry));
		f->qf_entries++;
		return;
	}

	if (! ss_qfoccupied_is(T_fq))
		ss_qfset(f, fq, ss_qfoccupied_set(T_fq));

	uint64_t start = ss_qffind(f, fq);
	uint64_t s = start;

	if (ss_qfoccupied_is(T_fq)) {
		do {
			uint64_t rem = ss_qfremainder_of(ss_qfget(f, s));
			if (rem == fr) {
				return;
			} else if (rem > fr) {
				break;
			}
			s = ss_qfincr(f, s);
		} while (ss_qfcontinuation_is(ss_qfget(f, s)));

		if (s == start) {
			uint64_t old_head = ss_qfget(f, start);
			ss_qfset(f, start, ss_qfcontinuation_set(old_head));
		} else {
			entry = ss_qfcontinuation_set(entry);
		}
	}

	if (s != fq)
		entry = ss_qfshifted_set(entry);

	ss_qfinsert(f, s, entry);
	f->qf_entries++;
}

int ss_qfhas(ssqf *f, uint64_t h)
{
	uint64_t fq   = ss_qfhash_to_q(f, h);
	uint64_t fr   = ss_qfhash_to_r(f, h);
	uint64_t T_fq = ss_qfget(f, fq);

	if (! ss_qfoccupied_is(T_fq))
		return 0;

	uint64_t s = ss_qffind(f, fq);
	do {
		uint64_t rem = ss_qfremainder_of(ss_qfget(f, s));
		if (rem == fr)
			return 1;
		else
		if (rem > fr)
			return 0;
		s = ss_qfincr(f, s);
	} while (ss_qfcontinuation_is(ss_qfget(f, s)));

	return 0;
}
