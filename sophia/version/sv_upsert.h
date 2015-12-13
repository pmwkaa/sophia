#ifndef SV_UPSERT_H_
#define SV_UPSERT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct svupsertnode svupsertnode;
typedef struct svupsert svupsert;

struct svupsertnode {
	uint64_t lsn;
	uint8_t flags;
	ssbuf buf;
};

#define SV_UPSERTRESRV 16

struct svupsert {
	svupsertnode reserve[SV_UPSERTRESRV];
	ssbuf stack;
	int max;
	int count;
	sv result;
};

static inline void
sv_upsertinit(svupsert *u)
{
	const int reserve = SV_UPSERTRESRV;
	int i = 0;
	while (i < reserve) {
		ss_bufinit(&u->reserve[i].buf);
		i++;
	}
	memset(&u->result, 0, sizeof(u->result));
	u->max = reserve;
	u->count = 0;
	ss_bufinit_reserve(&u->stack, u->reserve, sizeof(u->reserve));
}

static inline void
sv_upsertfree(svupsert *u, sr *r)
{
	svupsertnode *n = (svupsertnode*)u->stack.s;
	int i = 0;
	while (i < u->max) {
		ss_buffree(&n[i].buf, r->a);
		i++;
	}
	ss_buffree(&u->stack, r->a);
}

static inline void
sv_upsertreset(svupsert *u)
{
	svupsertnode *n = (svupsertnode*)u->stack.s;
	int i = 0;
	while (i < u->count) {
		ss_bufreset(&n[i].buf);
		i++;
	}
	u->count = 0;
	ss_bufreset(&u->stack);
	memset(&u->result, 0, sizeof(u->result));
}

static inline void
sv_upsertgc(svupsert *u, sr *r, int wm_stack, int wm_buf)
{
	svupsertnode *n = (svupsertnode*)u->stack.s;
	if (u->max >= wm_stack) {
		sv_upsertfree(u, r);
		sv_upsertinit(u);
		return;
	}
	int i = 0;
	while (i < u->count) {
		ss_bufgc(&n[i].buf, r->a, wm_buf);
		i++;
	}
	u->count = 0;
	memset(&u->result, 0, sizeof(u->result));
}

static inline int
sv_upsertpush_raw(svupsert *u, sr *r, char *pointer, int size,
                  uint8_t flags, uint64_t lsn)
{
	svupsertnode *n;
	int rc;
	if (sslikely(u->max > u->count)) {
		n = (svupsertnode*)u->stack.p;
		ss_bufreset(&n->buf);
	} else {
		rc = ss_bufensure(&u->stack, r->a, sizeof(svupsertnode));
		if (ssunlikely(rc == -1))
			return -1;
		n = (svupsertnode*)u->stack.p;
		ss_bufinit(&n->buf);
		u->max++;
	}
	rc = ss_bufensure(&n->buf, r->a, size);
	if (ssunlikely(rc == -1))
		return -1;
	memcpy(n->buf.p, pointer, size);
	n->flags = flags;
	n->lsn = lsn;
	ss_bufadvance(&n->buf, size);
	ss_bufadvance(&u->stack, sizeof(svupsertnode));
	u->count++;
	return 0;
}

static inline int
sv_upsertpush(svupsert *u, sr *r, sv *v)
{
	return sv_upsertpush_raw(u, r, sv_pointer(v),
	                         sv_size(v),
	                         sv_flags(v), sv_lsn(v));
}

static inline svupsertnode*
sv_upsertpop(svupsert *u)
{
	if (u->count == 0)
		return NULL;
	int pos = u->count - 1;
	u->count--;
	u->stack.p -= sizeof(svupsertnode);
	return ss_bufat(&u->stack, sizeof(svupsertnode), pos);
}

static inline int
sv_upsertdo(svupsert *u, sr *r, svupsertnode *a, svupsertnode *b)
{
	assert(b->flags & SVUPSERT);
	int       b_flagsraw = b->flags;
	int       b_flags = b->flags & SVUPSERT;
	uint64_t  b_lsn = b->lsn;
	void     *b_pointer = b->buf.s;
	int       b_size = ss_bufused(&b->buf);
	int       a_flags;
	void     *a_pointer;
	int       a_size;
	if (sslikely(a && !(a->flags & SVDELETE)))
	{
		a_flags   = a->flags & SVUPSERT;
		a_pointer = a->buf.s;
		a_size    = ss_bufused(&a->buf);
	} else {
		/* convert delete to orphan upsert case */
		a_flags   = 0;
		a_pointer = NULL;
		a_size    = 0;
	}
	void *c_pointer;
	int c_size;
	int rc = r->fmt_upsert->function(a_flags, a_pointer, a_size,
	                                 b_flags, b_pointer, b_size,
	                                 r->fmt_upsert->arg,
	                                 &c_pointer, &c_size);
	if (ssunlikely(rc == -1))
		return -1;
	assert(c_pointer != NULL);
	rc = sv_upsertpush_raw(u, r, c_pointer, c_size,
	                       b_flagsraw & ~SVUPSERT,
	                       b_lsn);
	free(c_pointer);
	return rc;
}

static inline int
sv_upsert(svupsert *u, sr *r)
{
	assert(u->count >= 1 );
	svupsertnode *f = ss_bufat(&u->stack, sizeof(svupsertnode), u->count - 1);
	int rc;
	if (f->flags & SVUPSERT) {
		f = sv_upsertpop(u);
		rc = sv_upsertdo(u, r, NULL, f);
		if (ssunlikely(rc == -1))
			return -1;
	}
	if (u->count == 1)
		goto done;
	while (u->count > 1) {
		svupsertnode *f = sv_upsertpop(u);
		svupsertnode *s = sv_upsertpop(u);
		assert(f != NULL);
		assert(s != NULL);
		rc = sv_upsertdo(u, r, f, s);
		if (ssunlikely(rc == -1))
			return -1;
	}
done:
	sv_init(&u->result, &sv_upsertvif, u->stack.s, NULL);
	return 0;
}

#endif
