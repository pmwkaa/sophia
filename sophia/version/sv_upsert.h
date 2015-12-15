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
	ssbuf tmp;
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
	ss_bufinit(&u->tmp);
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
	ss_buffree(&u->tmp, r->a);
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
	ss_bufreset(&u->tmp);
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
	ss_bufgc(&u->tmp, r->a, wm_buf);
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
	int rc;
	int key_count = r->scheme->count;

	/* source record */
	char *src_value;
	int   src_size;
	if (sslikely(a && !(a->flags & SVDELETE)))
	{
		/* convert delete to orphan upsert case */
		src_value = sf_value(r->fmt, a->buf.s, key_count);
		src_size  = sf_valuesize(r->fmt,
		                         a->buf.s,
		                         ss_bufused(&a->buf),
		                         key_count);
	} else {
		src_value = NULL;
		src_size  = 0;
	}

	/* upsert record */
	assert(b->flags & SVUPSERT);
	int   key_size[SR_SCHEME_MAXKEY];
	char *key[SR_SCHEME_MAXKEY];
	int i;
	for (i = 0; i < key_count; i++) {
		key[i] = sf_key(b->buf.s, i);
		key_size[i] = sf_keysize(b->buf.s, i);
	}
	char *upsert_value;
	int   upsert_size;
	upsert_value = sf_value(r->fmt, b->buf.s, key_count);
	upsert_size  = sf_valuesize(r->fmt,
	                            b->buf.s,
	                            ss_bufused(&b->buf),
	                            key_count);

	/* execute */
	sfupsertf upsert = r->fmt_upsert->function;
	char *result;
	int   result_size;
	result_size  = upsert(&result, key, key_size, key_count,
	                      src_value,
	                      src_size,
	                      upsert_value,
	                      upsert_size,
	                      r->fmt_upsert->arg);
	if (ssunlikely(result_size == -1))
		return -1;
	assert(result != NULL);

	/* rebuild record with new value */
	int v_size = (upsert_value - b->buf.s) + result_size;
	ss_bufreset(&u->tmp);
	rc = ss_bufensure(&u->tmp, r->a, v_size);
	if (ssunlikely(rc == -1)) {
		free(result);
		return -1;
	}
	int off = sf_keycopy(u->tmp.p, b->buf.s, key_count);
	ss_bufadvance(&u->tmp, off);
	memcpy(u->tmp.p, result, result_size);
	ss_bufadvance(&u->tmp, result_size);
	free(result);

	/* push result */
	return sv_upsertpush_raw(u, r, u->tmp.s, ss_bufused(&u->tmp),
	                         b->flags & ~SVUPSERT,
	                         b->lsn);
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
