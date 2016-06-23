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
	ssbuf buf;
};

#define SV_UPSERTRESRV 16

struct svupsert {
	svupsertnode reserve[SV_UPSERTRESRV];
	ssbuf        stack;
	ssbuf        tmp;
	int          max;
	int          count;
	char        *result;
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
	u->result = NULL;
	u->count  = 0;
	u->max    = reserve;
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
	u->result = NULL;
	u->count  = 0;
	ss_bufreset(&u->stack);
	ss_bufreset(&u->tmp);
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
	u->result = NULL;
}

static inline int
sv_upsertpush(svupsert *u, sr *r, char *pointer)
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
	uint32_t size = sf_size(r->scheme, pointer);
	rc = ss_bufensure(&n->buf, r->a, size);
	if (ssunlikely(rc == -1))
		return -1;
	memcpy(n->buf.p, pointer, size);
	ss_bufadvance(&n->buf, size);
	ss_bufadvance(&u->stack, sizeof(svupsertnode));
	u->count++;
	return 0;
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
	assert(r->scheme->fields_count <= 16);

	uint32_t  src_size[16];
	char     *src[16];
	void     *src_ptr;
	uint32_t *src_size_ptr;

	uint32_t  upsert_size[16];
	char     *upsert[16];
	uint32_t  result_size[16];
	char     *result[16];

	int i = 0;
	if (sslikely(a && !(sf_flags(r->scheme, a->buf.s) & SVDELETE) ))
	{
		src_ptr = src;
		src_size_ptr = src_size;
		for (; i < r->scheme->fields_count; i++) {
			src[i]    = sf_fieldof(r->scheme, i, a->buf.s, &src_size[i]);
			upsert[i] = sf_fieldof(r->scheme, i, b->buf.s, &upsert_size[i]);
			result[i] = src[i];
			result_size[i] = src_size[i];
		}
	} else {
		src_ptr = NULL;
		src_size_ptr = NULL;
		for (; i < r->scheme->fields_count; i++) {
			upsert[i] = sf_fieldof(r->scheme, i, b->buf.s, &upsert_size[i]);
			result[i] = upsert[i];
			result_size[i] = upsert_size[i];
		}
	}

	/* execute */
	int rc;
	rc = r->upsert->function(r->scheme->fields_count,
	                         src_ptr,
	                         src_size_ptr,
	                         upsert,
	                         upsert_size,
	                         result,
	                         result_size,
	                         r->upsert->arg);
	if (ssunlikely(rc == -1))
		return -1;

	/* validate and create new record */
	sfv v[16];
	i = 0;
	for ( ; i < r->scheme->fields_count; i++) {
		v[i].pointer = result[i];
		v[i].size = result_size[i];
	}
	uint32_t size = sf_writesize(r->scheme, v);
	ss_bufreset(&u->tmp);
	rc = ss_bufensure(&u->tmp, r->a, size);
	if (ssunlikely(rc == -1))
		goto cleanup;
	sf_write(r->scheme, v, u->tmp.s);
	ss_bufadvance(&u->tmp, size);
	/* update meta-fields */
	sf_sizeset(r->scheme, u->tmp.s, size);
	sf_flagsset(r->scheme, u->tmp.s,
	            sf_flags(r->scheme, b->buf.s) & ~SVUPSERT);

	/* save result */
	rc = sv_upsertpush(u, r, u->tmp.s);
cleanup:
	/* free fields */
	i = 0;
	for ( ; i < r->scheme->fields_count; i++) {
		if (src_ptr == NULL) {
			if (v[i].pointer != upsert[i])
				free(v[i].pointer);
		} else {
			if (v[i].pointer != src[i])
				free(v[i].pointer);
		}
	}
	return rc;
}

static inline int
sv_upsert(svupsert *u, sr *r)
{
	assert(u->count >= 1 );
	svupsertnode *f = ss_bufat(&u->stack, sizeof(svupsertnode), u->count - 1);
	int rc;
	if (sf_flags(r->scheme, f->buf.s) & SVUPSERT) {
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
	f = ss_bufat(&u->stack, sizeof(svupsertnode), 0);
	u->result = f->buf.s;
	return 0;
}

#endif
