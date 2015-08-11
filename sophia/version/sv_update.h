#ifndef SV_UPDATE_H_
#define SV_UPDATE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct svupdatenode svupdatenode;
typedef struct svupdate svupdate;

struct svupdatenode {
	uint64_t lsn;
	uint8_t flags;
	ssbuf buf;
};

#define SV_UPDATERESRV 16

struct svupdate {
	svupdatenode reserve[SV_UPDATERESRV];
	ssbuf stack;
	int max;
	int count;
	sv result;
};

static inline void
sv_updateinit(svupdate *u)
{
	const int reserve = SV_UPDATERESRV;
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
sv_updatefree(svupdate *u, sr *r)
{
	svupdatenode *n = (svupdatenode*)u->stack.s;
	int i = 0;
	while (i < u->max) {
		ss_buffree(&n[i].buf, r->a);
		i++;
	}
	ss_buffree(&u->stack, r->a);
}

static inline void
sv_updatereset(svupdate *u)
{
	svupdatenode *n = (svupdatenode*)u->stack.s;
	int i = 0;
	while (i < u->count) {
		ss_bufreset(&n[i].buf);
		i++;
	}
	u->count = 0;
	ss_bufreset(&u->stack);
	memset(&u->result, 0, sizeof(u->result));
}

static inline int
sv_updatepush_raw(svupdate *u, sr *r, char *pointer, int size,
                  uint8_t flags, uint64_t lsn)
{
	svupdatenode *n;
	int rc;
	if (sslikely(u->max > u->count)) {
		n = (svupdatenode*)u->stack.p;
		ss_bufreset(&n->buf);
	} else {
		rc = ss_bufensure(&u->stack, r->a, sizeof(svupdatenode));
		if (ssunlikely(rc == -1))
			return -1;
		n = (svupdatenode*)u->stack.p;
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
	ss_bufadvance(&u->stack, sizeof(svupdatenode));
	u->count++;
	return 0;
}

static inline int
sv_updatepush(svupdate *u, sr *r, sv *v)
{
	return sv_updatepush_raw(u, r, sv_pointer(v),
	                         sv_size(v),
	                         sv_flags(v), sv_lsn(v));
}

static inline svupdatenode*
sv_updatepop(svupdate *u)
{
	if (u->count == 0)
		return NULL;
	int pos = u->count - 1;
	u->count--;
	u->stack.p -= sizeof(svupdatenode);
	return ss_bufat(&u->stack, sizeof(svupdatenode), pos);
}

static inline int
sv_updatedo(svupdate *u, sr *r, svupdatenode *a, svupdatenode *b)
{
	assert(b->flags & SVUPDATE);
	int       b_flagsraw = b->flags;
	int       b_flags = b->flags & SVUPDATE;
	uint64_t  b_lsn = b->lsn;
	void     *b_pointer = b->buf.s;
	int       b_size = ss_bufused(&b->buf);
	int       a_flags;
	void     *a_pointer;
	int       a_size;
	if (sslikely(a && !(a->flags & SVDELETE)))
	{
		a_flags   = a->flags & SVUPDATE;
		a_pointer = a->buf.s;
		a_size    = ss_bufused(&a->buf);
	} else {
		/* convert delete to orphan update case */
		a_flags   = 0;
		a_pointer = NULL;
		a_size    = 0;
	}
	void *c_pointer;
	int c_size;
	int rc = r->fmt_update->function(a_flags, a_pointer, a_size,
	                                 b_flags, b_pointer, b_size,
	                                 r->fmt_update->arg,
	                                 &c_pointer, &c_size);
	if (ssunlikely(rc == -1))
		return -1;
	assert(c_pointer != NULL);
	rc = sv_updatepush_raw(u, r, c_pointer, c_size,
	                       b_flagsraw & ~SVUPDATE,
	                       b_lsn);
	free(c_pointer);
	return rc;
}

static inline int
sv_update(svupdate *u, sr *r)
{
	assert(u->count >= 1 );
	svupdatenode *f = ss_bufat(&u->stack, sizeof(svupdatenode), u->count - 1);
	int rc;
	if (f->flags & SVUPDATE) {
		f = sv_updatepop(u);
		rc = sv_updatedo(u, r, NULL, f);
		if (ssunlikely(rc == -1))
			return -1;
	}
	if (u->count == 1)
		goto done;
	while (u->count > 1) {
		svupdatenode *f = sv_updatepop(u);
		svupdatenode *s = sv_updatepop(u);
		assert(f != NULL);
		assert(s != NULL);
		rc = sv_updatedo(u, r, f, s);
		if (ssunlikely(rc == -1))
			return -1;
	}
done:
	sv_init(&u->result, &sv_updatevif, u->stack.s, NULL);
	return 0;
}

#endif
