
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libst.h>
#include <sophia.h>

static svv*
allocv(sra *a, uint64_t lsn, uint8_t flags, uint32_t *key)
{
	svlocal l;
	l.lsn         = lsn;
	l.flags       = flags;
	l.key         = key;
	l.keysize     = sizeof(uint32_t);
	l.value       = NULL;
	l.valuesize   = 0;
	l.valueoffset = 0;
	sv lv;
	svinit(&lv, &sv_localif, &l, NULL);
	return sv_valloc(a, &lv);
}

static inline svv*
getv(svindex *i, sr *r, uint64_t lsvn, uint32_t *key) {
	srrbnode *n = NULL;
	int rc = sv_indexmatch(&i->i, r->cmp, (char*)key, sizeof(uint32_t), &n);
	if (rc == 0 && n) {
		return sv_visible(srcast(n, svv, node), lsvn);
	}
	return NULL;
}

static void
svindex_replace0(stc *cx srunused)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	uint32_t key = 7;
	svv *old = NULL;
	svv *h = allocv(&a, 0, SVSET, &key);
	svv *n = allocv(&a, 1, SVSET, &key);
	t( sv_indexset(&i, &r, 0, h, &old) == 0 );
	t( old == NULL );
	t( sv_indexset(&i, &r, 1, n, &old) == 0 );
	t( old == h );
	sr_free(&a, old);

	svv *p = getv(&i, &r, 1, &key);
	t( p == n );
	t( n->next == NULL );

	sv_indexfree(&i, &r);
}

static void
svindex_replace1(stc *cx srunused)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	uint32_t key = 7;
	svv *old = NULL;
	svv *h = allocv(&a, 0, SVSET, &key);
	svv *n = allocv(&a, 1, SVSET, &key);
	t( sv_indexset(&i, &r, 0, h, &old) == 0 );
	t( sv_indexset(&i, &r, 0, n, &old) == 0 );
	t( old == NULL );

	svv *p = getv(&i, &r, 1, &key);
	t( p == n );
	t( n->next == h );
	t( h->next == NULL );

	p = getv(&i, &r, 0, &key);
	t( p == h );
	t( h->next == NULL );

	sv_indexfree(&i, &r);
}

static void
svindex_replace2(stc *cx srunused)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	uint32_t key = 7;
	svv *old = NULL;
	svv *h = allocv(&a, 0, SVSET, &key);
	svv *n = allocv(&a, 1, SVSET, &key);
	svv *p = allocv(&a, 2, SVSET, &key);
	t( sv_indexset(&i, &r, 0, h, &old) == 0 );
	t( old == NULL );
	t( sv_indexset(&i, &r, 0, n, &old) == 0 );
	t( old == NULL );
	t( sv_indexset(&i, &r, 0, p, &old) == 0 );
	t( old == NULL );

	svv *q = getv(&i, &r, 1, &key);
	t( q == n );
	t( q->next == h );
	t( h->next == NULL );

	q = getv(&i, &r, 0, &key);
	t( q == h );
	t( h->next == NULL );

	q = getv(&i, &r, 2, &key);
	t( q == p );
	t( p->next == n );
	t( n->next == h );
	t( h->next == NULL );

	sv_indexfree(&i, &r);
}

stgroup *svindex_group(void)
{
	stgroup *group = st_group("svindex");
	st_groupadd(group, st_test("replace0", svindex_replace0));
	st_groupadd(group, st_test("replace1", svindex_replace1));
	st_groupadd(group, st_test("replace2", svindex_replace2));
	return group;
}
