
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
allocv(sr *r, uint64_t lsn, uint8_t flags, uint32_t *key)
{
	srformatv pv;
	pv.key = (char*)key;
	pv.r.size = sizeof(uint32_t);
	pv.r.offset = 0;
	svv *v = sv_vbuild(r, &pv, 1, NULL, 0);
	v->lsn = lsn;
	v->flags = flags;
	return v;
}

static inline svv*
getv(svindex *i, sr *r, uint64_t vlsn, uint32_t *key) {
	srrbnode *n = NULL;
	int rc = sv_indexmatch(&i->i, r->cmp, (char*)key, sizeof(uint32_t), &n);
	if (rc == 0 && n) {
		return sv_visible(srcast(n, svv, node), vlsn);
	}
	return NULL;
}

static void
svindex_replace0(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SR_FKV, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	uint32_t key = 7;
	svv *old = NULL;
	svv *h = allocv(&r, 0, SVSET, &key);
	svv *n = allocv(&r, 1, SVSET, &key);
	t( sv_indexset(&i, &r, 0, h, &old) == 0 );
	t( old == NULL );
	t( sv_indexset(&i, &r, 1, n, &old) == 0 );
	t( old == h );
	sr_free(&a, old);

	svv *p = getv(&i, &r, 1, &key);
	t( p == n );
	t( n->next == NULL );

	sv_indexfree(&i, &r);
	sr_keyfree(&cmp, &a);
}

static void
svindex_replace1(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SR_FKV, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	uint32_t key = 7;
	svv *old = NULL;
	svv *h = allocv(&r, 0, SVSET, &key);
	svv *n = allocv(&r, 1, SVSET, &key);
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
	sr_keyfree(&cmp, &a);
}

static void
svindex_replace2(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SR_FKV, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	uint32_t key = 7;
	svv *old = NULL;
	svv *h = allocv(&r, 0, SVSET, &key);
	svv *n = allocv(&r, 1, SVSET, &key);
	svv *p = allocv(&r, 2, SVSET, &key);
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
	sr_keyfree(&cmp, &a);
}

stgroup *svindex_group(void)
{
	stgroup *group = st_group("svindex");
	(void)svindex_replace0;
	(void)svindex_replace1;
	(void)svindex_replace2;
	/*
	st_groupadd(group, st_test("replace0", svindex_replace0));
	st_groupadd(group, st_test("replace1", svindex_replace1));
	st_groupadd(group, st_test("replace2", svindex_replace2));
	*/
	return group;
}
