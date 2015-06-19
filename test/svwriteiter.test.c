
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libsv.h>
#include <libst.h>
#include <sophia.h>

static sv*
allocv(sr *r, uint64_t lsn, uint8_t flags, void *key)
{
	sfv pv;
	pv.key = (char*)key;
	pv.r.size = sizeof(uint32_t);
	pv.r.offset = 0;
	svv *v = sv_vbuild(r, &pv, 1, NULL, 0);
	v->lsn = lsn;
	v->flags = flags;
	sv *vv = ss_malloc(r->a, sizeof(sv));
	sv_init(vv, &sv_vif, v, NULL);
	return vv;
}

static void
svwriteiter_iter(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);
	int i = 0;
	while (i < 10)
	{
		sv *v = allocv(&r, 10 - i, 0, &i);
		t(ss_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 20 * (sizeof(svv) + sizeof(i));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 10ULL, 0);

	i = 0;
	while (ss_iteratorhas(&iter)) {
		sv *v = (sv*)ss_iteratorof(&iter);
		t( *(int*)sv_key(v, &r, 0) == i );
		t( sv_lsn(v) == 10 - i );
		t( sv_flags(v) == 0 );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 10 );
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_limit(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);
	int i = 0;
	while (i < 18)
	{
		sv *v = allocv(&r, 18 - i, 0, &i);
		t(ss_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 5 * (sizeof(svv) + sizeof(i));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 18ULL, 0);

	i = 0;
	while (ss_iteratorhas(&iter)) {
		sv *v = (sv*)ss_iteratorof(&iter);
		t( *(int*)sv_key(v, &r, 0) == i );
		t( sv_lsn(v) == 18 - i );
		t( sv_flags(v) == 0 );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 5 );
	int j = 0;
	sv_writeiter_resume(&iter);
	while (ss_iteratorhas(&iter)) {
		sv *v = (sv*)ss_iteratorof(&iter);
		t( *(int*)sv_key(v, &r, 0) == i );
		t( sv_lsn(v) == 18 - i );
		t( sv_flags(v) == 0 );
		ss_iteratornext(&iter);
		i++;
		j++;
	}
	t( j == 5 );
	j = 0;
	sv_writeiter_resume(&iter);
	while (ss_iteratorhas(&iter)) {
		sv *v = (sv*)ss_iteratorof(&iter);
		t( *(int*)sv_key(v, &r, 0) == i );
		t( sv_lsn(v) == 18 - i );
		t( sv_flags(v) == 0 );
		ss_iteratornext(&iter);
		i++;
		j++;
	}
	t( j == 5 );
	j = 0;
	sv_writeiter_resume(&iter);
	while (ss_iteratorhas(&iter)) {
		sv *v = (sv*)ss_iteratorof(&iter);
		t( *(int*)sv_key(v, &r, 0) == i );
		t( sv_lsn(v) == 18 - i );
		t( sv_flags(v) == 0 );
		ss_iteratornext(&iter);
		i++;
		j++;
	}
	t( j == 3 );
	t( i == 18 );
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_limit_small(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);
	int i = 0;
	while (i < 18)
	{
		sv *v = allocv(&r, 18 - i, 0, &i);
		t(ss_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(i));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 18ULL, 0);

	i = 0;
	while (ss_iteratorhas(&iter)) {
		sv *v = (sv*)ss_iteratorof(&iter);
		t( *(int*)sv_key(v, &r, 0) == i );
		t( sv_lsn(v) == 18 - i );
		t( sv_flags(v) == 0 );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 1 );
	int j = 0;
	sv_writeiter_resume(&iter);
	while (ss_iteratorhas(&iter)) {
		sv *v = (sv*)ss_iteratorof(&iter);
		t( *(int*)sv_key(v, &r, 0) == i );
		t( sv_lsn(v) == 18 - i );
		t( sv_flags(v) == 0 );
		ss_iteratornext(&iter);
		i++;
		j++;
	}
	t( j == 1 );
	j = 0;
	sv_writeiter_resume(&iter);
	while (ss_iteratorhas(&iter)) {
		sv *v = (sv*)ss_iteratorof(&iter);
		t( *(int*)sv_key(v, &r, 0) == i );
		t( sv_lsn(v) == 18 - i );
		t( sv_flags(v) == 0 );
		ss_iteratornext(&iter);
		i++;
		j++;
	}
	t( j == 1 );
	j = 0;
	sv_writeiter_resume(&iter);
	while (ss_iteratorhas(&iter)) {
		sv *v = (sv*)ss_iteratorof(&iter);
		t( *(int*)sv_key(v, &r, 0) == i );
		t( sv_lsn(v) == 18 - i );
		t( sv_flags(v) == 0 );
		ss_iteratornext(&iter);
		i++;
		j++;
	}
	t( j == 1 );
	t( i == 4 );
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
addv(ssbuf *list, sr *r, uint64_t lsn, int flags, char *key, int keysize)
{
	sv *v = allocv(r, lsn, flags, key);
	ss_bufadd(list, r->a, &v, sizeof(sv**));
}

static void
checkv(stc *cx, sr *r, ssiter *i, uint64_t lsn, int flags, int key)
{
	sv *v = (sv*)ss_iteratorof(i);
	t( *(int*)sv_key(v, r, 0) == key );
	t( sv_lsn(v) == lsn );
	t( sv_flags(v) == flags );
}

static void
svwriteiter_dup_lsn_gt(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int key = 7;
	addv(&vlista, &r, 10, 0, (char*)&key, sizeof(key));
	addv(&vlista, &r,  9, 0|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r,  8, 0|SVDUP, (char*)&key, sizeof(key));

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 10ULL, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		sv *v = (sv*)ss_iteratorof(&iter);
		t( *(int*)sv_key(v, &r, 0) == key );
		t( sv_lsn(v) == 10 - i );
		if (i == 0)
			t( sv_flags(v) == 0 );
		else
			t( sv_flags(v) == (0 | SVDUP) );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 1 );
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_dup_lsn_lt0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &r, 10, 0, (char*)&key, sizeof(key));
	addv(&vlista, &r,  9, 0|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r,  8, 0|SVDUP, (char*)&key, sizeof(key));

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 9ULL, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		sv *v = (sv*)ss_iteratorof(&iter);
		t( *(int*)sv_key(v, &r, 0) == key );
		t( sv_lsn(v) == 10 - i );
		if (i == 0)
			t( sv_flags(v) == 0 );
		else
			t( sv_flags(v) == (0 | SVDUP) );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 2 );
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_dup_lsn_lt1(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &r, 10, 0, (char*)&key, sizeof(key));
	addv(&vlista, &r,  9, 0|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r,  8, 0|SVDUP, (char*)&key, sizeof(key));

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 8ULL, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		sv *v = (sv*)ss_iteratorof(&iter);
		t( *(int*)sv_key(v, &r, 0) == key );
		t( sv_lsn(v) == 10 - i );
		if (i == 0)
			t( sv_flags(v) == 0 );
		else
			t( sv_flags(v) == (0 | SVDUP) );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 3 );
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_dup_lsn_lt2(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &r, 10, 0, (char*)&key, sizeof(key));
	addv(&vlista, &r,  9, 0|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r,  8, 0|SVDUP, (char*)&key, sizeof(key));

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 2ULL, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		sv *v = (sv*)ss_iteratorof(&iter);
		t( *(int*)sv_key(v, &r, 0) == key );
		t( sv_lsn(v) == 10 - i );
		if (i == 0)
			t( sv_flags(v) == 0 );
		else
			t( sv_flags(v) == (0 | SVDUP) );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 3 );
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_dup_lsn_gt_chain(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int key = 7;
	int key2 = 8;
	int key3 = 9;
	int key4 = 10;

	addv(&vlista, &r, 10, 0, (char*)&key, sizeof(key));
	addv(&vlista, &r,  9, 0|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r,  8, 0|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r, 12, 0, (char*)&key2, sizeof(key2));
	addv(&vlista, &r, 11, 0|SVDUP, (char*)&key2, sizeof(key2));
	addv(&vlista, &r, 13, 0, (char*)&key3, sizeof(key3));
	addv(&vlista, &r, 14, 0, (char*)&key4, sizeof(key4));

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 15ULL, 0);

	checkv(cx, &r, &iter, 10, 0, key);
	ss_iteratornext(&iter);
	checkv(cx, &r, &iter, 12, 0, key2);
	ss_iteratornext(&iter);
	checkv(cx, &r, &iter, 13, 0, key3);
	ss_iteratornext(&iter);
	checkv(cx, &r, &iter, 14, 0, key4);
	ss_iteratornext(&iter);
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_dup_lsn_lt0_chain(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int key = 7;
	int key2 = 8;
	int key3 = 9;
	int key4 = 10;

	addv(&vlista, &r, 10, 0, (char*)&key, sizeof(key));
	addv(&vlista, &r,  9, 0|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r,  8, 0|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r, 12, 0, (char*)&key2, sizeof(key2));
	addv(&vlista, &r, 11, 0|SVDUP, (char*)&key2, sizeof(key2));
	addv(&vlista, &r, 13, 0, (char*)&key3, sizeof(key3));
	addv(&vlista, &r, 14, 0, (char*)&key4, sizeof(key4));

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 11ULL, 0);

	checkv(cx, &r, &iter, 10, 0, key);
	ss_iteratornext(&iter);
	checkv(cx, &r, &iter, 12, 0, key2);
	ss_iteratornext(&iter);
	checkv(cx, &r, &iter, 11, 0|SVDUP, key2);
	ss_iteratornext(&iter);
	checkv(cx, &r, &iter, 13, 0, key3);
	ss_iteratornext(&iter);
	checkv(cx, &r, &iter, 14, 0, key4);
	ss_iteratornext(&iter);
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_dup_lsn_lt1_chain(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int key = 7;
	int key2 = 8;
	int key3 = 9;
	int key4 = 10;

	addv(&vlista, &r, 10, 0, (char*)&key, sizeof(key));
	addv(&vlista, &r,  9, 0|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r,  8, 0|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r, 12, 0, (char*)&key2, sizeof(key2));
	addv(&vlista, &r, 11, 0|SVDUP, (char*)&key2, sizeof(key2));
	addv(&vlista, &r, 13, 0, (char*)&key3, sizeof(key3));
	addv(&vlista, &r, 14, 0, (char*)&key4, sizeof(key4));

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 9ULL, 0);

	checkv(cx, &r, &iter, 10, 0, key);
	ss_iteratornext(&iter);
	checkv(cx, &r, &iter,  9, 0|SVDUP, key);
	ss_iteratornext(&iter);
	checkv(cx, &r, &iter, 12, 0, key2);
	ss_iteratornext(&iter);
	checkv(cx, &r, &iter, 11, 0|SVDUP, key2);
	ss_iteratornext(&iter);
	checkv(cx, &r, &iter, 13, 0, key3);
	ss_iteratornext(&iter);
	checkv(cx, &r, &iter, 14, 0, key4);
	ss_iteratornext(&iter);
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_dup_lsn_lt2_chain(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int key = 7;
	int key2 = 8;
	int key3 = 9;
	int key4 = 10;

	addv(&vlista, &r, 10, 0, (char*)&key, sizeof(key));
	addv(&vlista, &r,  9, 0|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r,  8, 0|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r, 12, 0, (char*)&key2, sizeof(key2));
	addv(&vlista, &r, 11, 0|SVDUP, (char*)&key2, sizeof(key2));
	addv(&vlista, &r, 13, 0, (char*)&key3, sizeof(key3));
	addv(&vlista, &r, 14, 0, (char*)&key4, sizeof(key4));

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 3ULL, 0);

	checkv(cx, &r, &iter, 10, 0, key);
	ss_iteratornext(&iter);
	checkv(cx, &r, &iter,  9, 0|SVDUP, key);
	ss_iteratornext(&iter);
	checkv(cx, &r, &iter,  8, 0|SVDUP, key);
	ss_iteratornext(&iter);
	checkv(cx, &r, &iter, 12, 0, key2);
	ss_iteratornext(&iter);
	checkv(cx, &r, &iter, 11, 0|SVDUP, key2);
	ss_iteratornext(&iter);
	checkv(cx, &r, &iter, 13, 0, key3);
	ss_iteratornext(&iter);
	checkv(cx, &r, &iter, 14, 0, key4);
	ss_iteratornext(&iter);
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_dup_lsn_limit0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &r, 10, 0, (char*)&key, sizeof(key));
	addv(&vlista, &r,  9, 0|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r,  8, 0|SVDUP, (char*)&key, sizeof(key));

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 15ULL, 0);

	checkv(cx, &r, &iter, 10, 0, key);
	ss_iteratornext(&iter);
	t( ss_iteratorhas(&iter) == 0 );
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_dup_lsn_limit1(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &r, 10, 0, (char*)&key, sizeof(key));
	addv(&vlista, &r,  9, 0|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r,  8, 0|SVDUP, (char*)&key, sizeof(key));

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 9ULL, 0);

	checkv(cx, &r, &iter, 10, 0, key);
	ss_iteratornext(&iter);
	checkv(cx, &r, &iter,  9, 0|SVDUP, key);
	ss_iteratornext(&iter);
	t( ss_iteratorhas(&iter) == 0 );
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_dup_lsn_limit2(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int key = 7;
	addv(&vlista, &r, 10, 0, (char*)&key, sizeof(key));
	addv(&vlista, &r,  9, 0|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r,  8, 0|SVDUP, (char*)&key, sizeof(key));

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 5ULL, 0);

	checkv(cx, &r, &iter, 10, 0, key);
	ss_iteratornext(&iter);
	checkv(cx, &r, &iter,  9, 0|SVDUP, key);
	ss_iteratornext(&iter);
	checkv(cx, &r, &iter,  8, 0|SVDUP, key);
	ss_iteratornext(&iter);
	t( ss_iteratorhas(&iter) == 0 );
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_dup_lsn_limit3(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int k = 0;
	int key = 7;
	addv(&vlista, &r, 412 - k, 0, (char*)&key, sizeof(key));
	while (k < 411) {
		addv(&vlista, &r, 411 - k, 0|SVDUP, (char*)&key, sizeof(key));
		k++;
	}
	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 2 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 500ULL, 0);

	t(ss_iteratorhas(&iter) == 1);
	checkv(cx, &r, &iter, 412, 0, key);
	ss_iteratornext(&iter);
	t(ss_iteratorhas(&iter) == 0);
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_dup_lsn_limit4(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int k = 0;
	int key = 7;
	addv(&vlista, &r, 412 - k, 0, (char*)&key, sizeof(key));
	while (k < 411) {
		addv(&vlista, &r, 411 - k, 0|SVDUP, (char*)&key, sizeof(key));
		k++;
	}
	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(k));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 0ULL, 0);

	k = 0;
	while (ss_iteratorhas(&iter))
	{
		if (k == 0)
			checkv(cx, &r, &iter, 412 - k, 0, key);
		else
			checkv(cx, &r, &iter, 412 - k, 0|SVDUP, key);
		ss_iteratornext(&iter);
		k++;
	}
	t( k == 412 );
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_dup_lsn_limit5(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int k = 0;
	int key = 7;
	addv(&vlista, &r, 412 - k, 0, (char*)&key, sizeof(key));
	while (k < 411) {
		addv(&vlista, &r, 411 - k, 0|SVDUP, (char*)&key, sizeof(key));
		k++;
	}
	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(k));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 0ULL, 0);

	k = 0;
	while (ss_iteratorhas(&iter))
	{
		if (k == 0)
			checkv(cx, &r, &iter, 412 - k, 0, key);
		else
			checkv(cx, &r, &iter, 412 - k, 0|SVDUP, key);
		ss_iteratornext(&iter);
		k++;
	}
	t( k == 412 );
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_delete0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &r, 10, 0, (char*)&key, sizeof(key));
	addv(&vlista, &r,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 10ULL, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		sv *v = (sv*)ss_iteratorof(&iter);
		t( *(int*)sv_key(v, &r, 0) == key );
		t( sv_lsn(v) == 10 - i );
		if (i == 0)
			t( sv_flags(v) == 0 );
		else
			t( sv_flags(v) == (SVDELETE | SVDUP) );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 1 );
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_delete1(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &r, 10, 0, (char*)&key, sizeof(key));
	addv(&vlista, &r,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 9ULL, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		sv *v = (sv*)ss_iteratorof(&iter);
		t( *(int*)sv_key(v, &r, 0) == key );
		t( sv_lsn(v) == 10 - i );
		if (i == 0)
			t( sv_flags(v) == 0 );
		else
			t( sv_flags(v) == (SVDELETE | SVDUP) );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 2 );
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_delete2(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &r, 10, 0, (char*)&key, sizeof(key));
	addv(&vlista, &r,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 8ULL, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		sv *v = (sv*)ss_iteratorof(&iter);
		t( *(int*)sv_key(v, &r, 0) == key );
		t( sv_lsn(v) == 10 - i );
		if (i == 0)
			t( sv_flags(v) == 0 );
		else
			t( sv_flags(v) == (SVDELETE | SVDUP) );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 3 );
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_delete3(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &r, 10, SVDELETE, (char*)&key, sizeof(key));
	addv(&vlista, &r,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 7ULL, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		sv *v = (sv*)ss_iteratorof(&iter);
		t( *(int*)sv_key(v, &r, 0) == key );
		t( sv_lsn(v) == 10 - i );
		if (i == 0)
			t( sv_flags(v) == SVDELETE );
		else
			t( sv_flags(v) == (SVDELETE | SVDUP) );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 3 );
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_delete4(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &r, 10, SVDELETE, (char*)&key, sizeof(key));
	addv(&vlista, &r,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 10ULL, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		sv *v = (sv*)ss_iteratorof(&iter);
		t( *(int*)sv_key(v, &r, 0) == key );
		t( sv_lsn(v) == 10 - i );
		if (i == 0)
			t( sv_flags(v) == SVDELETE );
		else
			t( sv_flags(v) == (SVDELETE | SVDUP) );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 0 );
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_delete5(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &r, 10, SVDELETE, (char*)&key, sizeof(key));
	addv(&vlista, &r,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 11ULL, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		sv *v = (sv*)ss_iteratorof(&iter);
		t( *(int*)sv_key(v, &r, 0) == key );
		t( sv_lsn(v) == 10 - i );
		if (i == 0)
			t( sv_flags(v) == SVDELETE );
		else
			t( sv_flags(v) == (SVDELETE | SVDUP) );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 0 );
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_delete6(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int key = 6;
	addv(&vlista, &r, 12, 0, (char*)&key, sizeof(key));
	key = 7;
	addv(&vlista, &r, 10, SVDELETE, (char*)&key, sizeof(key));
	addv(&vlista, &r,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	key = 10;
	addv(&vlista, &r, 11, 0, (char*)&key, sizeof(key));

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 13ULL, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		sv *v = (sv*)ss_iteratorof(&iter);
		if (i == 0) {
			t( *(int*)sv_key(v, &r, 0) == 6 );
			t( sv_lsn(v) == 12 );
		} else {
			t( *(int*)sv_key(v, &r, 0) == 10 );
			t( sv_lsn(v) == 11 );
		}
		t( sv_flags(v) == 0 );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 2 );
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_delete7(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int key = 6;
	addv(&vlista, &r, 12, 0, (char*)&key, sizeof(key));
	key = 7;
	addv(&vlista, &r, 10, SVDELETE, (char*)&key, sizeof(key));
	addv(&vlista, &r,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	key = 10;
	addv(&vlista, &r, 11, 0, (char*)&key, sizeof(key));

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 10ULL, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		sv *v = (sv*)ss_iteratorof(&iter);
		if (i == 0) {
			t( *(int*)sv_key(v, &r, 0) == 6 );
			t( sv_flags(v) == 0 );
			t( sv_lsn(v) == 12 );
		} else {
			t( *(int*)sv_key(v, &r, 0) == 10 );
			t( sv_flags(v) == 0 );
			t( sv_lsn(v) == 11 );
		}
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 2 );
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_delete8(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int key = 6;
	addv(&vlista, &r, 12, 0, (char*)&key, sizeof(key));
	key = 7;
	addv(&vlista, &r, 10, SVDELETE, (char*)&key, sizeof(key));
	addv(&vlista, &r,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &r,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	key = 10;
	addv(&vlista, &r, 11, 0, (char*)&key, sizeof(key));

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 9ULL, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		sv *v = (sv*)ss_iteratorof(&iter);
		if (i == 0) {
			t( *(int*)sv_key(v, &r, 0) == 6 );
			t( sv_flags(v) == 0 );
			t( sv_lsn(v) == 12 );
		} else
		if (i == 1) {
			t( *(int*)sv_key(v, &r, 0) == 7 );
			t( sv_flags(v) == SVDELETE );
			t( sv_lsn(v) == 10 );
		} else
		if (i == 2) {
			t( *(int*)sv_key(v, &r, 0) == 7 );
			t( sv_flags(v) == (SVDELETE|SVDUP) );
			t( sv_lsn(v) ==  9 );
		} else {
			t( *(int*)sv_key(v, &r, 0) == 10 );
			t( sv_flags(v) == 0 );
			t( sv_lsn(v) == 11 );
		}
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 4 );
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_duprange0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int key = 7;
	int lsn = 1;
	int i = 0;
	while (i < 100) {
		addv(&vlista, &r, 100 + lsn, 0, (char*)&key, sizeof(key));
		addv(&vlista, &r, lsn, 0|SVDUP, (char*)&key, sizeof(key));
		lsn++;
		i++;
	}

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = UINT64_MAX;
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 100ULL, 0);

	i = 0;
	while (ss_iteratorhas(&iter)) {
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 200 );
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svwriteiter_duprange1(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	ssbuf vlista;
	ssbuf vlistb;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);

	int key = 7;
	int lsn = 1;
	int i = 0;
	while (i < 100) {
		addv(&vlista, &r, 100 + lsn, 0, (char*)&key, sizeof(key));
		addv(&vlista, &r, lsn, 0|SVDUP, (char*)&key, sizeof(key));
		lsn++;
		i++;
	}

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = UINT64_MAX;
	ss_iteropen(sv_writeiter, &iter, &merge, limit, sizeof(svv), 100ULL + lsn, 0);

	i = 0;
	while (ss_iteratorhas(&iter)) {
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 100 );
	ss_iteratorclose(&iter);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

stgroup *svwriteiter_group(void)
{
	stgroup *group = st_group("svwriteiter");
	st_groupadd(group, st_test("iter", svwriteiter_iter));
	st_groupadd(group, st_test("iter_limit", svwriteiter_limit));
	st_groupadd(group, st_test("iter_limit_small", svwriteiter_limit_small));
	st_groupadd(group, st_test("iter_dup_lsn_gt", svwriteiter_dup_lsn_gt));
	st_groupadd(group, st_test("iter_dup_lsn_lt0", svwriteiter_dup_lsn_lt0));
	st_groupadd(group, st_test("iter_dup_lsn_lt1", svwriteiter_dup_lsn_lt1));
	st_groupadd(group, st_test("iter_dup_lsn_lt2", svwriteiter_dup_lsn_lt2));
	st_groupadd(group, st_test("iter_dup_lsn_gt_chain", svwriteiter_dup_lsn_gt_chain));
	st_groupadd(group, st_test("iter_dup_lsn_lt0_chain", svwriteiter_dup_lsn_lt0_chain));
	st_groupadd(group, st_test("iter_dup_lsn_lt1_chain", svwriteiter_dup_lsn_lt1_chain));
	st_groupadd(group, st_test("iter_dup_lsn_lt2_chain", svwriteiter_dup_lsn_lt2_chain));
	st_groupadd(group, st_test("iter_dup_lsn_limit0", svwriteiter_dup_lsn_limit0));
	st_groupadd(group, st_test("iter_dup_lsn_limit1", svwriteiter_dup_lsn_limit1));
	st_groupadd(group, st_test("iter_dup_lsn_limit2", svwriteiter_dup_lsn_limit2));
	st_groupadd(group, st_test("iter_dup_lsn_limit3", svwriteiter_dup_lsn_limit3));
	st_groupadd(group, st_test("iter_dup_lsn_limit4", svwriteiter_dup_lsn_limit4));
	st_groupadd(group, st_test("iter_dup_lsn_limit5", svwriteiter_dup_lsn_limit5));
	st_groupadd(group, st_test("iter_delete0", svwriteiter_delete0));
	st_groupadd(group, st_test("iter_delete1", svwriteiter_delete1));
	st_groupadd(group, st_test("iter_delete2", svwriteiter_delete2));
	st_groupadd(group, st_test("iter_delete3", svwriteiter_delete3));
	st_groupadd(group, st_test("iter_delete4", svwriteiter_delete4));
	st_groupadd(group, st_test("iter_delete5", svwriteiter_delete5));
	st_groupadd(group, st_test("iter_delete6", svwriteiter_delete6));
	st_groupadd(group, st_test("iter_delete7", svwriteiter_delete7));
	st_groupadd(group, st_test("iter_delete8", svwriteiter_delete8));
	st_groupadd(group, st_test("iter_duprange0", svwriteiter_duprange0));
	st_groupadd(group, st_test("iter_duprange1", svwriteiter_duprange1));
	return group;
}
