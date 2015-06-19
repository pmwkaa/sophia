
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
svmergeiter_merge_a(stc *cx ssunused)
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
		sv *v = allocv(&r, i, 0, &i);
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

	i = 0;
	while (ss_iteratorhas(&merge)) {
		sv *v = (sv*)ss_iteratorof(&merge);
		t( *(int*)sv_key(v, &r, 0) == i );
		t( sv_lsn(v) == i );
		t( sv_flags(v) == 0 );
		ss_iteratornext(&merge);
		i++;
	}
	t( i == 10 );
	ss_iteratorclose(&merge);

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
svmergeiter_merge_b(stc *cx ssunused)
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
		sv *v = allocv(&r, i, 0, &i);
		t(ss_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
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

	i = 0;
	while (ss_iteratorhas(&merge)) {
		sv *v = (sv*)ss_iteratorof(&merge);
		t( *(int*)sv_key(v, &r, 0) == i );
		t( sv_lsn(v) == i );
		t( sv_flags(v) == 0 );
		ss_iteratornext(&merge);
		i++;
	}
	t( i == 10 );
	ss_iteratorclose(&merge);

	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));
	while (ss_iteratorhas(&itb)) {
		sv *v = (sv*)ss_iteratorof(&itb);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&itb);
	}
	ss_buffree(&vlistb, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svmergeiter_merge_ab(stc *cx ssunused)
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
	while (i < 5)
	{
		sv *v = allocv(&r, i, 0, &i);
		t(ss_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	while (i < 10)
	{
		sv *v = allocv(&r, i, 0, &i);
		t(ss_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
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

	i = 0;
	while (ss_iteratorhas(&merge)) {
		sv *v = (sv*)ss_iteratorof(&merge);
		t( *(int*)sv_key(v, &r, 0) == i );
		t( sv_lsn(v) == i );
		t( sv_flags(v) == 0 );
		ss_iteratornext(&merge);
		i++;
	}
	t( i == 10 );
	ss_iteratorclose(&merge);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));
	while (ss_iteratorhas(&itb)) {
		sv *v = (sv*)ss_iteratorof(&itb);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&itb);
	}
	ss_buffree(&vlista, &a);
	ss_buffree(&vlistb, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svmergeiter_merge_abc(stc *cx ssunused)
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
	ssbuf vlistc;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);
	ss_bufinit(&vlistc);
	int i = 0;
	while (i < 5)
	{
		sv *v = allocv(&r, i, 0, &i);
		t(ss_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	while (i < 10)
	{
		sv *v = allocv(&r, i, 0, &i);
		t(ss_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	while (i < 15)
	{
		sv *v = allocv(&r, i, 0, &i);
		t(ss_bufadd(&vlistc, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));
	ssiter itc;
	ss_iterinit(ss_bufiterref, &itc);
	ss_iteropen(ss_bufiterref, &itc, &vlistc, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 3);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itc;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	i = 0;
	while (ss_iteratorhas(&merge)) {
		sv *v = (sv*)ss_iteratorof(&merge);
		t( *(int*)sv_key(v, &r, 0) == i );
		t( sv_lsn(v) == i );
		t( sv_flags(v) == 0 );
		ss_iteratornext(&merge);
		i++;
	}
	t( i == 15 );
	ss_iteratorclose(&merge);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));
	while (ss_iteratorhas(&itb)) {
		sv *v = (sv*)ss_iteratorof(&itb);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&itb);
	}
	ss_iterinit(ss_bufiterref, &itc);
	ss_iteropen(ss_bufiterref, &itc, &vlistc, sizeof(sv*));
	while (ss_iteratorhas(&itc)) {
		sv *v = (sv*)ss_iteratorof(&itc);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&itc);
	}
	ss_buffree(&vlista, &a);
	ss_buffree(&vlistb, &a);
	ss_buffree(&vlistc, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svmergeiter_merge_ba(stc *cx ssunused)
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
	while (i < 5)
	{
		sv *v = allocv(&r, i, 0, &i);
		t(ss_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	while (i < 10)
	{
		sv *v = allocv(&r, i, 0, &i);
		t(ss_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
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
	sv_mergeprepare(&m, &r, 3);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	i = 0;
	while (ss_iteratorhas(&merge)) {
		sv *v = (sv*)ss_iteratorof(&merge);
		t( *(int*)sv_key(v, &r, 0) == i );
		t( sv_lsn(v) == i );
		t( sv_flags(v) == 0 );
		ss_iteratornext(&merge);
		i++;
	}
	t( i == 10 );
	ss_iteratorclose(&merge);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));
	while (ss_iteratorhas(&itb)) {
		sv *v = (sv*)ss_iteratorof(&itb);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&itb);
	}
	ss_buffree(&vlista, &a);
	ss_buffree(&vlistb, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svmergeiter_merge_dup_ab(stc *cx ssunused)
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
	int lsn = 10;
	while (i < 5)
	{
		sv *v = allocv(&r, lsn, 0, &i);
		t(ss_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
	}
	i = 0 ;
	while (i < 5)
	{
		sv *v = allocv(&r, lsn, 0, &i);
		t(ss_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
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

	int key = 0;
	i = 0;
	while (ss_iteratorhas(&merge)) {
		sv *v = (sv*)ss_iteratorof(&merge);
		if ((i % 2) == 0) {
			t( *(int*)sv_key(v, &r, 0) == key );
			t( sv_flags(v) == 0 );
			key++;
		} else {
			t( *(int*)sv_key(v, &r, 0) == key - 1);
			t( (sv_flags(v) | sv_mergeisdup(&merge)) == (0|SVDUP) );
		}
		ss_iteratornext(&merge);
		i++;
	}
	t( i == 10 );
	ss_iteratorclose(&merge);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));
	while (ss_iteratorhas(&itb)) {
		sv *v = (sv*)ss_iteratorof(&itb);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&itb);
	}
	ss_buffree(&vlista, &a);
	ss_buffree(&vlistb, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svmergeiter_merge_dup_a_chain(stc *cx ssunused)
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
	int lsn = 5;
	int i = 0;
	while (i < 5)
	{
		sv *v = allocv(&r, lsn, 0 | ((i > 0) ? SVDUP: 0), &key);
		t(ss_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
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

	i = 0;
	while (ss_iteratorhas(&merge)) {
		sv *v = (sv*)ss_iteratorof(&merge);
		t( *(int*)sv_key(v, &r, 0) == key );
		if (i == 0) {
			t( sv_flags(v) == 0 );
		} else {
			t( (sv_flags(v) | sv_mergeisdup(&merge)) == (0|SVDUP) );
		}
		ss_iteratornext(&merge);
		i++;
	}
	t( i == 5 );
	ss_iteratorclose(&merge);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));
	while (ss_iteratorhas(&itb)) {
		sv *v = (sv*)ss_iteratorof(&itb);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&itb);
	}
	ss_buffree(&vlista, &a);
	ss_buffree(&vlistb, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svmergeiter_merge_dup_ab_chain(stc *cx ssunused)
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
	int lsn = 10;
	int key = 7;
	int i = 0;
	while (i < 5)
	{
		sv *v = allocv(&r, lsn, 0 | ((i > 0) ? SVDUP: 0), &key);
		t(ss_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
	}
	i = 0 ;
	while (i < 5)
	{
		sv *v = allocv(&r, lsn, 0 | ((i > 0) ? SVDUP: 0), &key);
		t(ss_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
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

	i = 0;
	while (ss_iteratorhas(&merge)) {
		sv *v = (sv*)ss_iteratorof(&merge);
		t( *(int*)sv_key(v, &r, 0) == key );
		if (i == 0) {
			t( sv_flags(v) == 0 );
		} else {
			t( (sv_flags(v) | sv_mergeisdup(&merge)) == (0|SVDUP) );
		}
		ss_iteratornext(&merge);
		i++;
	}
	t( i == 10 );
	ss_iteratorclose(&merge);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));
	while (ss_iteratorhas(&itb)) {
		sv *v = (sv*)ss_iteratorof(&itb);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&itb);
	}
	ss_buffree(&vlista, &a);
	ss_buffree(&vlistb, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

static void
svmergeiter_merge_dup_abc_chain(stc *cx ssunused)
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
	ssbuf vlistc;
	ss_bufinit(&vlista);
	ss_bufinit(&vlistb);
	ss_bufinit(&vlistc);
	int lsn = 15;
	int key = 7;
	int i = 0;
	while (i < 5)
	{
		sv *v = allocv(&r, lsn, 0 | ((i > 0) ? SVDUP: 0), &key);
		t(ss_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
	}
	i = 0 ;
	while (i < 5)
	{
		sv *v = allocv(&r, lsn, 0 | ((i > 0) ? SVDUP: 0), &key);
		t(ss_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
	}
	i = 0 ;
	while (i < 5)
	{
		sv *v = allocv(&r, lsn, 0 | ((i > 0) ? SVDUP: 0), &key);
		t(ss_bufadd(&vlistc, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
	}

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));
	ssiter itc;
	ss_iterinit(ss_bufiterref, &itc);
	ss_iteropen(ss_bufiterref, &itc, &vlistc, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 3);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itc;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &r, &m, SS_GTE);

	i = 0;
	while (ss_iteratorhas(&merge)) {
		sv *v = (sv*)ss_iteratorof(&merge);
		t( *(int*)sv_key(v, &r, 0) == key );
		if (i == 0) {
			t( sv_flags(v) == 0 );
		} else {
			t( (sv_flags(v) | sv_mergeisdup(&merge)) == (0|SVDUP) );
		}
		ss_iteratornext(&merge);
		i++;
	}
	t( i == 15 );
	ss_iteratorclose(&merge);

	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista, sizeof(sv*));
	while (ss_iteratorhas(&ita)) {
		sv *v = (sv*)ss_iteratorof(&ita);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&ita);
	}
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb, sizeof(sv*));
	while (ss_iteratorhas(&itb)) {
		sv *v = (sv*)ss_iteratorof(&itb);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&itb);
	}
	ss_iterinit(ss_bufiterref, &itc);
	ss_iteropen(ss_bufiterref, &itc, &vlistc, sizeof(sv*));
	while (ss_iteratorhas(&itc)) {
		sv *v = (sv*)ss_iteratorof(&itc);
		ss_free(&a, v->v);
		ss_free(&a, v);
		ss_iteratornext(&itc);
	}
	ss_buffree(&vlista, &a);
	ss_buffree(&vlistb, &a);
	ss_buffree(&vlistc, &a);
	sv_mergefree(&m, &a);
	sr_schemefree(&cmp, &a);
}

stgroup *svmergeiter_group(void)
{
	stgroup *group = st_group("svmergeiter");
	st_groupadd(group, st_test("merge_a", svmergeiter_merge_a));
	st_groupadd(group, st_test("merge_b", svmergeiter_merge_b));
	st_groupadd(group, st_test("merge_ab", svmergeiter_merge_ab));
	st_groupadd(group, st_test("merge_abc", svmergeiter_merge_abc));
	st_groupadd(group, st_test("merge_ba", svmergeiter_merge_ba));
	st_groupadd(group, st_test("merge_dup_ab", svmergeiter_merge_dup_ab));
	st_groupadd(group, st_test("merge_dup_a_chain", svmergeiter_merge_dup_a_chain));
	st_groupadd(group, st_test("merge_dup_ab_chain", svmergeiter_merge_dup_ab_chain));
	st_groupadd(group, st_test("merge_dup_abc_chain", svmergeiter_merge_dup_abc_chain));
	return group;
}
