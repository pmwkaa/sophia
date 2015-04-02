
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

static sv*
svmergeiter_valloc(sra *a, svlocal *l)
{
	sv lv;
	sv_init(&lv, &sv_localif, l, NULL);
	svv *kv = sv_valloc(a, &lv);
	sv *v = sr_malloc(a, sizeof(sv));
	sv_init(v, &sv_vif, kv, NULL);
	return v;
}

static void
svmergeiter_merge_a(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL, NULL, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);
	int i = 0;
	while (i < 10)
	{
		svlocal l;
		l.lsn         = i,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		sv *v = svmergeiter_valloc(&a, &l);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &r);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &r);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &r);
	sr_iteropen(&merge, &m, SR_GTE);

	i = 0;
	while (sr_iterhas(&merge)) {
		sv *v = (sv*)sr_iterof(&merge);
		t( *(int*)sv_key(v) == i );
		t( sv_lsn(v) == i );
		t( sv_flags(v) == SVSET );
		sr_iternext(&merge);
		i++;
	}
	t( i == 10 );
	sr_iterclose(&merge);

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
}

static void
svmergeiter_merge_b(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL, NULL, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);
	int i = 0;
	while (i < 10)
	{
		svlocal l;
		l.lsn         = i,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		sv *v = svmergeiter_valloc(&a, &l);
		t(sr_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &r);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &r);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &r);
	sr_iteropen(&merge, &m, SR_GTE);

	i = 0;
	while (sr_iterhas(&merge)) {
		sv *v = (sv*)sr_iterof(&merge);
		t( *(int*)sv_key(v) == i );
		t( sv_lsn(v) == i );
		t( sv_flags(v) == SVSET );
		sr_iternext(&merge);
		i++;
	}
	t( i == 10 );
	sr_iterclose(&merge);

	sr_iterinit(&itb, &sr_bufiterref, NULL);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));
	while (sr_iterhas(&itb)) {
		sv *v = (sv*)sr_iterof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&itb);
	}
	sr_buffree(&vlistb, &a);
	sv_mergefree(&m, &a);
}

static void
svmergeiter_merge_ab(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL, NULL, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);
	int i = 0;
	while (i < 5)
	{
		svlocal l;
		l.lsn         = i,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		sv *v = svmergeiter_valloc(&a, &l);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	while (i < 10)
	{
		svlocal l;
		l.lsn         = i,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		sv *v = svmergeiter_valloc(&a, &l);
		t(sr_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
		i++;
	}

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &r);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &r);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &r);
	sr_iteropen(&merge, &m, SR_GTE);

	i = 0;
	while (sr_iterhas(&merge)) {
		sv *v = (sv*)sr_iterof(&merge);
		t( *(int*)sv_key(v) == i );
		t( sv_lsn(v) == i );
		t( sv_flags(v) == SVSET );
		sr_iternext(&merge);
		i++;
	}
	t( i == 10 );
	sr_iterclose(&merge);

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_iterinit(&itb, &sr_bufiterref, NULL);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));
	while (sr_iterhas(&itb)) {
		sv *v = (sv*)sr_iterof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&itb);
	}
	sr_buffree(&vlista, &a);
	sr_buffree(&vlistb, &a);
	sv_mergefree(&m, &a);
}

static void
svmergeiter_merge_abc(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL, NULL, NULL);

	srbuf vlista;
	srbuf vlistb;
	srbuf vlistc;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);
	sr_bufinit(&vlistc);
	int i = 0;
	while (i < 5)
	{
		svlocal l;
		l.lsn         = i,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		sv *v = svmergeiter_valloc(&a, &l);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	while (i < 10)
	{
		svlocal l;
		l.lsn         = i,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		sv *v = svmergeiter_valloc(&a, &l);
		t(sr_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	while (i < 15)
	{
		svlocal l;
		l.lsn         = i,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		sv *v = svmergeiter_valloc(&a, &l);
		t(sr_bufadd(&vlistc, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &r);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &r);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));
	sriter itc;
	sr_iterinit(&itc, &sr_bufiterref, &r);
	sr_iteropen(&itc, &vlistc, sizeof(sv*));

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
	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &r);
	sr_iteropen(&merge, &m, SR_GTE);

	i = 0;
	while (sr_iterhas(&merge)) {
		sv *v = (sv*)sr_iterof(&merge);
		t( *(int*)sv_key(v) == i );
		t( sv_lsn(v) == i );
		t( sv_flags(v) == SVSET );
		sr_iternext(&merge);
		i++;
	}
	t( i == 15 );
	sr_iterclose(&merge);

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_iterinit(&itb, &sr_bufiterref, NULL);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));
	while (sr_iterhas(&itb)) {
		sv *v = (sv*)sr_iterof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&itb);
	}
	sr_iterinit(&itc, &sr_bufiterref, NULL);
	sr_iteropen(&itc, &vlistc, sizeof(sv*));
	while (sr_iterhas(&itc)) {
		sv *v = (sv*)sr_iterof(&itc);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&itc);
	}
	sr_buffree(&vlista, &a);
	sr_buffree(&vlistb, &a);
	sr_buffree(&vlistc, &a);
	sv_mergefree(&m, &a);
}

static void
svmergeiter_merge_ba(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL, NULL, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);
	int i = 0;
	while (i < 5)
	{
		svlocal l;
		l.lsn         = i,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		sv *v = svmergeiter_valloc(&a, &l);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	while (i < 10)
	{
		svlocal l;
		l.lsn         = i,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		sv *v = svmergeiter_valloc(&a, &l);
		t(sr_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
		i++;
	}

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &r);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &r);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 3);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &r);
	sr_iteropen(&merge, &m, SR_GTE);

	i = 0;
	while (sr_iterhas(&merge)) {
		sv *v = (sv*)sr_iterof(&merge);
		t( *(int*)sv_key(v) == i );
		t( sv_lsn(v) == i );
		t( sv_flags(v) == SVSET );
		sr_iternext(&merge);
		i++;
	}
	t( i == 10 );
	sr_iterclose(&merge);

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_iterinit(&itb, &sr_bufiterref, NULL);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));
	while (sr_iterhas(&itb)) {
		sv *v = (sv*)sr_iterof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&itb);
	}
	sr_buffree(&vlista, &a);
	sr_buffree(&vlistb, &a);
	sv_mergefree(&m, &a);
}

static void
svmergeiter_merge_dup_ab(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL, NULL, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);
	int i = 0;
	int lsn = 10;
	while (i < 5)
	{
		svlocal l;
		l.lsn         = lsn,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		sv *v = svmergeiter_valloc(&a, &l);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
	}
	i = 0 ;
	while (i < 5)
	{
		svlocal l;
		l.lsn         = lsn,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		sv *v = svmergeiter_valloc(&a, &l);
		t(sr_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
	}

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &r);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &r);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &r);
	sr_iteropen(&merge, &m, SR_GTE);

	int key = 0;
	i = 0;
	while (sr_iterhas(&merge)) {
		sv *v = (sv*)sr_iterof(&merge);
		if ((i % 2) == 0) {
			t( *(int*)sv_key(v) == key );
			t( sv_flags(v) == SVSET );
			key++;
		} else {
			t( *(int*)sv_key(v) == key - 1);
			t( (sv_flags(v) | sv_mergeisdup(&merge)) == (SVSET|SVDUP) );
		}
		sr_iternext(&merge);
		i++;
	}
	t( i == 10 );
	sr_iterclose(&merge);

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_iterinit(&itb, &sr_bufiterref, NULL);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));
	while (sr_iterhas(&itb)) {
		sv *v = (sv*)sr_iterof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&itb);
	}
	sr_buffree(&vlista, &a);
	sr_buffree(&vlistb, &a);
	sv_mergefree(&m, &a);
}

static void
svmergeiter_merge_dup_a_chain(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL, NULL, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);
	int key = 7;
	int lsn = 5;
	int i = 0;
	while (i < 5)
	{
		svlocal l;
		l.lsn         = lsn,
		l.flags       = SVSET | ((i > 0) ? SVDUP: 0),
		l.key         = &key;
		l.keysize     = sizeof(key);
		l.value       = NULL;
		l.valuesize   = 0;
		sv *v = svmergeiter_valloc(&a, &l);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
	}

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &r);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &r);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &r);
	sr_iteropen(&merge, &m, SR_GTE);

	i = 0;
	while (sr_iterhas(&merge)) {
		sv *v = (sv*)sr_iterof(&merge);
		t( *(int*)sv_key(v) == key );
		if (i == 0) {
			t( sv_flags(v) == SVSET );
		} else {
			t( (sv_flags(v) | sv_mergeisdup(&merge)) == (SVSET|SVDUP) );
		}
		sr_iternext(&merge);
		i++;
	}
	t( i == 5 );
	sr_iterclose(&merge);

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_iterinit(&itb, &sr_bufiterref, NULL);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));
	while (sr_iterhas(&itb)) {
		sv *v = (sv*)sr_iterof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&itb);
	}
	sr_buffree(&vlista, &a);
	sr_buffree(&vlistb, &a);
	sv_mergefree(&m, &a);
}

static void
svmergeiter_merge_dup_ab_chain(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL, NULL, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);
	int lsn = 10;
	int key = 7;
	int i = 0;
	while (i < 5)
	{
		svlocal l;
		l.lsn         = lsn,
		l.flags       = SVSET | ((i > 0) ? SVDUP: 0),
		l.key         = &key;
		l.keysize     = sizeof(key);
		l.value       = NULL;
		l.valuesize   = 0;
		sv *v = svmergeiter_valloc(&a, &l);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
	}
	i = 0 ;
	while (i < 5)
	{
		svlocal l;
		l.lsn         = lsn,
		l.flags       = SVSET | ((i > 0) ? SVDUP: 0),
		l.key         = &key;
		l.keysize     = sizeof(key);
		l.value       = NULL;
		l.valuesize   = 0;
		sv *v = svmergeiter_valloc(&a, &l);
		t(sr_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
	}

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &r);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &r);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &r);
	sr_iteropen(&merge, &m, SR_GTE);

	i = 0;
	while (sr_iterhas(&merge)) {
		sv *v = (sv*)sr_iterof(&merge);
		t( *(int*)sv_key(v) == key );
		if (i == 0) {
			t( sv_flags(v) == SVSET );
		} else {
			t( (sv_flags(v) | sv_mergeisdup(&merge)) == (SVSET|SVDUP) );
		}
		sr_iternext(&merge);
		i++;
	}
	t( i == 10 );
	sr_iterclose(&merge);

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_iterinit(&itb, &sr_bufiterref, NULL);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));
	while (sr_iterhas(&itb)) {
		sv *v = (sv*)sr_iterof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&itb);
	}
	sr_buffree(&vlista, &a);
	sr_buffree(&vlistb, &a);
	sv_mergefree(&m, &a);
}

static void
svmergeiter_merge_dup_abc_chain(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL, NULL, NULL);

	srbuf vlista;
	srbuf vlistb;
	srbuf vlistc;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);
	sr_bufinit(&vlistc);
	int lsn = 15;
	int key = 7;
	int i = 0;
	while (i < 5)
	{
		svlocal l;
		l.lsn         = lsn,
		l.flags       = SVSET | ((i > 0) ? SVDUP: 0),
		l.key         = &key;
		l.keysize     = sizeof(key);
		l.value       = NULL;
		l.valuesize   = 0;
		sv *v = svmergeiter_valloc(&a, &l);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
	}
	i = 0 ;
	while (i < 5)
	{
		svlocal l;
		l.lsn         = lsn,
		l.flags       = SVSET | ((i > 0) ? SVDUP: 0),
		l.key         = &key;
		l.keysize     = sizeof(key);
		l.value       = NULL;
		l.valuesize   = 0;
		sv *v = svmergeiter_valloc(&a, &l);
		t(sr_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
	}
	i = 0 ;
	while (i < 5)
	{
		svlocal l;
		l.lsn         = lsn,
		l.flags       = SVSET | ((i > 0) ? SVDUP: 0),
		l.key         = &key;
		l.keysize     = sizeof(key);
		l.value       = NULL;
		l.valuesize   = 0;
		sv *v = svmergeiter_valloc(&a, &l);
		t(sr_bufadd(&vlistc, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
	}

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &r);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &r);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));
	sriter itc;
	sr_iterinit(&itc, &sr_bufiterref, &r);
	sr_iteropen(&itc, &vlistc, sizeof(sv*));

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
	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &r);
	sr_iteropen(&merge, &m, SR_GTE);

	i = 0;
	while (sr_iterhas(&merge)) {
		sv *v = (sv*)sr_iterof(&merge);
		t( *(int*)sv_key(v) == key );
		if (i == 0) {
			t( sv_flags(v) == SVSET );
		} else {
			t( (sv_flags(v) | sv_mergeisdup(&merge)) == (SVSET|SVDUP) );
		}
		sr_iternext(&merge);
		i++;
	}
	t( i == 15 );
	sr_iterclose(&merge);

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_iterinit(&itb, &sr_bufiterref, NULL);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));
	while (sr_iterhas(&itb)) {
		sv *v = (sv*)sr_iterof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&itb);
	}
	sr_iterinit(&itc, &sr_bufiterref, NULL);
	sr_iteropen(&itc, &vlistc, sizeof(sv*));
	while (sr_iterhas(&itc)) {
		sv *v = (sv*)sr_iterof(&itc);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&itc);
	}
	sr_buffree(&vlista, &a);
	sr_buffree(&vlistb, &a);
	sr_buffree(&vlistc, &a);
	sv_mergefree(&m, &a);
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
