
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
svsiftiter_valloc(sra *a, svlocal *l)
{
	sv lv;
	svinit(&lv, &sv_localif, l, NULL);
	svv *kv = sv_valloc(a, &lv);
	sv *v = sr_malloc(a, sizeof(sv));
	svinit(v, &sv_vif, kv, NULL);
	return v;
}

static void
svsiftiter_sift(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);
	int i = 0;
	while (i < 10)
	{
		svlocal l;
		l.lsn         = 10 - i,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		l.valueoffset = 0;
		sv *v = svsiftiter_valloc(&a, &l);
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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 20 * (sizeof(svv) + sizeof(i));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 10ULL, 0);

	i = 0;
	while (sr_iterhas(&sift)) {
		sv *v = (sv*)sr_iterof(&sift);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 10 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&sift);
		i++;
	}
	t( i == 10 );
	sr_iterclose(&sift);

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
svsiftiter_limit(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);
	int i = 0;
	while (i < 18)
	{
		svlocal l;
		l.lsn         = 18 - i,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		l.valueoffset = 0;
		sv *v = svsiftiter_valloc(&a, &l);
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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 5 * (sizeof(svv) + sizeof(i));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 18ULL, 0);

	i = 0;
	while (sr_iterhas(&sift)) {
		sv *v = (sv*)sr_iterof(&sift);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&sift);
		i++;
	}
	t( i == 5 );
	int j = 0;
	sv_siftiter_resume(&sift);
	while (sr_iterhas(&sift)) {
		sv *v = (sv*)sr_iterof(&sift);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&sift);
		i++;
		j++;
	}
	t( j == 5 );
	j = 0;
	sv_siftiter_resume(&sift);
	while (sr_iterhas(&sift)) {
		sv *v = (sv*)sr_iterof(&sift);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&sift);
		i++;
		j++;
	}
	t( j == 5 );
	j = 0;
	sv_siftiter_resume(&sift);
	while (sr_iterhas(&sift)) {
		sv *v = (sv*)sr_iterof(&sift);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&sift);
		i++;
		j++;
	}
	t( j == 3 );
	t( i == 18 );
	sr_iterclose(&sift);

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
svsiftiter_limit_small(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);
	int i = 0;
	while (i < 18)
	{
		svlocal l;
		l.lsn         = 18 - i,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		l.valueoffset = 0;
		sv *v = svsiftiter_valloc(&a, &l);
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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(i));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 18ULL, 0);

	i = 0;
	while (sr_iterhas(&sift)) {
		sv *v = (sv*)sr_iterof(&sift);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&sift);
		i++;
	}
	t( i == 1 );
	int j = 0;
	sv_siftiter_resume(&sift);
	while (sr_iterhas(&sift)) {
		sv *v = (sv*)sr_iterof(&sift);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&sift);
		i++;
		j++;
	}
	t( j == 1 );
	j = 0;
	sv_siftiter_resume(&sift);
	while (sr_iterhas(&sift)) {
		sv *v = (sv*)sr_iterof(&sift);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&sift);
		i++;
		j++;
	}
	t( j == 1 );
	j = 0;
	sv_siftiter_resume(&sift);
	while (sr_iterhas(&sift)) {
		sv *v = (sv*)sr_iterof(&sift);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&sift);
		i++;
		j++;
	}
	t( j == 1 );
	t( i == 4 );
	sr_iterclose(&sift);

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
addv(srbuf *list, sra *a, uint64_t lsn, int flags, char *key, int keysize)
{
	svlocal l;
	l.lsn         = lsn;
	l.flags       = flags;
	l.key         = key;
	l.keysize     = keysize;
	l.value       = NULL;
	l.valuesize   = 0;
	l.valueoffset = 0;
	sv *v = svsiftiter_valloc(a, &l);
	sr_bufadd(list, a, &v, sizeof(sv**));
}

static void
checkv(stc *cx, sriter *i, uint64_t lsn, int flags, int key)
{
	sv *v = (sv*)sr_iterof(i);
	t( *(int*)svkey(v) == key );
	t( svlsn(v) == lsn );
	t( svflags(v) == flags );
}

static void
svsiftiter_dup_lsn_gt(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVSET|SVDUP, (char*)&key, sizeof(key));

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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 10ULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&sift)) {
		sv *v = (sv*)sr_iterof(&sift);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVSET );
		else
			t( svflags(v) == (SVSET | SVDUP) );
		sr_iternext(&sift);
		i++;
	}
	t( i == 1 );
	sr_iterclose(&sift);

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
svsiftiter_dup_lsn_lt0(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVSET|SVDUP, (char*)&key, sizeof(key));

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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 9ULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&sift)) {
		sv *v = (sv*)sr_iterof(&sift);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVSET );
		else
			t( svflags(v) == (SVSET | SVDUP) );
		sr_iternext(&sift);
		i++;
	}
	t( i == 2 );
	sr_iterclose(&sift);

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
svsiftiter_dup_lsn_lt1(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVSET|SVDUP, (char*)&key, sizeof(key));

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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 8ULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&sift)) {
		sv *v = (sv*)sr_iterof(&sift);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVSET );
		else
			t( svflags(v) == (SVSET | SVDUP) );
		sr_iternext(&sift);
		i++;
	}
	t( i == 3 );
	sr_iterclose(&sift);

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
svsiftiter_dup_lsn_lt2(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVSET|SVDUP, (char*)&key, sizeof(key));

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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 2ULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&sift)) {
		sv *v = (sv*)sr_iterof(&sift);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVSET );
		else
			t( svflags(v) == (SVSET | SVDUP) );
		sr_iternext(&sift);
		i++;
	}
	t( i == 3 );
	sr_iterclose(&sift);

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
svsiftiter_dup_lsn_gt_chain(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;
	int key2 = 8;
	int key3 = 9;
	int key4 = 10;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a, 12, SVSET, (char*)&key2, sizeof(key2));
	addv(&vlista, &a, 11, SVSET|SVDUP, (char*)&key2, sizeof(key2));
	addv(&vlista, &a, 13, SVSET, (char*)&key3, sizeof(key3));
	addv(&vlista, &a, 14, SVSET, (char*)&key4, sizeof(key4));

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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 15ULL, 0);

	checkv(cx, &sift, 10, SVSET, key);
	sr_iternext(&sift);
	checkv(cx, &sift, 12, SVSET, key2);
	sr_iternext(&sift);
	checkv(cx, &sift, 13, SVSET, key3);
	sr_iternext(&sift);
	checkv(cx, &sift, 14, SVSET, key4);
	sr_iternext(&sift);
	sr_iterclose(&sift);

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
svsiftiter_dup_lsn_lt0_chain(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;
	int key2 = 8;
	int key3 = 9;
	int key4 = 10;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a, 12, SVSET, (char*)&key2, sizeof(key2));
	addv(&vlista, &a, 11, SVSET|SVDUP, (char*)&key2, sizeof(key2));
	addv(&vlista, &a, 13, SVSET, (char*)&key3, sizeof(key3));
	addv(&vlista, &a, 14, SVSET, (char*)&key4, sizeof(key4));

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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 11ULL, 0);

	checkv(cx, &sift, 10, SVSET, key);
	sr_iternext(&sift);
	checkv(cx, &sift, 12, SVSET, key2);
	sr_iternext(&sift);
	checkv(cx, &sift, 11, SVSET|SVDUP, key2);
	sr_iternext(&sift);
	checkv(cx, &sift, 13, SVSET, key3);
	sr_iternext(&sift);
	checkv(cx, &sift, 14, SVSET, key4);
	sr_iternext(&sift);
	sr_iterclose(&sift);

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
svsiftiter_dup_lsn_lt1_chain(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;
	int key2 = 8;
	int key3 = 9;
	int key4 = 10;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a, 12, SVSET, (char*)&key2, sizeof(key2));
	addv(&vlista, &a, 11, SVSET|SVDUP, (char*)&key2, sizeof(key2));
	addv(&vlista, &a, 13, SVSET, (char*)&key3, sizeof(key3));
	addv(&vlista, &a, 14, SVSET, (char*)&key4, sizeof(key4));

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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 9ULL, 0);

	checkv(cx, &sift, 10, SVSET, key);
	sr_iternext(&sift);
	checkv(cx, &sift,  9, SVSET|SVDUP, key);
	sr_iternext(&sift);
	checkv(cx, &sift, 12, SVSET, key2);
	sr_iternext(&sift);
	checkv(cx, &sift, 11, SVSET|SVDUP, key2);
	sr_iternext(&sift);
	checkv(cx, &sift, 13, SVSET, key3);
	sr_iternext(&sift);
	checkv(cx, &sift, 14, SVSET, key4);
	sr_iternext(&sift);
	sr_iterclose(&sift);

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
svsiftiter_dup_lsn_lt2_chain(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;
	int key2 = 8;
	int key3 = 9;
	int key4 = 10;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a, 12, SVSET, (char*)&key2, sizeof(key2));
	addv(&vlista, &a, 11, SVSET|SVDUP, (char*)&key2, sizeof(key2));
	addv(&vlista, &a, 13, SVSET, (char*)&key3, sizeof(key3));
	addv(&vlista, &a, 14, SVSET, (char*)&key4, sizeof(key4));

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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 3ULL, 0);

	checkv(cx, &sift, 10, SVSET, key);
	sr_iternext(&sift);
	checkv(cx, &sift,  9, SVSET|SVDUP, key);
	sr_iternext(&sift);
	checkv(cx, &sift,  8, SVSET|SVDUP, key);
	sr_iternext(&sift);
	checkv(cx, &sift, 12, SVSET, key2);
	sr_iternext(&sift);
	checkv(cx, &sift, 11, SVSET|SVDUP, key2);
	sr_iternext(&sift);
	checkv(cx, &sift, 13, SVSET, key3);
	sr_iternext(&sift);
	checkv(cx, &sift, 14, SVSET, key4);
	sr_iternext(&sift);
	sr_iterclose(&sift);

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
svsiftiter_dup_lsn_limit0(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVSET|SVDUP, (char*)&key, sizeof(key));

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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 15ULL, 0);

	checkv(cx, &sift, 10, SVSET, key);
	sr_iternext(&sift);
	t( sr_iterhas(&sift) == 0 );
	sr_iterclose(&sift);

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
svsiftiter_dup_lsn_limit1(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVSET|SVDUP, (char*)&key, sizeof(key));

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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 9ULL);

	checkv(cx, &sift, 10, SVSET, key);
	sr_iternext(&sift);
	checkv(cx, &sift,  9, SVSET|SVDUP, key);
	sr_iternext(&sift);
	t( sr_iterhas(&sift) == 0 );
	sr_iterclose(&sift);

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
svsiftiter_dup_lsn_limit2(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;
	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVSET|SVDUP, (char*)&key, sizeof(key));

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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 5ULL, 0);

	checkv(cx, &sift, 10, SVSET, key);
	sr_iternext(&sift);
	checkv(cx, &sift,  9, SVSET|SVDUP, key);
	sr_iternext(&sift);
	checkv(cx, &sift,  8, SVSET|SVDUP, key);
	sr_iternext(&sift);
	t( sr_iterhas(&sift) == 0 );
	sr_iterclose(&sift);

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
svsiftiter_dup_lsn_limit3(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int k = 0;
	int key = 7;
	addv(&vlista, &a, 412 - k, SVSET, (char*)&key, sizeof(key));
	while (k < 411) {
		addv(&vlista, &a, 411 - k, SVSET|SVDUP, (char*)&key, sizeof(key));
		k++;
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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 2 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 500ULL, 0);

	t(sr_iterhas(&sift) == 1);
	checkv(cx, &sift, 412, SVSET, key);
	sr_iternext(&sift);
	t(sr_iterhas(&sift) == 0);
	sr_iterclose(&sift);

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
svsiftiter_dup_lsn_limit4(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int k = 0;
	int key = 7;
	addv(&vlista, &a, 412 - k, SVSET, (char*)&key, sizeof(key));
	while (k < 411) {
		addv(&vlista, &a, 411 - k, SVSET|SVDUP, (char*)&key, sizeof(key));
		k++;
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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(k));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 0ULL, 0);

	k = 0;
	while (sr_iterhas(&sift))
	{
		if (k == 0)
			checkv(cx, &sift, 412 - k, SVSET, key);
		else
			checkv(cx, &sift, 412 - k, SVSET|SVDUP, key);
		sr_iternext(&sift);
		k++;
	}
	t( k == 412 );
	sr_iterclose(&sift);

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
svsiftiter_dup_lsn_limit5(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int k = 0;
	int key = 7;
	addv(&vlista, &a, 412 - k, SVSET, (char*)&key, sizeof(key));
	while (k < 411) {
		addv(&vlista, &a, 411 - k, SVSET|SVDUP, (char*)&key, sizeof(key));
		k++;
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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(k));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 0ULL, 0);

	k = 0;
	while (sr_iterhas(&sift))
	{
		if (k == 0)
			checkv(cx, &sift, 412 - k, SVSET, key);
		else
			checkv(cx, &sift, 412 - k, SVSET|SVDUP, key);
		sr_iternext(&sift);
		k++;
	}
	t( k == 412 );
	sr_iterclose(&sift);

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
svsiftiter_delete0(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));

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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 10ULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&sift)) {
		sv *v = (sv*)sr_iterof(&sift);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVSET );
		else
			t( svflags(v) == (SVDELETE | SVDUP) );
		sr_iternext(&sift);
		i++;
	}
	t( i == 1 );
	sr_iterclose(&sift);

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
svsiftiter_delete1(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));

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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 9ULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&sift)) {
		sv *v = (sv*)sr_iterof(&sift);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVSET );
		else
			t( svflags(v) == (SVDELETE | SVDUP) );
		sr_iternext(&sift);
		i++;
	}
	t( i == 2 );
	sr_iterclose(&sift);

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
svsiftiter_delete2(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));

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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 8ULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&sift)) {
		sv *v = (sv*)sr_iterof(&sift);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVSET );
		else
			t( svflags(v) == (SVDELETE | SVDUP) );
		sr_iternext(&sift);
		i++;
	}
	t( i == 3 );
	sr_iterclose(&sift);

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
svsiftiter_delete3(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVDELETE, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));

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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 7ULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&sift)) {
		sv *v = (sv*)sr_iterof(&sift);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVDELETE );
		else
			t( svflags(v) == (SVDELETE | SVDUP) );
		sr_iternext(&sift);
		i++;
	}
	t( i == 3 );
	sr_iterclose(&sift);

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
svsiftiter_delete4(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVDELETE, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));

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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 10ULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&sift)) {
		sv *v = (sv*)sr_iterof(&sift);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVDELETE );
		else
			t( svflags(v) == (SVDELETE | SVDUP) );
		sr_iternext(&sift);
		i++;
	}
	t( i == 1 );
	sr_iterclose(&sift);

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
svsiftiter_delete5(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVDELETE, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));

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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 11ULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&sift)) {
		sv *v = (sv*)sr_iterof(&sift);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVDELETE );
		else
			t( svflags(v) == (SVDELETE | SVDUP) );
		sr_iternext(&sift);
		i++;
	}
	t( i == 0 );
	sr_iterclose(&sift);

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
svsiftiter_delete6(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 6;
	addv(&vlista, &a, 12, SVSET, (char*)&key, sizeof(key));
	key = 7;
	addv(&vlista, &a, 10, SVDELETE, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	key = 10;
	addv(&vlista, &a, 11, SVSET, (char*)&key, sizeof(key));

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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 13ULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&sift)) {
		sv *v = (sv*)sr_iterof(&sift);
		if (i == 0) {
			t( *(int*)svkey(v) == 6 );
			t( svlsn(v) == 12 );
		} else {
			t( *(int*)svkey(v) == 10 );
			t( svlsn(v) == 11 );
		}
		t( svflags(v) == SVSET );
		sr_iternext(&sift);
		i++;
	}
	t( i == 2 );
	sr_iterclose(&sift);

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
svsiftiter_delete7(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 6;
	addv(&vlista, &a, 12, SVSET, (char*)&key, sizeof(key));
	key = 7;
	addv(&vlista, &a, 10, SVDELETE, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	key = 10;
	addv(&vlista, &a, 11, SVSET, (char*)&key, sizeof(key));

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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 10ULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&sift)) {
		sv *v = (sv*)sr_iterof(&sift);
		if (i == 0) {
			t( *(int*)svkey(v) == 6 );
			t( svflags(v) == SVSET );
			t( svlsn(v) == 12 );
		} else
		if (i == 1) {
			t( *(int*)svkey(v) == 7 );
			t( svflags(v) == SVDELETE );
			t( svlsn(v) == 10 );
		} else {
			t( *(int*)svkey(v) == 10 );
			t( svflags(v) == SVSET );
			t( svlsn(v) == 11 );
		}
		sr_iternext(&sift);
		i++;
	}
	t( i == 3 );
	sr_iterclose(&sift);

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
svsiftiter_delete8(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 6;
	addv(&vlista, &a, 12, SVSET, (char*)&key, sizeof(key));
	key = 7;
	addv(&vlista, &a, 10, SVDELETE, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	key = 10;
	addv(&vlista, &a, 11, SVSET, (char*)&key, sizeof(key));

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

	sriter sift;
	sr_iterinit(&sift, &sv_siftiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&sift, &merge, limit, sizeof(svv), 9ULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&sift)) {
		sv *v = (sv*)sr_iterof(&sift);
		if (i == 0) {
			t( *(int*)svkey(v) == 6 );
			t( svflags(v) == SVSET );
			t( svlsn(v) == 12 );
		} else
		if (i == 1) {
			t( *(int*)svkey(v) == 7 );
			t( svflags(v) == SVDELETE );
			t( svlsn(v) == 10 );
		} else
		if (i == 2) {
			t( *(int*)svkey(v) == 7 );
			t( svflags(v) == (SVDELETE|SVDUP) );
			t( svlsn(v) ==  9 );
		} else {
			t( *(int*)svkey(v) == 10 );
			t( svflags(v) == SVSET );
			t( svlsn(v) == 11 );
		}
		sr_iternext(&sift);
		i++;
	}
	t( i == 4 );
	sr_iterclose(&sift);

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

stgroup *svsiftiter_group(void)
{
	stgroup *group = st_group("svsiftiter");
	st_groupadd(group, st_test("sift", svsiftiter_sift));
	st_groupadd(group, st_test("sift_limit", svsiftiter_limit));
	st_groupadd(group, st_test("sift_limit_small", svsiftiter_limit_small));
	st_groupadd(group, st_test("sift_dup_lsn_gt", svsiftiter_dup_lsn_gt));
	st_groupadd(group, st_test("sift_dup_lsn_lt0", svsiftiter_dup_lsn_lt0));
	st_groupadd(group, st_test("sift_dup_lsn_lt1", svsiftiter_dup_lsn_lt1));
	st_groupadd(group, st_test("sift_dup_lsn_lt2", svsiftiter_dup_lsn_lt2));
	st_groupadd(group, st_test("sift_dup_lsn_gt_chain", svsiftiter_dup_lsn_gt_chain));
	st_groupadd(group, st_test("sift_dup_lsn_lt0_chain", svsiftiter_dup_lsn_lt0_chain));
	st_groupadd(group, st_test("sift_dup_lsn_lt1_chain", svsiftiter_dup_lsn_lt1_chain));
	st_groupadd(group, st_test("sift_dup_lsn_lt2_chain", svsiftiter_dup_lsn_lt2_chain));
	st_groupadd(group, st_test("sift_dup_lsn_limit0", svsiftiter_dup_lsn_limit0));
	st_groupadd(group, st_test("sift_dup_lsn_limit1", svsiftiter_dup_lsn_limit1));
	st_groupadd(group, st_test("sift_dup_lsn_limit2", svsiftiter_dup_lsn_limit2));
	st_groupadd(group, st_test("sift_dup_lsn_limit3", svsiftiter_dup_lsn_limit3));
	st_groupadd(group, st_test("sift_dup_lsn_limit4", svsiftiter_dup_lsn_limit4));
	st_groupadd(group, st_test("sift_dup_lsn_limit5", svsiftiter_dup_lsn_limit5));
	st_groupadd(group, st_test("sift_delete0", svsiftiter_delete0));
	st_groupadd(group, st_test("sift_delete1", svsiftiter_delete1));
	st_groupadd(group, st_test("sift_delete2", svsiftiter_delete2));
	st_groupadd(group, st_test("sift_delete3", svsiftiter_delete3));
	st_groupadd(group, st_test("sift_delete4", svsiftiter_delete4));
	st_groupadd(group, st_test("sift_delete5", svsiftiter_delete5));
	st_groupadd(group, st_test("sift_delete6", svsiftiter_delete6));
	st_groupadd(group, st_test("sift_delete7", svsiftiter_delete7));
	st_groupadd(group, st_test("sift_delete8", svsiftiter_delete8));
	return group;
}
