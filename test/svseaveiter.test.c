
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
svseaveiter_valloc(sra *a, svlocal *l)
{
	sv lv;
	svinit(&lv, &sv_localif, l, NULL);
	svv *kv = sv_valloc(a, &lv);
	sv *v = sr_malloc(a, sizeof(sv));
	svinit(v, &sv_vif, kv, NULL);
	return v;
}

static void
svseaveiter_seave(stc *cx srunused)
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
		sv *v = svseaveiter_valloc(&a, &l);
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 20 * (sizeof(svv) + sizeof(i));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 10ULL);

	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 10 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&seave);
		i++;
	}
	t( i == 10 );
	sr_iterclose(&seave);

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
svseaveiter_limit(stc *cx srunused)
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
		sv *v = svseaveiter_valloc(&a, &l);
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 5 * (sizeof(svv) + sizeof(i));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 18ULL);

	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&seave);
		i++;
	}
	t( i == 5 );
	int j = 0;
	sv_seaveiter_resume(&seave);
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&seave);
		i++;
		j++;
	}
	t( j == 5 );
	j = 0;
	sv_seaveiter_resume(&seave);
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&seave);
		i++;
		j++;
	}
	t( j == 5 );
	j = 0;
	sv_seaveiter_resume(&seave);
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&seave);
		i++;
		j++;
	}
	t( j == 3 );
	t( i == 18 );
	sr_iterclose(&seave);

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
svseaveiter_limit_small(stc *cx srunused)
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
		sv *v = svseaveiter_valloc(&a, &l);
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(i));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 18ULL);

	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&seave);
		i++;
	}
	t( i == 1 );
	int j = 0;
	sv_seaveiter_resume(&seave);
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&seave);
		i++;
		j++;
	}
	t( j == 1 );
	j = 0;
	sv_seaveiter_resume(&seave);
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&seave);
		i++;
		j++;
	}
	t( j == 1 );
	j = 0;
	sv_seaveiter_resume(&seave);
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&seave);
		i++;
		j++;
	}
	t( j == 1 );
	t( i == 4 );
	sr_iterclose(&seave);

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
	sv *v = svseaveiter_valloc(a, &l);
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
svseaveiter_dup_lsn_gt(stc *cx srunused)
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 10ULL);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVSET );
		else
			t( svflags(v) == (SVSET | SVDUP) );
		sr_iternext(&seave);
		i++;
	}
	t( i == 1 );
	sr_iterclose(&seave);

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
svseaveiter_dup_lsn_lt0(stc *cx srunused)
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 9ULL);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVSET );
		else
			t( svflags(v) == (SVSET | SVDUP) );
		sr_iternext(&seave);
		i++;
	}
	t( i == 2 );
	sr_iterclose(&seave);

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
svseaveiter_dup_lsn_lt1(stc *cx srunused)
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 8ULL);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVSET );
		else
			t( svflags(v) == (SVSET | SVDUP) );
		sr_iternext(&seave);
		i++;
	}
	t( i == 3 );
	sr_iterclose(&seave);

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
svseaveiter_dup_lsn_lt2(stc *cx srunused)
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 2ULL);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVSET );
		else
			t( svflags(v) == (SVSET | SVDUP) );
		sr_iternext(&seave);
		i++;
	}
	t( i == 3 );
	sr_iterclose(&seave);

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
svseaveiter_dup_lsn_gt_chain(stc *cx srunused)
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 15ULL);

	checkv(cx, &seave, 10, SVSET, key);
	sr_iternext(&seave);
	checkv(cx, &seave, 12, SVSET, key2);
	sr_iternext(&seave);
	checkv(cx, &seave, 13, SVSET, key3);
	sr_iternext(&seave);
	checkv(cx, &seave, 14, SVSET, key4);
	sr_iternext(&seave);
	sr_iterclose(&seave);

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
svseaveiter_dup_lsn_lt0_chain(stc *cx srunused)
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 11ULL);

	checkv(cx, &seave, 10, SVSET, key);
	sr_iternext(&seave);
	checkv(cx, &seave, 12, SVSET, key2);
	sr_iternext(&seave);
	checkv(cx, &seave, 11, SVSET|SVDUP, key2);
	sr_iternext(&seave);
	checkv(cx, &seave, 13, SVSET, key3);
	sr_iternext(&seave);
	checkv(cx, &seave, 14, SVSET, key4);
	sr_iternext(&seave);
	sr_iterclose(&seave);

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
svseaveiter_dup_lsn_lt1_chain(stc *cx srunused)
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 9ULL);

	checkv(cx, &seave, 10, SVSET, key);
	sr_iternext(&seave);
	checkv(cx, &seave,  9, SVSET|SVDUP, key);
	sr_iternext(&seave);
	checkv(cx, &seave, 12, SVSET, key2);
	sr_iternext(&seave);
	checkv(cx, &seave, 11, SVSET|SVDUP, key2);
	sr_iternext(&seave);
	checkv(cx, &seave, 13, SVSET, key3);
	sr_iternext(&seave);
	checkv(cx, &seave, 14, SVSET, key4);
	sr_iternext(&seave);
	sr_iterclose(&seave);

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
svseaveiter_dup_lsn_lt2_chain(stc *cx srunused)
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 3ULL);

	checkv(cx, &seave, 10, SVSET, key);
	sr_iternext(&seave);
	checkv(cx, &seave,  9, SVSET|SVDUP, key);
	sr_iternext(&seave);
	checkv(cx, &seave,  8, SVSET|SVDUP, key);
	sr_iternext(&seave);
	checkv(cx, &seave, 12, SVSET, key2);
	sr_iternext(&seave);
	checkv(cx, &seave, 11, SVSET|SVDUP, key2);
	sr_iternext(&seave);
	checkv(cx, &seave, 13, SVSET, key3);
	sr_iternext(&seave);
	checkv(cx, &seave, 14, SVSET, key4);
	sr_iternext(&seave);
	sr_iterclose(&seave);

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
svseaveiter_dup_lsn_limit0(stc *cx srunused)
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 15ULL);

	checkv(cx, &seave, 10, SVSET, key);
	sr_iternext(&seave);
	t( sr_iterhas(&seave) == 0 );
	sr_iterclose(&seave);

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
svseaveiter_dup_lsn_limit1(stc *cx srunused)
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 9ULL);

	checkv(cx, &seave, 10, SVSET, key);
	sr_iternext(&seave);
	checkv(cx, &seave,  9, SVSET|SVDUP, key);
	sr_iternext(&seave);
	t( sr_iterhas(&seave) == 0 );
	sr_iterclose(&seave);

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
svseaveiter_dup_lsn_limit2(stc *cx srunused)
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 5ULL);

	checkv(cx, &seave, 10, SVSET, key);
	sr_iternext(&seave);
	checkv(cx, &seave,  9, SVSET|SVDUP, key);
	sr_iternext(&seave);
	checkv(cx, &seave,  8, SVSET|SVDUP, key);
	sr_iternext(&seave);
	t( sr_iterhas(&seave) == 0 );
	sr_iterclose(&seave);

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
svseaveiter_dup_lsn_limit3(stc *cx srunused)
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 2 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 500ULL);

	t(sr_iterhas(&seave) == 1);
	checkv(cx, &seave, 412, SVSET, key);
	sr_iternext(&seave);
	t(sr_iterhas(&seave) == 0);
	sr_iterclose(&seave);

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
svseaveiter_dup_lsn_limit4(stc *cx srunused)
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(k));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 0ULL);

	k = 0;
	while (sr_iterhas(&seave))
	{
		if (k == 0)
			checkv(cx, &seave, 412 - k, SVSET, key);
		else
			checkv(cx, &seave, 412 - k, SVSET|SVDUP, key);
		sr_iternext(&seave);
		k++;
	}
	t( k == 412 );
	sr_iterclose(&seave);

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
svseaveiter_dup_lsn_limit5(stc *cx srunused)
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(k));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 0ULL);

	k = 0;
	while (sr_iterhas(&seave))
	{
		if (k == 0)
			checkv(cx, &seave, 412 - k, SVSET, key);
		else
			checkv(cx, &seave, 412 - k, SVSET|SVDUP, key);
		sr_iternext(&seave);
		k++;
	}
	t( k == 412 );
	sr_iterclose(&seave);

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
svseaveiter_delete0(stc *cx srunused)
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 10ULL);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVSET );
		else
			t( svflags(v) == (SVDELETE | SVDUP) );
		sr_iternext(&seave);
		i++;
	}
	t( i == 1 );
	sr_iterclose(&seave);

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
svseaveiter_delete1(stc *cx srunused)
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 9ULL);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVSET );
		else
			t( svflags(v) == (SVDELETE | SVDUP) );
		sr_iternext(&seave);
		i++;
	}
	t( i == 2 );
	sr_iterclose(&seave);

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
svseaveiter_delete2(stc *cx srunused)
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 8ULL);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVSET );
		else
			t( svflags(v) == (SVDELETE | SVDUP) );
		sr_iternext(&seave);
		i++;
	}
	t( i == 3 );
	sr_iterclose(&seave);

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
svseaveiter_delete3(stc *cx srunused)
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 7ULL);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVDELETE );
		else
			t( svflags(v) == (SVDELETE | SVDUP) );
		sr_iternext(&seave);
		i++;
	}
	t( i == 3 );
	sr_iterclose(&seave);

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
svseaveiter_delete4(stc *cx srunused)
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 10ULL);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVDELETE );
		else
			t( svflags(v) == (SVDELETE | SVDUP) );
		sr_iternext(&seave);
		i++;
	}
	t( i == 1 );
	sr_iterclose(&seave);

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
svseaveiter_delete5(stc *cx srunused)
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 11ULL);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVDELETE );
		else
			t( svflags(v) == (SVDELETE | SVDUP) );
		sr_iternext(&seave);
		i++;
	}
	t( i == 0 );
	sr_iterclose(&seave);

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
svseaveiter_delete6(stc *cx srunused)
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 13ULL);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		if (i == 0) {
			t( *(int*)svkey(v) == 6 );
			t( svlsn(v) == 12 );
		} else {
			t( *(int*)svkey(v) == 10 );
			t( svlsn(v) == 11 );
		}
		t( svflags(v) == SVSET );
		sr_iternext(&seave);
		i++;
	}
	t( i == 2 );
	sr_iterclose(&seave);

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
svseaveiter_delete7(stc *cx srunused)
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 10ULL);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
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
		sr_iternext(&seave);
		i++;
	}
	t( i == 3 );
	sr_iterclose(&seave);

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
svseaveiter_delete8(stc *cx srunused)
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

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &r);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	sr_iteropen(&seave, &merge, limit, sizeof(svv), 9ULL);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
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
		sr_iternext(&seave);
		i++;
	}
	t( i == 4 );
	sr_iterclose(&seave);

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

stgroup *svseaveiter_group(void)
{
	stgroup *group = st_group("svseaveiter");
	st_groupadd(group, st_test("seave", svseaveiter_seave));
	st_groupadd(group, st_test("seave_limit", svseaveiter_limit));
	st_groupadd(group, st_test("seave_limit_small", svseaveiter_limit_small));
	st_groupadd(group, st_test("seave_dup_lsn_gt", svseaveiter_dup_lsn_gt));
	st_groupadd(group, st_test("seave_dup_lsn_lt0", svseaveiter_dup_lsn_lt0));
	st_groupadd(group, st_test("seave_dup_lsn_lt1", svseaveiter_dup_lsn_lt1));
	st_groupadd(group, st_test("seave_dup_lsn_lt2", svseaveiter_dup_lsn_lt2));
	st_groupadd(group, st_test("seave_dup_lsn_gt_chain", svseaveiter_dup_lsn_gt_chain));
	st_groupadd(group, st_test("seave_dup_lsn_lt0_chain", svseaveiter_dup_lsn_lt0_chain));
	st_groupadd(group, st_test("seave_dup_lsn_lt1_chain", svseaveiter_dup_lsn_lt1_chain));
	st_groupadd(group, st_test("seave_dup_lsn_lt2_chain", svseaveiter_dup_lsn_lt2_chain));
	st_groupadd(group, st_test("seave_dup_lsn_limit0", svseaveiter_dup_lsn_limit0));
	st_groupadd(group, st_test("seave_dup_lsn_limit1", svseaveiter_dup_lsn_limit1));
	st_groupadd(group, st_test("seave_dup_lsn_limit2", svseaveiter_dup_lsn_limit2));
	st_groupadd(group, st_test("seave_dup_lsn_limit3", svseaveiter_dup_lsn_limit3));
	st_groupadd(group, st_test("seave_dup_lsn_limit4", svseaveiter_dup_lsn_limit4));
	st_groupadd(group, st_test("seave_dup_lsn_limit5", svseaveiter_dup_lsn_limit5));
	st_groupadd(group, st_test("seave_delete0", svseaveiter_delete0));
	st_groupadd(group, st_test("seave_delete1", svseaveiter_delete1));
	st_groupadd(group, st_test("seave_delete2", svseaveiter_delete2));
	st_groupadd(group, st_test("seave_delete3", svseaveiter_delete3));
	st_groupadd(group, st_test("seave_delete4", svseaveiter_delete4));
	st_groupadd(group, st_test("seave_delete5", svseaveiter_delete5));
	st_groupadd(group, st_test("seave_delete6", svseaveiter_delete6));
	st_groupadd(group, st_test("seave_delete7", svseaveiter_delete7));
	st_groupadd(group, st_test("seave_delete8", svseaveiter_delete8));
	return group;
}
