
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <sophia.h>
#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libsv.h>
#include <libso.h>
#include <libst.h>

static void
sv_writeiter_iter(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);
	int i = 0;
	while (i < 10)
	{
		st_svv(&st_r.g, &vlista, 10 - i, 0, i, NULL, 0);
		i++;
	}
	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 20 * (sizeof(svv) + sizeof(i));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 10ULL, 0, 0);

	i = 0;
	while (ss_iteratorhas(&iter)) {
		char *v = ss_iteratorof(&iter);
		t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == i );
		t( sf_lsn(st_r.r.scheme, v) == 10 - i );
		t( sf_flags(st_r.r.scheme, v) == 0 );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 10 );
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_limit(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);
	int i = 0;
	while (i < 18)
	{
		st_svv(&st_r.g, &vlista, 18 - i, 0, i, NULL, 0);
		i++;
	}

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 5 * (sizeof(svv) + sizeof(sfvar) + sizeof(i));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 18ULL, 0, 0);

	i = 0;
	while (ss_iteratorhas(&iter)) {
		char *v = ss_iteratorof(&iter);
		t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == i );
		t( sf_lsn(st_r.r.scheme, v) == 18 - i );
		t( sf_flags(st_r.r.scheme, v) == 0 );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 5 );
	int j = 0;
	sv_writeiter_resume(&iter);
	while (ss_iteratorhas(&iter)) {
		char *v = ss_iteratorof(&iter);
		t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == i );
		t( sf_lsn(st_r.r.scheme, v) == 18 - i );
		t( sf_flags(st_r.r.scheme, v) == 0 );
		ss_iteratornext(&iter);
		i++;
		j++;
	}
	t( j == 5 );
	j = 0;
	sv_writeiter_resume(&iter);
	while (ss_iteratorhas(&iter)) {
		char *v = ss_iteratorof(&iter);
		t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == i );
		t( sf_lsn(st_r.r.scheme, v) == 18 - i );
		t( sf_flags(st_r.r.scheme, v) == 0 );
		ss_iteratornext(&iter);
		i++;
		j++;
	}
	t( j == 5 );
	j = 0;
	sv_writeiter_resume(&iter);
	while (ss_iteratorhas(&iter)) {
		char *v = ss_iteratorof(&iter);
		t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == i );
		t( sf_lsn(st_r.r.scheme, v) == 18 - i );
		t( sf_flags(st_r.r.scheme, v) == 0 );
		ss_iteratornext(&iter);
		i++;
		j++;
	}
	t( j == 3 );
	t( i == 18 );
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_limit_small(void)
{

	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);
	int i = 0;
	while (i < 18)
	{
		st_svv(&st_r.g, &vlista, 18 - i, 0, i, NULL, 0);
		i++;
	}

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(sfvar) + sizeof(i));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 18ULL, 0, 0);

	i = 0;
	while (ss_iteratorhas(&iter)) {
		char *v = ss_iteratorof(&iter);
		t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == i );
		t( sf_lsn(st_r.r.scheme, v) == 18 - i );
		t( sf_flags(st_r.r.scheme, v) == 0 );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 1 );
	int j = 0;
	sv_writeiter_resume(&iter);
	while (ss_iteratorhas(&iter)) {
		char *v = ss_iteratorof(&iter);
		t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == i );
		t( sf_lsn(st_r.r.scheme, v) == 18 - i );
		t( sf_flags(st_r.r.scheme, v) == 0 );
		ss_iteratornext(&iter);
		i++;
		j++;
	}
	t( j == 1 );
	j = 0;
	sv_writeiter_resume(&iter);
	while (ss_iteratorhas(&iter)) {
		char *v = ss_iteratorof(&iter);
		t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == i );
		t( sf_lsn(st_r.r.scheme, v) == 18 - i );
		t( sf_flags(st_r.r.scheme, v) == 0 );
		ss_iteratornext(&iter);
		i++;
		j++;
	}
	t( j == 1 );
	j = 0;
	sv_writeiter_resume(&iter);
	while (ss_iteratorhas(&iter)) {
		char *v = ss_iteratorof(&iter);
		t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == i );
		t( sf_lsn(st_r.r.scheme, v) == 18 - i );
		t( sf_flags(st_r.r.scheme, v) == 0 );
		ss_iteratornext(&iter);
		i++;
		j++;
	}
	t( j == 1 );
	t( i == 4 );
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
checkv(sr *r, ssiter *i, uint64_t lsn, int flags, int key)
{
	char *v = ss_iteratorof(i);
	t( *(int*)sf_field(r->scheme, 0, v, &st_r.size) == key );
	t( sf_lsn(st_r.r.scheme, v) == lsn );
	t( sf_flags(st_r.r.scheme, v) == flags );
}

static void
sv_writeiter_dup_lsn_gt(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int key = 7;
	st_svv(&st_r.g, &vlista, 10, 0, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  9, 0|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  8, 0|SVDUP, key, NULL, 0);

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 10ULL, 0, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		char *v = ss_iteratorof(&iter);
		t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == key );
		t( sf_lsn(st_r.r.scheme, v) == 10 - i );
		if (i == 0)
			t( sf_flags(st_r.r.scheme, v) == 0 );
		else
			t( sf_flags(st_r.r.scheme, v) == (0 | SVDUP) );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 1 );
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_dup_lsn_lt0(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int key = 7;

	st_svv(&st_r.g, &vlista, 10, 0, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  9, 0|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  8, 0|SVDUP, key, NULL, 0);

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 9ULL, 0, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		char *v = ss_iteratorof(&iter);
		t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == key );
		t( sf_lsn(st_r.r.scheme, v) == 10 - i );
		if (i == 0)
			t( sf_flags(st_r.r.scheme, v) == 0 );
		else
			t( sf_flags(st_r.r.scheme, v) == (0 | SVDUP) );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 2 );
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_dup_lsn_lt1(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int key = 7;

	st_svv(&st_r.g, &vlista, 10, 0, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  9, 0|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  8, 0|SVDUP, key, NULL, 0);

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 8ULL, 0, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		char *v = ss_iteratorof(&iter);
		t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == key );
		t( sf_lsn(st_r.r.scheme, v) == 10 - i );
		if (i == 0)
			t( sf_flags(st_r.r.scheme, v) == 0 );
		else
			t( sf_flags(st_r.r.scheme, v) == (0 | SVDUP) );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 3 );
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_dup_lsn_lt2(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int key = 7;

	st_svv(&st_r.g, &vlista, 10, 0, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  9, 0|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  8, 0|SVDUP, key, NULL, 0);

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 2ULL, 0, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		char *v = ss_iteratorof(&iter);
		t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == key );
		t( sf_lsn(st_r.r.scheme, v) == 10 - i );
		if (i == 0)
			t( sf_flags(st_r.r.scheme, v) == 0 );
		else
			t( sf_flags(st_r.r.scheme, v) == (0 | SVDUP) );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 3 );
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_dup_lsn_gt_chain(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int key = 7;
	int key2 = 8;
	int key3 = 9;
	int key4 = 10;

	st_svv(&st_r.g, &vlista, 10, 0, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  9, 0|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  8, 0|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista, 12, 0, key2, NULL, 0);
	st_svv(&st_r.g, &vlista, 11, 0|SVDUP, key2, NULL, 0);
	st_svv(&st_r.g, &vlista, 13, 0, key3, NULL, 0);
	st_svv(&st_r.g, &vlista, 14, 0, key4, NULL, 0);

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 15ULL, 0, 0);

	checkv(&st_r.r, &iter, 10, 0, key);
	ss_iteratornext(&iter);
	checkv(&st_r.r, &iter, 12, 0, key2);
	ss_iteratornext(&iter);
	checkv(&st_r.r, &iter, 13, 0, key3);
	ss_iteratornext(&iter);
	checkv(&st_r.r, &iter, 14, 0, key4);
	ss_iteratornext(&iter);
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_dup_lsn_lt0_chain(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int key = 7;
	int key2 = 8;
	int key3 = 9;
	int key4 = 10;

	st_svv(&st_r.g, &vlista, 10, 0, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  9, 0|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  8, 0|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista, 12, 0, key2, NULL, 0);
	st_svv(&st_r.g, &vlista, 11, 0|SVDUP, key2, NULL, 0);
	st_svv(&st_r.g, &vlista, 13, 0, key3, NULL, 0);
	st_svv(&st_r.g, &vlista, 14, 0, key4, NULL, 0);

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 11ULL, 0, 0);

	checkv(&st_r.r, &iter, 10, 0, key);
	ss_iteratornext(&iter);
	checkv(&st_r.r, &iter, 12, 0, key2);
	ss_iteratornext(&iter);
	checkv(&st_r.r, &iter, 11, 0|SVDUP, key2);
	ss_iteratornext(&iter);
	checkv(&st_r.r, &iter, 13, 0, key3);
	ss_iteratornext(&iter);
	checkv(&st_r.r, &iter, 14, 0, key4);
	ss_iteratornext(&iter);
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_dup_lsn_lt1_chain(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int key = 7;
	int key2 = 8;
	int key3 = 9;
	int key4 = 10;

	st_svv(&st_r.g, &vlista, 10, 0, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  9, 0|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  8, 0|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista, 12, 0, key2, NULL, 0);
	st_svv(&st_r.g, &vlista, 11, 0|SVDUP, key2, NULL, 0);
	st_svv(&st_r.g, &vlista, 13, 0, key3, NULL, 0);
	st_svv(&st_r.g, &vlista, 14, 0, key4, NULL, 0);

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 9ULL, 0, 0);

	checkv(&st_r.r, &iter, 10, 0, key);
	ss_iteratornext(&iter);
	checkv(&st_r.r, &iter,  9, 0|SVDUP, key);
	ss_iteratornext(&iter);
	checkv(&st_r.r, &iter, 12, 0, key2);
	ss_iteratornext(&iter);
	checkv(&st_r.r, &iter, 11, 0|SVDUP, key2);
	ss_iteratornext(&iter);
	checkv(&st_r.r, &iter, 13, 0, key3);
	ss_iteratornext(&iter);
	checkv(&st_r.r, &iter, 14, 0, key4);
	ss_iteratornext(&iter);
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_dup_lsn_lt2_chain(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int key = 7;
	int key2 = 8;
	int key3 = 9;
	int key4 = 10;

	st_svv(&st_r.g, &vlista, 10, 0, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  9, 0|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  8, 0|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista, 12, 0, key2, NULL, 0);
	st_svv(&st_r.g, &vlista, 11, 0|SVDUP, key2, NULL, 0);
	st_svv(&st_r.g, &vlista, 13, 0, key3, NULL, 0);
	st_svv(&st_r.g, &vlista, 14, 0, key4, NULL, 0);

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 3ULL, 0, 0);

	checkv(&st_r.r, &iter, 10, 0, key);
	ss_iteratornext(&iter);
	checkv(&st_r.r, &iter,  9, 0|SVDUP, key);
	ss_iteratornext(&iter);
	checkv(&st_r.r, &iter,  8, 0|SVDUP, key);
	ss_iteratornext(&iter);
	checkv(&st_r.r, &iter, 12, 0, key2);
	ss_iteratornext(&iter);
	checkv(&st_r.r, &iter, 11, 0|SVDUP, key2);
	ss_iteratornext(&iter);
	checkv(&st_r.r, &iter, 13, 0, key3);
	ss_iteratornext(&iter);
	checkv(&st_r.r, &iter, 14, 0, key4);
	ss_iteratornext(&iter);
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_dup_lsn_limit0(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int key = 7;

	st_svv(&st_r.g, &vlista, 10, 0, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  9, 0|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  8, 0|SVDUP, key, NULL, 0);

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 15ULL, 0, 0);

	checkv(&st_r.r, &iter, 10, 0, key);
	ss_iteratornext(&iter);
	t( ss_iteratorhas(&iter) == 0 );
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_dup_lsn_limit1(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int key = 7;

	st_svv(&st_r.g, &vlista, 10, 0, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  9, 0|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  8, 0|SVDUP, key, NULL, 0);

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 9ULL, 0, 0);

	checkv(&st_r.r, &iter, 10, 0, key);
	ss_iteratornext(&iter);
	checkv(&st_r.r, &iter,  9, 0|SVDUP, key);
	ss_iteratornext(&iter);
	t( ss_iteratorhas(&iter) == 0 );
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_dup_lsn_limit2(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int key = 7;
	st_svv(&st_r.g, &vlista, 10, 0, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  9, 0|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  8, 0|SVDUP, key, NULL, 0);

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 5ULL, 0, 0);

	checkv(&st_r.r, &iter, 10, 0, key);
	ss_iteratornext(&iter);
	checkv(&st_r.r, &iter,  9, 0|SVDUP, key);
	ss_iteratornext(&iter);
	checkv(&st_r.r, &iter,  8, 0|SVDUP, key);
	ss_iteratornext(&iter);
	t( ss_iteratorhas(&iter) == 0 );
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_dup_lsn_limit3(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int k = 0;
	int key = 7;
	st_svv(&st_r.g, &vlista, 412 - k, 0, key, NULL, 0);
	while (k < 411) {
		st_svv(&st_r.g, &vlista, 411 - k, 0|SVDUP, key, NULL, 0);
		k++;
	}
	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 2 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 500ULL, 0, 0);

	t(ss_iteratorhas(&iter) == 1);
	checkv(&st_r.r, &iter, 412, 0, key);
	ss_iteratornext(&iter);
	t(ss_iteratorhas(&iter) == 0);
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_dup_lsn_limit4(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int k = 0;
	int key = 7;
	st_svv(&st_r.g, &vlista, 412 - k, 0, key, NULL, 0);
	while (k < 411) {
		st_svv(&st_r.g, &vlista, 411 - k, 0|SVDUP, key, NULL, 0);
		k++;
	}
	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(k));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 0ULL, 0, 0);

	k = 0;
	while (ss_iteratorhas(&iter))
	{
		if (k == 0)
			checkv(&st_r.r, &iter, 412 - k, 0, key);
		else
			checkv(&st_r.r, &iter, 412 - k, 0|SVDUP, key);
		ss_iteratornext(&iter);
		k++;
	}
	t( k == 412 );
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_dup_lsn_limit5(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int k = 0;
	int key = 7;
	st_svv(&st_r.g, &vlista, 412 - k, 0, key, NULL, 0);
	while (k < 411) {
		st_svv(&st_r.g, &vlista, 411 - k, 0|SVDUP, key, NULL, 0);
		k++;
	}
	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 1 * (sizeof(svv) + sizeof(k));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 0ULL, 0, 0);

	k = 0;
	while (ss_iteratorhas(&iter))
	{
		if (k == 0)
			checkv(&st_r.r, &iter, 412 - k, 0, key);
		else
			checkv(&st_r.r, &iter, 412 - k, 0|SVDUP, key);
		ss_iteratornext(&iter);
		k++;
	}
	t( k == 412 );
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_delete0(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int key = 7;

	st_svv(&st_r.g, &vlista, 10, 0, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  9, SVDELETE|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  8, SVDELETE|SVDUP, key, NULL, 0);

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 10ULL, 0, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		char *v = ss_iteratorof(&iter);
		t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == key );
		t( sf_lsn(st_r.r.scheme, v) == 10 - i );
		if (i == 0)
			t( sf_flags(st_r.r.scheme, v) == 0 );
		else
			t( sf_flags(st_r.r.scheme, v) == (SVDELETE | SVDUP) );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 1 );
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_delete1(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int key = 7;

	st_svv(&st_r.g, &vlista, 10, 0, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  9, SVDELETE|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  8, SVDELETE|SVDUP, key, NULL, 0);

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 9ULL, 0, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		char *v = ss_iteratorof(&iter);
		t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == key );
		t( sf_lsn(st_r.r.scheme, v) == 10 - i );
		if (i == 0)
			t( sf_flags(st_r.r.scheme, v) == 0 );
		else
			t( sf_flags(st_r.r.scheme, v) == (SVDELETE | SVDUP) );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 2 );
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_delete2(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int key = 7;

	st_svv(&st_r.g, &vlista, 10, 0, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  9, SVDELETE|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  8, SVDELETE|SVDUP, key, NULL, 0);

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 8ULL, 0, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		char *v = ss_iteratorof(&iter);
		t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == key );
		t( sf_lsn(st_r.r.scheme, v) == 10 - i );
		if (i == 0)
			t( sf_flags(st_r.r.scheme, v) == 0 );
		else
			t( sf_flags(st_r.r.scheme, v) == (SVDELETE | SVDUP) );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 3 );
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_delete3(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int key = 7;

	st_svv(&st_r.g, &vlista, 10, SVDELETE, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  9, SVDELETE|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  8, SVDELETE|SVDUP, key, NULL, 0);

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 7ULL, 0, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		char *v = ss_iteratorof(&iter);
		t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == key );
		t( sf_lsn(st_r.r.scheme, v) == 10 - i );
		if (i == 0)
			t( sf_flags(st_r.r.scheme, v) == SVDELETE );
		else
			t( sf_flags(st_r.r.scheme, v) == (SVDELETE | SVDUP) );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 3 );
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_delete4(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int key = 7;

	st_svv(&st_r.g, &vlista, 10, SVDELETE, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  9, SVDELETE|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  8, SVDELETE|SVDUP, key, NULL, 0);

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 10ULL, 0, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		char *v = ss_iteratorof(&iter);
		t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == key );
		t( sf_lsn(st_r.r.scheme, v) == 10 - i );
		if (i == 0)
			t( sf_flags(st_r.r.scheme, v) == SVDELETE );
		else
			t( sf_flags(st_r.r.scheme, v) == (SVDELETE | SVDUP) );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 0 );
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_delete5(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int key = 7;

	st_svv(&st_r.g, &vlista, 10, SVDELETE, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  9, SVDELETE|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  8, SVDELETE|SVDUP, key, NULL, 0);

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 11ULL, 0, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		char *v = ss_iteratorof(&iter);
		t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == key );
		t( sf_lsn(st_r.r.scheme, v) == 10 - i );
		if (i == 0)
			t( sf_flags(st_r.r.scheme, v) == SVDELETE );
		else
			t( sf_flags(st_r.r.scheme, v) == (SVDELETE | SVDUP) );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 0 );
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_delete6(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int key = 6;
	st_svv(&st_r.g, &vlista, 12, 0, key, NULL, 0);
	key = 7;
	st_svv(&st_r.g, &vlista, 10, SVDELETE, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  9, SVDELETE|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  8, SVDELETE|SVDUP, key, NULL, 0);
	key = 10;
	st_svv(&st_r.g, &vlista, 11, 0, key, NULL, 0);

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 13ULL, 0, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		char *v = ss_iteratorof(&iter);
		if (i == 0) {
			t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == 6 );
			t( sf_lsn(st_r.r.scheme, v) == 12 );
		} else {
			t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == 10 );
			t( sf_lsn(st_r.r.scheme, v) == 11 );
		}
		t( sf_flags(st_r.r.scheme, v) == 0 );
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 2 );
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_delete7(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int key = 6;
	st_svv(&st_r.g, &vlista, 12, 0, key, NULL, 0);
	key = 7;
	st_svv(&st_r.g, &vlista, 10, SVDELETE, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  9, SVDELETE|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  8, SVDELETE|SVDUP, key, NULL, 0);
	key = 10;
	st_svv(&st_r.g, &vlista, 11, 0, key, NULL, 0);

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 10ULL, 0, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		char *v = ss_iteratorof(&iter);
		if (i == 0) {
			t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == 6 );
			t( sf_flags(st_r.r.scheme, v) == 0 );
			t( sf_lsn(st_r.r.scheme, v) == 12 );
		} else {
			t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == 10 );
			t( sf_flags(st_r.r.scheme, v) == 0 );
			t( sf_lsn(st_r.r.scheme, v) == 11 );
		}
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 2 );
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_delete8(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int key = 6;
	st_svv(&st_r.g, &vlista, 12, 0, key, NULL, 0);
	key = 7;
	st_svv(&st_r.g, &vlista, 10, SVDELETE, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  9, SVDELETE|SVDUP, key, NULL, 0);
	st_svv(&st_r.g, &vlista,  8, SVDELETE|SVDUP, key, NULL, 0);
	key = 10;
	st_svv(&st_r.g, &vlista, 11, 0, key, NULL, 0);

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = 10 * (sizeof(svv) + sizeof(key));
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 9ULL, 0, 0);

	int i = 0;
	i = 0;
	while (ss_iteratorhas(&iter)) {
		char *v = ss_iteratorof(&iter);
		if (i == 0) {
			t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == 6 );
			t( sf_flags(st_r.r.scheme, v) == 0 );
			t( sf_lsn(st_r.r.scheme, v) == 12 );
		} else
		if (i == 1) {
			t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == 7 );
			t( sf_flags(st_r.r.scheme, v) == SVDELETE );
			t( sf_lsn(st_r.r.scheme, v) == 10 );
		} else
		if (i == 2) {
			t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == 7 );
			t( sf_flags(st_r.r.scheme, v) == (SVDELETE|SVDUP) );
			t( sf_lsn(st_r.r.scheme, v) ==  9 );
		} else {
			t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == 10 );
			t( sf_flags(st_r.r.scheme, v) == 0 );
			t( sf_lsn(st_r.r.scheme, v) == 11 );
		}
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 4 );
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_duprange0(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int key = 7;
	int lsn = 1;
	int i = 0;
	while (i < 100) {
		st_svv(&st_r.g, &vlista, 100 + lsn, 0, key, NULL, 0);
		st_svv(&st_r.g, &vlista, lsn, 0|SVDUP, key, NULL, 0);
		lsn++;
		i++;
	}

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	svupsert u;
	sv_upsertinit(&u);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = UINT64_MAX;
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 100ULL, 0, 0);

	i = 0;
	while (ss_iteratorhas(&iter)) {
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 200 );
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

static void
sv_writeiter_duprange1(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SVVRAW);
	st_listinit(&vlistb, ST_SVVRAW);

	int key = 7;
	int lsn = 1;
	int i = 0;
	while (i < 100) {
		st_svv(&st_r.g, &vlista, 100 + lsn, 0, key, NULL, 0);
		st_svv(&st_r.g, &vlista, lsn, 0|SVDUP, key, NULL, 0);
		lsn++;
		i++;
	}

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(svv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(svv*));

	svupsert u;
	sv_upsertinit(&u);

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 2);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);
	ssiter iter;
	ss_iterinit(sv_writeiter, &iter);
	uint64_t limit = UINT64_MAX;
	ss_iteropen(sv_writeiter, &iter, &st_r.r, &merge, &u, limit, sizeof(svv), 0, 0, 100ULL + lsn, 0, 0);

	i = 0;
	while (ss_iteratorhas(&iter)) {
		ss_iteratornext(&iter);
		i++;
	}
	t( i == 100 );
	ss_iteratorclose(&iter);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	sv_upsertfree(&u, &st_r.r);
}

stgroup *sv_writeiter_group(void)
{
	stgroup *group = st_group("svwriteiter");
	st_groupadd(group, st_test("iter", sv_writeiter_iter));
	st_groupadd(group, st_test("iter_limit", sv_writeiter_limit));
	st_groupadd(group, st_test("iter_limit_small", sv_writeiter_limit_small));
	st_groupadd(group, st_test("iter_dup_lsn_gt", sv_writeiter_dup_lsn_gt));
	st_groupadd(group, st_test("iter_dup_lsn_lt0", sv_writeiter_dup_lsn_lt0));
	st_groupadd(group, st_test("iter_dup_lsn_lt1", sv_writeiter_dup_lsn_lt1));
	st_groupadd(group, st_test("iter_dup_lsn_lt2", sv_writeiter_dup_lsn_lt2));
	st_groupadd(group, st_test("iter_dup_lsn_gt_chain", sv_writeiter_dup_lsn_gt_chain));
	st_groupadd(group, st_test("iter_dup_lsn_lt0_chain", sv_writeiter_dup_lsn_lt0_chain));
	st_groupadd(group, st_test("iter_dup_lsn_lt1_chain", sv_writeiter_dup_lsn_lt1_chain));
	st_groupadd(group, st_test("iter_dup_lsn_lt2_chain", sv_writeiter_dup_lsn_lt2_chain));
	st_groupadd(group, st_test("iter_dup_lsn_limit0", sv_writeiter_dup_lsn_limit0));
	st_groupadd(group, st_test("iter_dup_lsn_limit1", sv_writeiter_dup_lsn_limit1));
	st_groupadd(group, st_test("iter_dup_lsn_limit2", sv_writeiter_dup_lsn_limit2));
	st_groupadd(group, st_test("iter_dup_lsn_limit3", sv_writeiter_dup_lsn_limit3));
	st_groupadd(group, st_test("iter_dup_lsn_limit4", sv_writeiter_dup_lsn_limit4));
	st_groupadd(group, st_test("iter_dup_lsn_limit5", sv_writeiter_dup_lsn_limit5));
	st_groupadd(group, st_test("iter_delete0", sv_writeiter_delete0));
	st_groupadd(group, st_test("iter_delete1", sv_writeiter_delete1));
	st_groupadd(group, st_test("iter_delete2", sv_writeiter_delete2));
	st_groupadd(group, st_test("iter_delete3", sv_writeiter_delete3));
	st_groupadd(group, st_test("iter_delete4", sv_writeiter_delete4));
	st_groupadd(group, st_test("iter_delete5", sv_writeiter_delete5));
	st_groupadd(group, st_test("iter_delete6", sv_writeiter_delete6));
	st_groupadd(group, st_test("iter_delete7", sv_writeiter_delete7));
	st_groupadd(group, st_test("iter_delete8", sv_writeiter_delete8));
	st_groupadd(group, st_test("iter_duprange0", sv_writeiter_duprange0));
	st_groupadd(group, st_test("iter_duprange1", sv_writeiter_duprange1));
	return group;
}
