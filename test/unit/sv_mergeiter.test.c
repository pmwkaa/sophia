
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
#include <libst.h>

static void
sv_mergeiter_merge_a(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SV);
	st_listinit(&vlistb, ST_SV);
	int i = 0;
	while (i < 10)
	{
		st_sv(&st_r.g, &vlista, i, 0, i, NULL, 0);
		i++;
	}
	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(sv*));

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

	i = 0;
	while (ss_iteratorhas(&merge)) {
		sv *v = (sv*)ss_iteratorof(&merge);
		t( *(int*)sv_field(v, &st_r.r, 0, NULL) == i );
		t( sv_lsn(v, &st_r.r) == i );
		t( sv_flags(v) == 0 );
		ss_iteratornext(&merge);
		i++;
	}
	t( i == 10 );
	ss_iteratorclose(&merge);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
}

static void
sv_mergeiter_merge_b(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SV);
	st_listinit(&vlistb, ST_SV);
	int i = 0;
	while (i < 10)
	{
		st_sv(&st_r.g, &vlistb, i, 0, i, NULL, 0);
		i++;
	}
	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(sv*));

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

	i = 0;
	while (ss_iteratorhas(&merge)) {
		sv *v = (sv*)ss_iteratorof(&merge);
		t( *(int*)sv_field(v, &st_r.r, 0, NULL) == i );
		t( sv_lsn(v, &st_r.r) == i );
		t( sv_flags(v) == 0 );
		ss_iteratornext(&merge);
		i++;
	}
	t( i == 10 );
	ss_iteratorclose(&merge);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
}

static void
sv_mergeiter_merge_ab(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SV);
	st_listinit(&vlistb, ST_SV);
	int i = 0;
	while (i < 5)
	{
		st_sv(&st_r.g, &vlista, i, 0, i, NULL, 0);
		i++;
	}
	while (i < 10)
	{
		st_sv(&st_r.g, &vlistb, i, 0, i, NULL, 0);
		i++;
	}

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(sv*));

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

	i = 0;
	while (ss_iteratorhas(&merge)) {
		sv *v = (sv*)ss_iteratorof(&merge);
		t( *(int*)sv_field(v, &st_r.r, 0, NULL) == i );
		t( sv_lsn(v, &st_r.r) == i );
		t( sv_flags(v) == 0 );
		ss_iteratornext(&merge);
		i++;
	}
	t( i == 10 );
	ss_iteratorclose(&merge);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
}

static void
sv_mergeiter_merge_abc(void)
{
	stlist vlista;
	stlist vlistb;
	stlist vlistc;
	st_listinit(&vlista, ST_SV);
	st_listinit(&vlistb, ST_SV);
	st_listinit(&vlistc, ST_SV);
	int i = 0;
	while (i < 5)
	{
		st_sv(&st_r.g, &vlista, i, 0, i, NULL, 0);
		i++;
	}
	while (i < 10)
	{
		st_sv(&st_r.g, &vlistb, i, 0, i, NULL, 0);
		i++;
	}
	while (i < 15)
	{
		st_sv(&st_r.g, &vlistc, i, 0, i, NULL, 0);
		i++;
	}

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(sv*));
	ssiter itc;
	ss_iterinit(ss_bufiterref, &itc);
	ss_iteropen(ss_bufiterref, &itc, &vlistc.list, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 3);
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
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	i = 0;
	while (ss_iteratorhas(&merge)) {
		sv *v = (sv*)ss_iteratorof(&merge);
		t( *(int*)sv_field(v, &st_r.r, 0, NULL) == i );
		t( sv_lsn(v, &st_r.r) == i );
		t( sv_flags(v) == 0 );
		ss_iteratornext(&merge);
		i++;
	}
	t( i == 15 );
	ss_iteratorclose(&merge);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	st_listfree(&vlistc, &st_r.r);

	sv_mergefree(&m, &st_r.a);
}

static void
sv_mergeiter_merge_ba(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SV);
	st_listinit(&vlistb, ST_SV);
	int i = 0;
	while (i < 5)
	{
		st_sv(&st_r.g, &vlista, i, 0, i, NULL, 0);
		i++;
	}
	while (i < 10)
	{
		st_sv(&st_r.g, &vlistb, i, 0, i, NULL, 0);
		i++;
	}

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 3);
	svmergesrc *s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = ita;
	s = sv_mergeadd(&m, NULL);
	t(s != NULL);
	s->src = itb;
	ssiter merge;
	ss_iterinit(sv_mergeiter, &merge);
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	i = 0;
	while (ss_iteratorhas(&merge)) {
		sv *v = (sv*)ss_iteratorof(&merge);
		t( *(int*)sv_field(v, &st_r.r, 0, NULL) == i );
		t( sv_lsn(v, &st_r.r) == i );
		t( sv_flags(v) == 0 );
		ss_iteratornext(&merge);
		i++;
	}
	t( i == 10 );
	ss_iteratorclose(&merge);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
}

static void
sv_mergeiter_merge_dup_ab(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SV);
	st_listinit(&vlistb, ST_SV);
	int i = 0;
	int lsn = 10;
	while (i < 5)
	{
		st_sv(&st_r.g, &vlista, lsn, 0, i, NULL, 0);
		i++;
		lsn--;
	}
	i = 0;
	while (i < 5)
	{
		st_sv(&st_r.g, &vlistb, lsn, 0, i, NULL, 0);
		i++;
		lsn--;
	}

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(sv*));

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

	int key = 0;
	i = 0;
	while (ss_iteratorhas(&merge)) {
		sv *v = (sv*)ss_iteratorof(&merge);
		if ((i % 2) == 0) {
			t( *(int*)sv_field(v, &st_r.r, 0, NULL) == key );
			t( sv_flags(v) == 0 );
			key++;
		} else {
			t( *(int*)sv_field(v, &st_r.r, 0, NULL) == key - 1);
			t( (sv_flags(v) | sv_mergeisdup(&merge)) == (0|SVDUP) );
		}
		ss_iteratornext(&merge);
		i++;
	}
	t( i == 10 );
	ss_iteratorclose(&merge);

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
}

static void
sv_mergeiter_merge_dup_a_chain(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SV);
	st_listinit(&vlistb, ST_SV);
	int key = 7;
	int i = 0;
	int lsn = 5;
	while (i < 5)
	{
		st_sv(&st_r.g, &vlista, lsn, 0 | ((i > 0) ? SVDUP: 0), key, NULL, 0);
		i++;
		lsn--;
	}

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(sv*));

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

	i = 0;
	while (ss_iteratorhas(&merge)) {
		sv *v = (sv*)ss_iteratorof(&merge);
		t( *(int*)sv_field(v, &st_r.r, 0, NULL) == key );
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

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
}

static void
sv_mergeiter_merge_dup_ab_chain(void)
{
	stlist vlista;
	stlist vlistb;
	st_listinit(&vlista, ST_SV);
	st_listinit(&vlistb, ST_SV);
	int key = 7;
	int i = 0;
	int lsn = 10;
	while (i < 5)
	{
		st_sv(&st_r.g, &vlista, lsn, 0 | ((i > 0) ? SVDUP: 0), key, NULL, 0);
		i++;
		lsn--;
	}
	i = 0;
	while (i < 5)
	{
		st_sv(&st_r.g, &vlistb, lsn, 0 | ((i > 0) ? SVDUP: 0), key, NULL, 0);
		i++;
		lsn--;
	}

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(sv*));

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

	i = 0;
	while (ss_iteratorhas(&merge)) {
		sv *v = (sv*)ss_iteratorof(&merge);
		t( *(int*)sv_field(v, &st_r.r, 0, NULL) == key );
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

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
}

static void
sv_mergeiter_merge_dup_abc_chain(void)
{
	stlist vlista;
	stlist vlistb;
	stlist vlistc;
	st_listinit(&vlista, ST_SV);
	st_listinit(&vlistb, ST_SV);
	st_listinit(&vlistc, ST_SV);
	int key = 7;
	int i = 0;
	int lsn = 10;
	while (i < 5)
	{
		st_sv(&st_r.g, &vlista, lsn, 0 | ((i > 0) ? SVDUP: 0), key, NULL, 0);
		i++;
		lsn--;
	}
	i = 0;
	while (i < 5)
	{
		st_sv(&st_r.g, &vlistb, lsn, 0 | ((i > 0) ? SVDUP: 0), key, NULL, 0);
		i++;
		lsn--;
	}
	i = 0;
	while (i < 5)
	{
		st_sv(&st_r.g, &vlistc, lsn, 0 | ((i > 0) ? SVDUP: 0), key, NULL, 0);
		i++;
		lsn--;
	}

	ssiter ita;
	ss_iterinit(ss_bufiterref, &ita);
	ss_iteropen(ss_bufiterref, &ita, &vlista.list, sizeof(sv*));
	ssiter itb;
	ss_iterinit(ss_bufiterref, &itb);
	ss_iteropen(ss_bufiterref, &itb, &vlistb.list, sizeof(sv*));
	ssiter itc;
	ss_iterinit(ss_bufiterref, &itc);
	ss_iteropen(ss_bufiterref, &itc, &vlistc.list, sizeof(sv*));

	svmerge m;
	sv_mergeinit(&m);
	sv_mergeprepare(&m, &st_r.r, 3);
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
	ss_iteropen(sv_mergeiter, &merge, &st_r.r, &m, SS_GTE);

	i = 0;
	while (ss_iteratorhas(&merge)) {
		sv *v = (sv*)ss_iteratorof(&merge);
		t( *(int*)sv_field(v, &st_r.r, 0, NULL) == key );
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

	sv_mergefree(&m, &st_r.a);

	st_listfree(&vlista, &st_r.r);
	st_listfree(&vlistb, &st_r.r);
	st_listfree(&vlistc, &st_r.r);
}

stgroup *sv_mergeiter_group(void)
{
	stgroup *group = st_group("svmergeiter");
	st_groupadd(group, st_test("merge_a", sv_mergeiter_merge_a));
	st_groupadd(group, st_test("merge_b", sv_mergeiter_merge_b));
	st_groupadd(group, st_test("merge_ab", sv_mergeiter_merge_ab));
	st_groupadd(group, st_test("merge_abc", sv_mergeiter_merge_abc));
	st_groupadd(group, st_test("merge_ba", sv_mergeiter_merge_ba));
	st_groupadd(group, st_test("merge_dup_ab", sv_mergeiter_merge_dup_ab));
	st_groupadd(group, st_test("merge_dup_a_chain", sv_mergeiter_merge_dup_a_chain));
	st_groupadd(group, st_test("merge_dup_ab_chain", sv_mergeiter_merge_dup_ab_chain));
	st_groupadd(group, st_test("merge_dup_abc_chain", sv_mergeiter_merge_dup_abc_chain));
	return group;
}
