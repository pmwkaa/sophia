
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
allocv(sr *r, uint64_t lsn, uint8_t flags, void *key)
{
	srfmtv pv;
	pv.key = (char*)key;
	pv.r.size = sizeof(uint32_t);
	pv.r.offset = 0;
	svv *v = sv_vbuild(r, &pv, 1, NULL, 0);
	v->lsn = lsn;
	v->flags = flags;
	sv *vv = sr_malloc(r->a, sizeof(sv));
	sv_init(vv, &sv_vif, v, NULL);
	return vv;
}

static void
svmergeiter_merge_a(stc *cx srunused)
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
	sr_init(&r, &error, &a, NULL, SR_FKV, SR_FS_RAW, &cmp, NULL, NULL, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);
	int i = 0;
	while (i < 10)
	{
		sv *v = allocv(&r, i, 0, &i);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	sriter ita;
	sr_iterinit(sr_bufiterref, &ita, &r);
	sr_iteropen(sr_bufiterref, &ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(sr_bufiterref, &itb, &r);
	sr_iteropen(sr_bufiterref, &itb, &vlistb, sizeof(sv*));

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
	sr_iterinit(sv_mergeiter, &merge, &r);
	sr_iteropen(sv_mergeiter, &merge, &m, SR_GTE);

	i = 0;
	while (sr_iteratorhas(&merge)) {
		sv *v = (sv*)sr_iteratorof(&merge);
		t( *(int*)sv_key(v, &r, 0) == i );
		t( sv_lsn(v) == i );
		t( sv_flags(v) == 0 );
		sr_iteratornext(&merge);
		i++;
	}
	t( i == 10 );
	sr_iteratorclose(&merge);

	sr_iterinit(sr_bufiterref, &ita, NULL);
	sr_iteropen(sr_bufiterref, &ita, &vlista, sizeof(sv*));
	while (sr_iteratorhas(&ita)) {
		sv *v = (sv*)sr_iteratorof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iteratornext(&ita);
	}
	sr_buffree(&vlista, &a);
	sv_mergefree(&m, &a);
	sr_keyfree(&cmp, &a);
}

static void
svmergeiter_merge_b(stc *cx srunused)
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
	sr_init(&r, &error, &a, NULL, SR_FKV, SR_FS_RAW, &cmp, NULL, NULL, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);
	int i = 0;
	while (i < 10)
	{
		sv *v = allocv(&r, i, 0, &i);
		t(sr_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	sriter ita;
	sr_iterinit(sr_bufiterref, &ita, &r);
	sr_iteropen(sr_bufiterref, &ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(sr_bufiterref, &itb, &r);
	sr_iteropen(sr_bufiterref, &itb, &vlistb, sizeof(sv*));

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
	sr_iterinit(sv_mergeiter, &merge, &r);
	sr_iteropen(sv_mergeiter, &merge, &m, SR_GTE);

	i = 0;
	while (sr_iteratorhas(&merge)) {
		sv *v = (sv*)sr_iteratorof(&merge);
		t( *(int*)sv_key(v, &r, 0) == i );
		t( sv_lsn(v) == i );
		t( sv_flags(v) == 0 );
		sr_iteratornext(&merge);
		i++;
	}
	t( i == 10 );
	sr_iteratorclose(&merge);

	sr_iterinit(sr_bufiterref, &itb, NULL);
	sr_iteropen(sr_bufiterref, &itb, &vlistb, sizeof(sv*));
	while (sr_iteratorhas(&itb)) {
		sv *v = (sv*)sr_iteratorof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iteratornext(&itb);
	}
	sr_buffree(&vlistb, &a);
	sv_mergefree(&m, &a);
	sr_keyfree(&cmp, &a);
}

static void
svmergeiter_merge_ab(stc *cx srunused)
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
	sr_init(&r, &error, &a, NULL, SR_FKV, SR_FS_RAW, &cmp, NULL, NULL, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);
	int i = 0;
	while (i < 5)
	{
		sv *v = allocv(&r, i, 0, &i);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	while (i < 10)
	{
		sv *v = allocv(&r, i, 0, &i);
		t(sr_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
		i++;
	}

	sriter ita;
	sr_iterinit(sr_bufiterref, &ita, &r);
	sr_iteropen(sr_bufiterref, &ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(sr_bufiterref, &itb, &r);
	sr_iteropen(sr_bufiterref, &itb, &vlistb, sizeof(sv*));

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
	sr_iterinit(sv_mergeiter, &merge, &r);
	sr_iteropen(sv_mergeiter, &merge, &m, SR_GTE);

	i = 0;
	while (sr_iteratorhas(&merge)) {
		sv *v = (sv*)sr_iteratorof(&merge);
		t( *(int*)sv_key(v, &r, 0) == i );
		t( sv_lsn(v) == i );
		t( sv_flags(v) == 0 );
		sr_iteratornext(&merge);
		i++;
	}
	t( i == 10 );
	sr_iteratorclose(&merge);

	sr_iterinit(sr_bufiterref, &ita, NULL);
	sr_iteropen(sr_bufiterref, &ita, &vlista, sizeof(sv*));
	while (sr_iteratorhas(&ita)) {
		sv *v = (sv*)sr_iteratorof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iteratornext(&ita);
	}
	sr_iterinit(sr_bufiterref, &itb, NULL);
	sr_iteropen(sr_bufiterref, &itb, &vlistb, sizeof(sv*));
	while (sr_iteratorhas(&itb)) {
		sv *v = (sv*)sr_iteratorof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iteratornext(&itb);
	}
	sr_buffree(&vlista, &a);
	sr_buffree(&vlistb, &a);
	sv_mergefree(&m, &a);
	sr_keyfree(&cmp, &a);
}

static void
svmergeiter_merge_abc(stc *cx srunused)
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
	sr_init(&r, &error, &a, NULL, SR_FKV, SR_FS_RAW, &cmp, NULL, NULL, NULL);

	srbuf vlista;
	srbuf vlistb;
	srbuf vlistc;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);
	sr_bufinit(&vlistc);
	int i = 0;
	while (i < 5)
	{
		sv *v = allocv(&r, i, 0, &i);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	while (i < 10)
	{
		sv *v = allocv(&r, i, 0, &i);
		t(sr_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	while (i < 15)
	{
		sv *v = allocv(&r, i, 0, &i);
		t(sr_bufadd(&vlistc, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	sriter ita;
	sr_iterinit(sr_bufiterref, &ita, &r);
	sr_iteropen(sr_bufiterref, &ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(sr_bufiterref, &itb, &r);
	sr_iteropen(sr_bufiterref, &itb, &vlistb, sizeof(sv*));
	sriter itc;
	sr_iterinit(sr_bufiterref, &itc, &r);
	sr_iteropen(sr_bufiterref, &itc, &vlistc, sizeof(sv*));

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
	sr_iterinit(sv_mergeiter, &merge, &r);
	sr_iteropen(sv_mergeiter, &merge, &m, SR_GTE);

	i = 0;
	while (sr_iteratorhas(&merge)) {
		sv *v = (sv*)sr_iteratorof(&merge);
		t( *(int*)sv_key(v, &r, 0) == i );
		t( sv_lsn(v) == i );
		t( sv_flags(v) == 0 );
		sr_iteratornext(&merge);
		i++;
	}
	t( i == 15 );
	sr_iteratorclose(&merge);

	sr_iterinit(sr_bufiterref, &ita, NULL);
	sr_iteropen(sr_bufiterref, &ita, &vlista, sizeof(sv*));
	while (sr_iteratorhas(&ita)) {
		sv *v = (sv*)sr_iteratorof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iteratornext(&ita);
	}
	sr_iterinit(sr_bufiterref, &itb, NULL);
	sr_iteropen(sr_bufiterref, &itb, &vlistb, sizeof(sv*));
	while (sr_iteratorhas(&itb)) {
		sv *v = (sv*)sr_iteratorof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iteratornext(&itb);
	}
	sr_iterinit(sr_bufiterref, &itc, NULL);
	sr_iteropen(sr_bufiterref, &itc, &vlistc, sizeof(sv*));
	while (sr_iteratorhas(&itc)) {
		sv *v = (sv*)sr_iteratorof(&itc);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iteratornext(&itc);
	}
	sr_buffree(&vlista, &a);
	sr_buffree(&vlistb, &a);
	sr_buffree(&vlistc, &a);
	sv_mergefree(&m, &a);
	sr_keyfree(&cmp, &a);
}

static void
svmergeiter_merge_ba(stc *cx srunused)
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
	sr_init(&r, &error, &a, NULL, SR_FKV, SR_FS_RAW, &cmp, NULL, NULL, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);
	int i = 0;
	while (i < 5)
	{
		sv *v = allocv(&r, i, 0, &i);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	while (i < 10)
	{
		sv *v = allocv(&r, i, 0, &i);
		t(sr_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
		i++;
	}

	sriter ita;
	sr_iterinit(sr_bufiterref, &ita, &r);
	sr_iteropen(sr_bufiterref, &ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(sr_bufiterref, &itb, &r);
	sr_iteropen(sr_bufiterref, &itb, &vlistb, sizeof(sv*));

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
	sr_iterinit(sv_mergeiter, &merge, &r);
	sr_iteropen(sv_mergeiter, &merge, &m, SR_GTE);

	i = 0;
	while (sr_iteratorhas(&merge)) {
		sv *v = (sv*)sr_iteratorof(&merge);
		t( *(int*)sv_key(v, &r, 0) == i );
		t( sv_lsn(v) == i );
		t( sv_flags(v) == 0 );
		sr_iteratornext(&merge);
		i++;
	}
	t( i == 10 );
	sr_iteratorclose(&merge);

	sr_iterinit(sr_bufiterref, &ita, NULL);
	sr_iteropen(sr_bufiterref, &ita, &vlista, sizeof(sv*));
	while (sr_iteratorhas(&ita)) {
		sv *v = (sv*)sr_iteratorof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iteratornext(&ita);
	}
	sr_iterinit(sr_bufiterref, &itb, NULL);
	sr_iteropen(sr_bufiterref, &itb, &vlistb, sizeof(sv*));
	while (sr_iteratorhas(&itb)) {
		sv *v = (sv*)sr_iteratorof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iteratornext(&itb);
	}
	sr_buffree(&vlista, &a);
	sr_buffree(&vlistb, &a);
	sv_mergefree(&m, &a);
	sr_keyfree(&cmp, &a);
}

static void
svmergeiter_merge_dup_ab(stc *cx srunused)
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
	sr_init(&r, &error, &a, NULL, SR_FKV, SR_FS_RAW, &cmp, NULL, NULL, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);
	int i = 0;
	int lsn = 10;
	while (i < 5)
	{
		sv *v = allocv(&r, lsn, 0, &i);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
	}
	i = 0 ;
	while (i < 5)
	{
		sv *v = allocv(&r, lsn, 0, &i);
		t(sr_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
	}

	sriter ita;
	sr_iterinit(sr_bufiterref, &ita, &r);
	sr_iteropen(sr_bufiterref, &ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(sr_bufiterref, &itb, &r);
	sr_iteropen(sr_bufiterref, &itb, &vlistb, sizeof(sv*));

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
	sr_iterinit(sv_mergeiter, &merge, &r);
	sr_iteropen(sv_mergeiter, &merge, &m, SR_GTE);

	int key = 0;
	i = 0;
	while (sr_iteratorhas(&merge)) {
		sv *v = (sv*)sr_iteratorof(&merge);
		if ((i % 2) == 0) {
			t( *(int*)sv_key(v, &r, 0) == key );
			t( sv_flags(v) == 0 );
			key++;
		} else {
			t( *(int*)sv_key(v, &r, 0) == key - 1);
			t( (sv_flags(v) | sv_mergeisdup(&merge)) == (0|SVDUP) );
		}
		sr_iteratornext(&merge);
		i++;
	}
	t( i == 10 );
	sr_iteratorclose(&merge);

	sr_iterinit(sr_bufiterref, &ita, NULL);
	sr_iteropen(sr_bufiterref, &ita, &vlista, sizeof(sv*));
	while (sr_iteratorhas(&ita)) {
		sv *v = (sv*)sr_iteratorof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iteratornext(&ita);
	}
	sr_iterinit(sr_bufiterref, &itb, NULL);
	sr_iteropen(sr_bufiterref, &itb, &vlistb, sizeof(sv*));
	while (sr_iteratorhas(&itb)) {
		sv *v = (sv*)sr_iteratorof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iteratornext(&itb);
	}
	sr_buffree(&vlista, &a);
	sr_buffree(&vlistb, &a);
	sv_mergefree(&m, &a);
	sr_keyfree(&cmp, &a);
}

static void
svmergeiter_merge_dup_a_chain(stc *cx srunused)
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
	sr_init(&r, &error, &a, NULL, SR_FKV, SR_FS_RAW, &cmp, NULL, NULL, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);
	int key = 7;
	int lsn = 5;
	int i = 0;
	while (i < 5)
	{
		sv *v = allocv(&r, lsn, 0 | ((i > 0) ? SVDUP: 0), &key);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
	}

	sriter ita;
	sr_iterinit(sr_bufiterref, &ita, &r);
	sr_iteropen(sr_bufiterref, &ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(sr_bufiterref, &itb, &r);
	sr_iteropen(sr_bufiterref, &itb, &vlistb, sizeof(sv*));

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
	sr_iterinit(sv_mergeiter, &merge, &r);
	sr_iteropen(sv_mergeiter, &merge, &m, SR_GTE);

	i = 0;
	while (sr_iteratorhas(&merge)) {
		sv *v = (sv*)sr_iteratorof(&merge);
		t( *(int*)sv_key(v, &r, 0) == key );
		if (i == 0) {
			t( sv_flags(v) == 0 );
		} else {
			t( (sv_flags(v) | sv_mergeisdup(&merge)) == (0|SVDUP) );
		}
		sr_iteratornext(&merge);
		i++;
	}
	t( i == 5 );
	sr_iteratorclose(&merge);

	sr_iterinit(sr_bufiterref, &ita, NULL);
	sr_iteropen(sr_bufiterref, &ita, &vlista, sizeof(sv*));
	while (sr_iteratorhas(&ita)) {
		sv *v = (sv*)sr_iteratorof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iteratornext(&ita);
	}
	sr_iterinit(sr_bufiterref, &itb, NULL);
	sr_iteropen(sr_bufiterref, &itb, &vlistb, sizeof(sv*));
	while (sr_iteratorhas(&itb)) {
		sv *v = (sv*)sr_iteratorof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iteratornext(&itb);
	}
	sr_buffree(&vlista, &a);
	sr_buffree(&vlistb, &a);
	sv_mergefree(&m, &a);
	sr_keyfree(&cmp, &a);
}

static void
svmergeiter_merge_dup_ab_chain(stc *cx srunused)
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
	sr_init(&r, &error, &a, NULL, SR_FKV, SR_FS_RAW, &cmp, NULL, NULL, NULL);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);
	int lsn = 10;
	int key = 7;
	int i = 0;
	while (i < 5)
	{
		sv *v = allocv(&r, lsn, 0 | ((i > 0) ? SVDUP: 0), &key);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
	}
	i = 0 ;
	while (i < 5)
	{
		sv *v = allocv(&r, lsn, 0 | ((i > 0) ? SVDUP: 0), &key);
		t(sr_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
	}

	sriter ita;
	sr_iterinit(sr_bufiterref, &ita, &r);
	sr_iteropen(sr_bufiterref, &ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(sr_bufiterref, &itb, &r);
	sr_iteropen(sr_bufiterref, &itb, &vlistb, sizeof(sv*));

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
	sr_iterinit(sv_mergeiter, &merge, &r);
	sr_iteropen(sv_mergeiter, &merge, &m, SR_GTE);

	i = 0;
	while (sr_iteratorhas(&merge)) {
		sv *v = (sv*)sr_iteratorof(&merge);
		t( *(int*)sv_key(v, &r, 0) == key );
		if (i == 0) {
			t( sv_flags(v) == 0 );
		} else {
			t( (sv_flags(v) | sv_mergeisdup(&merge)) == (0|SVDUP) );
		}
		sr_iteratornext(&merge);
		i++;
	}
	t( i == 10 );
	sr_iteratorclose(&merge);

	sr_iterinit(sr_bufiterref, &ita, NULL);
	sr_iteropen(sr_bufiterref, &ita, &vlista, sizeof(sv*));
	while (sr_iteratorhas(&ita)) {
		sv *v = (sv*)sr_iteratorof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iteratornext(&ita);
	}
	sr_iterinit(sr_bufiterref, &itb, NULL);
	sr_iteropen(sr_bufiterref, &itb, &vlistb, sizeof(sv*));
	while (sr_iteratorhas(&itb)) {
		sv *v = (sv*)sr_iteratorof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iteratornext(&itb);
	}
	sr_buffree(&vlista, &a);
	sr_buffree(&vlistb, &a);
	sv_mergefree(&m, &a);
	sr_keyfree(&cmp, &a);
}

static void
svmergeiter_merge_dup_abc_chain(stc *cx srunused)
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
	sr_init(&r, &error, &a, NULL, SR_FKV, SR_FS_RAW, &cmp, NULL, NULL, NULL);

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
		sv *v = allocv(&r, lsn, 0 | ((i > 0) ? SVDUP: 0), &key);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
	}
	i = 0 ;
	while (i < 5)
	{
		sv *v = allocv(&r, lsn, 0 | ((i > 0) ? SVDUP: 0), &key);
		t(sr_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
	}
	i = 0 ;
	while (i < 5)
	{
		sv *v = allocv(&r, lsn, 0 | ((i > 0) ? SVDUP: 0), &key);
		t(sr_bufadd(&vlistc, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
	}

	sriter ita;
	sr_iterinit(sr_bufiterref, &ita, &r);
	sr_iteropen(sr_bufiterref, &ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(sr_bufiterref, &itb, &r);
	sr_iteropen(sr_bufiterref, &itb, &vlistb, sizeof(sv*));
	sriter itc;
	sr_iterinit(sr_bufiterref, &itc, &r);
	sr_iteropen(sr_bufiterref, &itc, &vlistc, sizeof(sv*));

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
	sr_iterinit(sv_mergeiter, &merge, &r);
	sr_iteropen(sv_mergeiter, &merge, &m, SR_GTE);

	i = 0;
	while (sr_iteratorhas(&merge)) {
		sv *v = (sv*)sr_iteratorof(&merge);
		t( *(int*)sv_key(v, &r, 0) == key );
		if (i == 0) {
			t( sv_flags(v) == 0 );
		} else {
			t( (sv_flags(v) | sv_mergeisdup(&merge)) == (0|SVDUP) );
		}
		sr_iteratornext(&merge);
		i++;
	}
	t( i == 15 );
	sr_iteratorclose(&merge);

	sr_iterinit(sr_bufiterref, &ita, NULL);
	sr_iteropen(sr_bufiterref, &ita, &vlista, sizeof(sv*));
	while (sr_iteratorhas(&ita)) {
		sv *v = (sv*)sr_iteratorof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iteratornext(&ita);
	}
	sr_iterinit(sr_bufiterref, &itb, NULL);
	sr_iteropen(sr_bufiterref, &itb, &vlistb, sizeof(sv*));
	while (sr_iteratorhas(&itb)) {
		sv *v = (sv*)sr_iteratorof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iteratornext(&itb);
	}
	sr_iterinit(sr_bufiterref, &itc, NULL);
	sr_iteropen(sr_bufiterref, &itc, &vlistc, sizeof(sv*));
	while (sr_iteratorhas(&itc)) {
		sv *v = (sv*)sr_iteratorof(&itc);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iteratornext(&itc);
	}
	sr_buffree(&vlista, &a);
	sr_buffree(&vlistb, &a);
	sr_buffree(&vlistc, &a);
	sv_mergefree(&m, &a);
	sr_keyfree(&cmp, &a);
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
