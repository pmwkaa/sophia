
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <sophia.h>
#include "test.h"

static char *dbrep = "./rep";

static inline int
cmp(char *a, size_t asz, char *b, size_t bsz, void *arg) {
	register uint32_t av = *(uint32_t*)a;
	register uint32_t bv = *(uint32_t*)b;
	if (av == bv)
		return 0;
	return (av > bv) ? 1 : -1;
}

static void
begin(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	rmrf(dbrep);
}

static void
begin_begin(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	t( sp_begin(db) == -1 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	rmrf(dbrep);
}

static void
commit(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_commit(db) == -1 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	rmrf(dbrep);
}

static void
rollback(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_rollback(db) == -1 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	rmrf(dbrep);
}

static void
begin_commit_empty(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	t( sp_commit(db) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	rmrf(dbrep);
}

static void
begin_rollback_empty(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	t( sp_commit(db) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	rmrf(dbrep);
}

static void
begin_commit(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	uint32_t k = 1;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	k = 2;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	k = 3;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	t( sp_commit(db) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	rmrf(dbrep);
}

static void
begin_commit_get(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	uint32_t k = 1;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	k = 2;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	k = 3;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	t( sp_commit(db) == 0 );
	size_t vsize = 0;
	void *vp = NULL;
	k = 1;
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 1 );
	t( vsize == sizeof(k) );
	t( *(uint32_t*)vp == k );
	free(vp);
	k = 2;
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 1 );
	t( vsize == sizeof(k) );
	t( *(uint32_t*)vp == k );
	free(vp);
	k = 3;
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 1 );
	t( vsize == sizeof(k) );
	t( *(uint32_t*)vp == k );
	free(vp);
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	rmrf(dbrep);
}

static void
begin_get_commit_get(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	uint32_t k = 1;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	size_t vsize = 0;
	void *vp = NULL;
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 1 );
	t( vsize == sizeof(k) );
	t( *(uint32_t*)vp == k );
	free(vp);
	t( sp_commit(db) == 0 );
	vsize = 0;
	vp = NULL;
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 1 );
	t( vsize == sizeof(k) );
	t( *(uint32_t*)vp == k );
	free(vp);
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	rmrf(dbrep);
}

static void
begin_rollback(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	uint32_t k = 1;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	k = 2;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	k = 3;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	t( sp_rollback(db) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	rmrf(dbrep);
}

static void
begin_rollback_get(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	uint32_t k = 1;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	k = 2;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	k = 3;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	t( sp_rollback(db) == 0 );
	size_t vsize = 0;
	void *vp = NULL;
	k = 1;
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 0 );
	k = 2;
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 0 );
	k = 3;
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	rmrf(dbrep);
}

static void
begin_rollback_consistent(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	uint32_t k = 1;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	k = 2;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	k = 3;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	t( sp_begin(db) == 0 );
	k = 1;
	uint32_t v = k + 1;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0);
	k = 2;
	v = k + 1;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0);
	k = 3;
	v = k + 1;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0);

	size_t vsize = 0;
	void *vp = NULL;
	k = 1;
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 1 );
	t( vsize == sizeof(k) );
	t( *(uint32_t*)vp == k + 1);
	free(vp);
	k = 2;
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 1 );
	t( vsize == sizeof(k) );
	t( *(uint32_t*)vp == k + 1);
	free(vp);
	k = 3;
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 1 );
	t( vsize == sizeof(k) );
	t( *(uint32_t*)vp == k + 1);
	free(vp);
	t( sp_rollback(db) == 0 );

	vsize = 0;
	vp = NULL;
	k = 1;
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 1 );
	t( vsize == sizeof(k) );
	t( *(uint32_t*)vp == k);
	free(vp);
	k = 2;
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 1 );
	t( vsize == sizeof(k) );
	t( *(uint32_t*)vp == k);
	free(vp);
	k = 3;
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 1 );
	t( vsize == sizeof(k) );
	t( *(uint32_t*)vp == k);
	free(vp);

	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	rmrf(dbrep);
}

static void
begin_commit_2x(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	uint32_t k = 1;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	k = 2;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	k = 3;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	t( sp_commit(db) == 0 );
	t( sp_begin(db) == 0 );
	k = 1;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	k = 2;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	k = 3;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	t( sp_commit(db) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	rmrf(dbrep);
}

static void
rollback_on_close(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	uint32_t k = 1;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	k = 2;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	k = 3;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	t( sp_destroy(db) == 0 );
	db = sp_open(env);
	t( db != NULL );
	size_t vsize = 0;
	void *vp = NULL;
	k = 1;
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 0 );
	k = 2;
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 0 );
	k = 3;
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	rmrf(dbrep);
}

static void
rollback_recover(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	uint32_t k = 1;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	k = 2;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	k = 3;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	t( sp_rollback(db) == 0);
	t( sp_destroy(db) == 0 );
	db = sp_open(env);
	t( db != NULL );
	size_t vsize = 0;
	void *vp = NULL;
	k = 1;
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 0 );
	k = 2;
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 0 );
	k = 3;
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	rmrf(dbrep);
}

static void
commit_recover(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	uint32_t k = 1;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	k = 2;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	k = 3;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0);
	t( sp_commit(db) == 0 );
	t( sp_destroy(db) == 0 );
	db = sp_open(env);
	t( db != NULL );
	size_t vsize = 0;
	void *vp = NULL;
	k = 1;
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 1 );
	t( vsize == sizeof(k) );
	t( *(uint32_t*)vp == k);
	free(vp);
	k = 2;
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 1 );
	t( vsize == sizeof(k) );
	t( *(uint32_t*)vp == k);
	free(vp);
	k = 3;
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 1 );
	t( vsize == sizeof(k) );
	t( *(uint32_t*)vp == k);
	free(vp);
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	rmrf(dbrep);
}

static void
cursor_begin(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	void *cur = sp_cursor(db, SPGTE, NULL, 0);
	t( cur != NULL );
	t( sp_begin(db) == -1 );
	t( sp_commit(db) == -1 );
	t( sp_destroy(cur) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	rmrf(dbrep);
}

static void
cursor_commit(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	void *cur = sp_cursor(db, SPGTE, NULL, 0);
	t( cur != NULL );
	t( sp_commit(db) == -1 );
	t( sp_destroy(cur) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	rmrf(dbrep);
}

static void
cursor_closed_commit(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	void *cur = sp_cursor(db, SPGTE, NULL, 0);
	t( cur != NULL );
	t( sp_destroy(cur) == 0 );
	t( sp_commit(db) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	rmrf(dbrep);
}

static void
cursor_rollback(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	void *cur = sp_cursor(db, SPGTE, NULL, 0);
	t( cur != NULL );
	t( sp_rollback(db) == -1 );
	t( sp_destroy(cur) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	rmrf(dbrep);
}

static void
begin_fetch_gte_commit(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	uint32_t k = 1, v = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 3;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	void *cur = sp_cursor(db, SPGTE, NULL, 0);
	t( cur != NULL );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 1 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 2 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 3 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 0 );
	t( sp_fetch(cur) == 0 );
	t( sp_destroy(cur) == 0 );
	t( sp_commit(db) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	t( rmrf(dbrep) == 0 );
}

static void
begin_fetch_gt_commit(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	uint32_t k = 1, v = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 3;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	void *cur = sp_cursor(db, SPGT, NULL, 0);
	t( cur != NULL );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 1 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 2 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 3 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 0 );
	t( sp_fetch(cur) == 0 );
	t( sp_destroy(cur) == 0 );
	t( sp_commit(db) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	t( rmrf(dbrep) == 0 );
}

static void
begin_fetch_lte_commit(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	uint32_t k = 1, v = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 3;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	void *cur = sp_cursor(db, SPLTE, NULL, 0);
	t( cur != NULL );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 3 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 2 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 1 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 0 );
	t( sp_fetch(cur) == 0 );
	t( sp_destroy(cur) == 0 );
	t( sp_commit(db) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	t( rmrf(dbrep) == 0 );
}

static void
begin_fetch_lt_commit(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	uint32_t k = 1, v = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 3;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	void *cur = sp_cursor(db, SPLT, NULL, 0);
	t( cur != NULL );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 3 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 2 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 1 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 0 );
	t( sp_fetch(cur) == 0 );
	t( sp_destroy(cur) == 0 );
	t( sp_commit(db) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	t( rmrf(dbrep) == 0 );
}

static void
begin_fetch_kgte_commit(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	uint32_t k = 1, v = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 3;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );

	k = 2;
	void *cur = sp_cursor(db, SPGTE, &k, sizeof(k));
	t( cur != NULL );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 2 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 3 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 0 );
	t( sp_fetch(cur) == 0 );
	t( sp_destroy(cur) == 0 );
	t( sp_commit(db) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	t( rmrf(dbrep) == 0 );
}

static void
begin_fetch_kgt_commit(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	uint32_t k = 1, v = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 3;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 2;
	void *cur = sp_cursor(db, SPGT, &k, sizeof(k));
	t( cur != NULL );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 3 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 0 );
	t( sp_fetch(cur) == 0 );
	t( sp_destroy(cur) == 0 );
	t( sp_commit(db) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	t( rmrf(dbrep) == 0 );
}

static void
begin_fetch_klte_commit(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	uint32_t k = 1, v = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 3;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 2;
	void *cur = sp_cursor(db, SPLTE, &k, sizeof(k) );
	t( cur != NULL );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 2 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 1 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 0 );
	t( sp_fetch(cur) == 0 );
	t( sp_destroy(cur) == 0 );
	t( sp_commit(db) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	t( rmrf(dbrep) == 0 );
}

static void
begin_fetch_klt_commit(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	uint32_t k = 1, v = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 3;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 2;
	void *cur = sp_cursor(db, SPLT, &k, sizeof(k) );
	t( cur != NULL );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 1 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 0 );
	t( sp_fetch(cur) == 0 );
	t( sp_destroy(cur) == 0 );
	t( sp_commit(db) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	t( rmrf(dbrep) == 0 );
}

static void
begin_fetch_after_end_commit(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	uint32_t k = 1, v = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 3;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	void *cur = sp_cursor(db, SPGTE, NULL, 0);
	t( cur != NULL );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 1 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 2 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 3 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 0 );
	t( sp_keysize(cur) == 0 );
	t( sp_key(cur) == NULL );
	t( sp_valuesize(cur) == 0 );
	t( sp_value(cur) == NULL );
	t( sp_destroy(cur) == 0 );
	t( sp_commit(db) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	t( rmrf(dbrep) == 0 );
}

static void
begin_fetch_gte_rollback(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	uint32_t k = 1, v = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 3;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	void *cur = sp_cursor(db, SPGTE, NULL, 0);
	t( cur != NULL );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 1 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 2 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 3 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 0 );
	t( sp_fetch(cur) == 0 );
	t( sp_destroy(cur) == 0 );
	t( sp_rollback(db) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	t( rmrf(dbrep) == 0 );
}

static void
begin_commit_fetch(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	uint32_t k = 1, v = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 3;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	t( sp_commit(db) == 0 );
	void *cur = sp_cursor(db, SPGTE, NULL, 0);
	t( cur != NULL );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 1 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 2 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 1 );
	t( *(uint32_t*)sp_key(cur) == 3 );
	t( sp_keysize(cur) == sizeof(k) );
	t( *(uint32_t*)sp_value(cur) == 2 );
	t( sp_valuesize(cur) == sizeof(v) );
	t( sp_fetch(cur) == 0 );
	t( sp_fetch(cur) == 0 );
	t( sp_destroy(cur) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	t( rmrf(dbrep) == 0 );
}

static void
begin_rollback_fetch(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	t( sp_begin(db) == 0 );
	uint32_t k = 1, v = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 2;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 3;
	t( sp_set(db, &k, sizeof(k), &v, sizeof(v)) == 0 );
	t( sp_rollback(db) == 0 );
	void *cur = sp_cursor(db, SPGTE, NULL, 0);
	t( cur != NULL );
	t( sp_fetch(cur) == 0 );
	t( sp_fetch(cur) == 0 );
	t( sp_destroy(cur) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	t( rmrf(dbrep) == 0 );
}

int
main(int argc, char *argv[])
{
	rmrf(dbrep);

	test(begin);
	test(begin_begin);
	test(begin);
	test(commit);
	test(rollback);
	test(begin_commit_empty);
	test(begin_rollback_empty);
	test(begin_commit);
	test(begin_commit_get);
	test(begin_get_commit_get);
	test(begin_rollback);
	test(begin_rollback_get);
	test(begin_rollback_consistent);
	test(begin_commit_2x);
	test(rollback_on_close);
	test(rollback_recover);
	test(commit_recover);
	test(cursor_begin);
	test(cursor_commit);
	test(cursor_closed_commit);
	test(cursor_rollback);
	test(begin_fetch_gte_commit);
	test(begin_fetch_gt_commit);
	test(begin_fetch_lte_commit);
	test(begin_fetch_lt_commit);
	test(begin_fetch_kgte_commit);
	test(begin_fetch_kgt_commit);
	test(begin_fetch_klte_commit);
	test(begin_fetch_klt_commit);
	test(begin_fetch_after_end_commit);
	test(begin_fetch_gte_rollback);
	test(begin_commit_fetch);
	test(begin_rollback_fetch);
	return 0;
}
