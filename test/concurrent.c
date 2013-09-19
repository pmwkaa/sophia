
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <sophia.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include "test.h"

static char *dbrep1 = "./rep";
static char *dbrep2 = "./rep2";

static inline int
cmp(char *a, size_t asz, char *b, size_t bsz, void *arg) {
	register uint32_t av = *(uint32_t*)a;
	register uint32_t bv = *(uint32_t*)b;
	if (av == bv)
		return 0;
	return (av > bv) ? 1 : -1;
}

static void
single_process(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep1) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	void *env2 = sp_env();
	t( env2 != NULL );
	t( sp_ctl(env2, SPDIR, SPO_CREAT|SPO_RDWR, dbrep1) == 0 );
	void *db2 = sp_open(env2);
	t( db2 == NULL );
	t( sp_destroy(env2) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	rmrf(dbrep1);
}

static void
single_process_2(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep1) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	void *env2 = sp_env();
	t( env != NULL );
	t( sp_ctl(env2, SPDIR, SPO_CREAT|SPO_RDWR, dbrep2) == 0 );
	t( sp_ctl(env2, SPCMP, cmp, NULL) == 0 );
	void *db2 = sp_open(env2);
	t( db2 != NULL );
	uint32_t k = 1;
	t( sp_set(db2, &k, sizeof(k), &k, sizeof(k)) == 0 );
	size_t vsize = 0;
	void *vp = NULL;
	t( sp_get(db2, &k, sizeof(k), &vp, &vsize) == 1 );
	t( vsize == sizeof(k) );
	t( *(uint32_t*)vp == k );
	free(vp);
	k = 3;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0 );
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 1 );
	t( vsize == sizeof(k) );
	t( *(uint32_t*)vp == k );
	free(vp);
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	t( sp_destroy(db2) == 0 );
	t( sp_destroy(env2) == 0 );
	t( rmrf(dbrep1) == 0 );
	t( rmrf(dbrep2) == 0 );
}

static void
multi_process(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep1) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	int pid = fork();
	t( pid != -1 );
	if (pid == 0) {
		/* new process */
		void *env2 = sp_env();
		t( env2 != NULL );
		t( sp_ctl(env2, SPDIR, SPO_CREAT|SPO_RDWR, dbrep1) == 0 );
		void *db2 = sp_open(env2);
		t( db2 == NULL );
		t( sp_destroy(env2) == 0 );
		/* valgrind: parent db and env are unfreed here, and that
		 *           is correct otherwise destroy would
		 *           corrupt the database.
		*/
		exit(0);
	} else {
		int status = 0;
		t( waitpid(pid, &status, 0) == pid );
		t( status == 0 );
	}
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	rmrf(dbrep1);
}

static void
multi_process_2(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep1) == 0 );
	t( sp_ctl(env, SPCMP, cmp, NULL) == 0 );
	void *db = sp_open(env);
	t( db != NULL );
	void *env2 = sp_env();
	t( env != NULL );
	t( sp_ctl(env2, SPDIR, SPO_CREAT|SPO_RDWR, dbrep2) == 0 );
	t( sp_ctl(env2, SPCMP, cmp, NULL) == 0 );
	uint32_t k = 1;
	size_t vsize = 0;
	void *vp = NULL;
	int pid = fork();
	t( pid != -1 );
	if (pid == 0) {
		void *db2 = sp_open(env2);
		t( db2 != NULL );
		t( sp_set(db2, &k, sizeof(k), &k, sizeof(k)) == 0 );
		t( sp_get(db2, &k, sizeof(k), &vp, &vsize) == 1 );
		t( vsize == sizeof(k) );
		t( *(uint32_t*)vp == k );
		free(vp);
		t( sp_destroy(db2) == 0 );
		t( sp_destroy(env2) == 0 );
		t( rmrf(dbrep2) == 0 );
		/* valgrind: parent db and env are unfreed here, and that
		 *           is correct otherwise destroy would
		 *           corrupt the database.
		*/
		exit(0);
	}
	k = 3;
	t( sp_set(db, &k, sizeof(k), &k, sizeof(k)) == 0 );
	t( sp_get(db, &k, sizeof(k), &vp, &vsize) == 1 );
	t( vsize == sizeof(k) );
	t( *(uint32_t*)vp == k );
	free(vp);
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	t( rmrf(dbrep1) == 0 );
	int status = 0;
	t( waitpid(pid, &status, 0) == pid );
	t( status == 0 );
}

int
main(int argc, char *argv[])
{
	rmrf(dbrep1);
	rmrf(dbrep2);

	test(single_process);
	test(single_process_2);
	test(multi_process);
	test(multi_process_2);
	return 0;
}
