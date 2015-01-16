
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libst.h>
#include <sophia.h>

int exists(char *path, char *name) {
	char file[1024];
	snprintf(file, sizeof(file), "%s/%s", path, name);
	return sr_fileexists(file);
}

int rmrf(char *path)
{
	DIR *d = opendir(path);
	if (d == NULL)
		return -1;
	char file[1024];
	struct dirent *de;
	while ((de = readdir(d))) {
		if (de->d_name[0] == '.')
			continue;
		snprintf(file, sizeof(file), "%s/%s", path, de->d_name);
		int rc;
		if (de->d_type == DT_DIR) {
			rc = rmrf(file);
		} else {
			rc = unlink(file);
		}
		if (rc == -1) {
			closedir(d);
			return -1;
		}
	}
	closedir(d);
	return rmdir(path);
}

void
st_scene_rmrf(stscene *g, stc *cx)
{
	rmrf(cx->suite->sophiadir);
	rmrf(cx->suite->backupdir);
	rmrf(cx->suite->logdir);
	rmrf(cx->suite->dir);
}

void
st_scene_create(stscene *g, stc *cx)
{
	printf(".create");
	fflush(NULL);
	t( cx->db  == NULL );
	t( cx->env == NULL );
	cx->env = sp_env();
	t( cx->env != NULL );
	void *c = sp_ctl(cx->env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.enable", "1") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.sync", "0") == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	cx->db = sp_get(c, "db.test");
	t( cx->db != NULL );
}

void
st_scene_branch_wm(stscene *g, stc *cx)
{
	void *c = sp_ctl(cx->env);
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
}

void st_scene_multithread(stscene *g, stc *cx)
{
	printf(".multithread");
	fflush(NULL);
	void *c = sp_ctl(cx->env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "5") == 0 );
}

void st_scene_multithread_1(stscene *g, stc *cx)
{
	printf(".multithread");
	fflush(NULL);
	void *c = sp_ctl(cx->env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "1") == 0 );
}

void
st_scene_open(stscene *g, stc *cx)
{
	printf(".open");
	fflush(NULL);
	t( sp_open(cx->env) == 0 );
}

static inline void
st_phase_commit(stc *cx)
{
	switch (cx->phase_scene) {
	case 0: break;
	case 1:
		t( sp_set(sp_ctl(cx->env), "db.test.branch") == 0 );
		break;
	case 2:
		t( sp_set(sp_ctl(cx->env), "db.test.branch") == 0 );
		t( sp_set(sp_ctl(cx->env), "db.test.compact") == 0 );
		break;
	case 3:
		t( sp_set(sp_ctl(cx->env), "log.rotate") == 0 );
		t( sp_set(sp_ctl(cx->env), "log.gc") == 0 );
		break;
	case 4:
		if (cx->phase == 0) {
			t( sp_set(sp_ctl(cx->env), "db.test.branch") == 0 );
			t( sp_set(sp_ctl(cx->env), "log.rotate") == 0 );
			t( sp_set(sp_ctl(cx->env), "log.gc") == 0 );
			cx->phase = 1;
		} else
		if (cx->phase == 1) {
			t( sp_set(sp_ctl(cx->env), "db.test.compact") == 0 );
			cx->phase = 0;
		}
		break;
	default: t(0);
	}
}

void
st_scene_phase(stscene *g, stc *cx)
{
	cx->commit = st_phase_commit;
	cx->phase_scene = g->state;
	cx->phase = 0;
	switch (g->state) {
	case 0:
		printf(".in_memory");
		fflush(NULL);
		break;
	case 1:
		printf(".branch");
		fflush(NULL);
		break;
	case 2:
		printf(".compact");
		fflush(NULL);
		break;
	case 3:
		printf(".logrotate_gc");
		fflush(NULL);
		break;
	case 4:
		printf(".branch_compact");
		fflush(NULL);
		break;
	}
}

void
st_scene_test(stscene *g, stc *cx)
{
	printf(".test");
	fflush(NULL);
	cx->test->function(cx);
	cx->suite->stat_test++;
}

void
st_scene_truncate(stscene *g, stc *cx)
{
	printf(".truncate");
	fflush(NULL);
	void *o = sp_object(cx->db);
	t( o != NULL );
	void *c = sp_cursor(cx->db, o);
	t( c != NULL );
	while ((o = sp_get(c))) {
		void *k = sp_object(cx->db);
		t( k != NULL );
		int keysize;
		void *key = sp_get(o, "key", &keysize);
		sp_set(k, "key", key, keysize);
		t( sp_delete(cx->db, k) == 0 );
	}
	t( sp_destroy(c) == 0 );
}

void
st_scene_destroy(stscene *g, stc *cx)
{
	printf(".destroy");
	fflush(NULL);
	t( cx->env != NULL );
	t( sp_destroy(cx->env) == 0 );
	cx->env = NULL;
	cx->db  = NULL;
}

void
st_scene_pass(stscene *g, stc *cx)
{
	printf(": ok\n");
	fflush(NULL);
}

void
st_scene_rerun(stscene *g srunused, stc *cx srunused)
{
	printf("\n (rerun) ");
	fflush(NULL);
}
