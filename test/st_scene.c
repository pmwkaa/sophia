
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
		int rc = unlink(file);
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
	t( sp_set(c, "db.test.logdir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "db.test.threads", "0") == 0 );
	cx->db = sp_get(c, "db.test");
	t( cx->db != NULL );
}

void st_scene_multithread(stscene *g, stc *cx)
{
	printf(".multithread");
	fflush(NULL);
	void *c = sp_ctl(cx->env);
	t( c != NULL );
	t( sp_set(c, "db.test.threads", "5") == 0 );
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
	case 0:
		t( sp_set(sp_ctl(cx->env), "db.test.run_branch") == 0 );
		break;
	case 1:
		t( sp_set(sp_ctl(cx->env), "db.test.run_branch") == 0 );
		t( sp_set(sp_ctl(cx->env), "db.test.run_merge") == 0 );
		break;
	case 2:
		t( sp_set(sp_ctl(cx->env), "db.test.run_logrotate") == 0 );
		break;
	case 3:
		if (cx->phase == 0) {
			t( sp_set(sp_ctl(cx->env), "db.test.run_branch") == 0 );
			cx->phase = 1;
		} else
		if (cx->phase == 1) {
			t( sp_set(sp_ctl(cx->env), "db.test.run_merge") == 0 );
			cx->phase = 0;
		}
		break;
	case 4:
		if (cx->phase == 0) {
			t( sp_set(sp_ctl(cx->env), "db.test.run_branch") == 0 );
			cx->phase = 1;
		} else
		if (cx->phase == 1) {
			t( sp_set(sp_ctl(cx->env), "db.test.run_merge") == 0 );
			cx->phase = 2;
		} else
		if (cx->phase == 2) {
			t( sp_set(sp_ctl(cx->env), "db.test.run_logrotate") == 0 );
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
		printf(".branch");
		fflush(NULL);
		break;
	case 1:
		printf(".merge");
		fflush(NULL);
		break;
	case 2:
		printf(".logrotate");
		fflush(NULL);
		break;
	case 3:
		printf(".branch+merge");
		fflush(NULL);
		break;
	case 4:
		printf(".branch+logrotate");
		fflush(NULL);
		break;
	}
}

void
st_scene_test(stscene *g, stc *cx)
{
	cx->test->function(cx);
	cx->suite->stat_test++;
}

void
st_scene_truncate(stscene *g, stc *cx)
{
	printf(".truncate");
	fflush(NULL);

	void *c = sp_cursor(cx->db, ">=", NULL);
	t( c != NULL );
	void *o;
	while ((o = sp_get(c))) {
		void *k = sp_object(cx->db);
		t( k != NULL );
		int keysize;
		void *key = sp_get(o, "key", &keysize);
		sp_set(k, "key", key, keysize);
		t( sp_delete(cx->db, k) == 0 );
	}
}

void
st_scene_destroy(stscene *g, stc *cx)
{
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
