
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

int rmrf(char *path) {
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
st_scene_create(stscene *g, stc *cx)
{
	printf(".create");
	fflush(NULL);
	rmrf(cx->suite->logdir);
	rmrf(cx->suite->dir);
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
st_scene_phases(stscene *g, stc *cx)
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
}

void
st_scene_destroy(stscene *g, stc *cx)
{
	t( cx->env != NULL );
	t( sp_destroy(cx->env) == 0 );
	cx->env = NULL;
	cx->db  = NULL;
	printf(": ok\n");
	fflush(NULL);
}

extern st *sliter_group(void);
extern st *sditer_group(void);
extern st *sdpageiter_group(void);
extern st *svindex_group(void);
extern st *svindexiter_group(void);
extern st *svmergeiter_group(void);
extern st *svseaveiter_group(void);
extern st *dml_group(void);
extern st *object_group(void);
extern st *profiler_group(void);
extern st *cursor_group(void);
extern st *transaction_group(void);

int
main(int argc, char *argv[])
{
	printf("sophia test-suite.\n\n");

	stsuite s;
	st_init(&s, "./dir", "./logdir");
	st_unit(&s, sliter_group());
	st_unit(&s, sditer_group());
	st_unit(&s, sdpageiter_group());
	st_unit(&s, svindex_group());
	st_unit(&s, svindexiter_group());
	st_unit(&s, svmergeiter_group());
	st_unit(&s, svseaveiter_group());
	st_unit(&s, dml_group());
	st_unit(&s, profiler_group());

	st_scene(&s, st_scene_create,  1);
	st_scene(&s, st_scene_open,    1);
	st_scene(&s, st_scene_phases,  5);
	st_scene(&s, st_scene_test,    1);
	st_scene(&s, st_scene_destroy, 1);

	st_group(&s, object_group());
	st_group(&s, cursor_group());
	st_group(&s, transaction_group());

	st_rununit(&s);
	do {
		st_run(&s);
	} while(st_next(&s));
	st_free(&s);
	return 0;
}
